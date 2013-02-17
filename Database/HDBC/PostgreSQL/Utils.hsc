{- -*- mode: haskell; -*- 
-}

module Database.HDBC.PostgreSQL.Utils where
import Foreign.C.String
import Foreign.ForeignPtr
import Foreign.Ptr
import Database.HDBC(throwSqlError)
import Database.HDBC.Types
import Database.HDBC.PostgreSQL.Types
import Control.Concurrent.MVar
import Foreign.C.Types
import Control.Exception
import Foreign.Storable
import Foreign.Marshal.Array
import Foreign.Marshal.Alloc
import Foreign.Marshal.Utils
import Data.Word
import Data.Foldable (forM_)
import Control.Applicative ((<$>))
import qualified Data.ByteString.UTF8 as BUTF8
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BCHAR8
#ifndef __HUGS__
-- Hugs includes this in Data.ByteString
import qualified Data.ByteString.Unsafe as B
#endif

#include "hdbc-postgresql-helper.h"
#include "pgtypes.h"

raiseError :: String -> Word32 -> (Ptr CConn) -> IO a
raiseError msg code cconn =
    do rc <- pqerrorMessage cconn
       bs <- B.packCString rc
       let str = BUTF8.toString bs
       throwSqlError $ SqlError {seState = "",
                                 seNativeError = fromIntegral code,
                                 seErrorMsg = msg ++ ": " ++ str}

{- This is a little hairy.

We have a Conn object that is actually a finalizeonce wrapper around
the real object.  We use withConn to dereference the foreign pointer,
and then extract the pointer to the real object from the finalizeonce struct.

But, when we close the connection, we need the finalizeonce struct, so that's
done by withRawConn.

Ditto for statements. -}

withConn :: Conn -> (Ptr CConn -> IO b) -> IO b
withConn (_lock,conn) = genericUnwrap conn

-- Perform the associated action with the connection lock held.
-- Care must be taken with the use of this as it is *not* re-entrant.  Calling it
-- a second time in the same thread will cause dead-lock. 
-- (A better approach would be to use RLock from concurrent-extra)
withConnLocked :: Conn -> (Ptr CConn -> IO b) -> IO b
withConnLocked c@(lock,_) a = withConn c (\cconn -> withMVar lock (\_ -> a cconn))

withRawConn :: Conn -> (Ptr WrappedCConn -> IO b) -> IO b
withRawConn (_lock,conn) = withForeignPtr conn

withStmt :: Stmt -> (Ptr CStmt -> IO b) -> IO b
withStmt = genericUnwrap

withRawStmt :: Stmt -> (Ptr WrappedCStmt -> IO b) -> IO b
withRawStmt = withForeignPtr


-- Prepares the type, length and format arrays for use with
-- PQexecParams. This is for use with 
-- See http://www.postgresql.org/docs/9.2/static/libpq-exec.html, section PQexecParams
withCArraysMixedFormats ::
       [SqlValue]
    -> (Ptr #{type Oid} -> Ptr CInt -> Ptr CInt -> IO a)
    -> IO a
withCArraysMixedFormats exampleInput action =
    withArray (map getOid exampleInput) $ \ types ->
    withArray (map getLength exampleInput) $ \ lengths ->
    withArray (map getFormat exampleInput) $ \ formats ->
    action types lengths formats
  where
    getOid :: SqlValue -> #{type Oid}
    getOid (SqlDouble _) = #{const PG_TYPE_FLOAT8}
    getOid (SqlInt64 _)  = #{const PG_TYPE_INT8}
    getOid _ = 0

    getLength :: SqlValue -> CInt
    getLength (SqlDouble _) = 8
    getLength (SqlInt64 _)  = 8
    getLength _             = 0

    getFormat :: SqlValue -> CInt
    getFormat (SqlDouble _) = 1
    getFormat (SqlInt64 _)  = 1
    getFormat _             = 0

withCArrayValuesMixedFormats :: [SqlValue] -> (Ptr (Ptr CChar) -> IO a) -> IO a
withCArrayValuesMixedFormats input action =
    withAnyArr0 convertValues freeValues input action
  where
    convertValues :: SqlValue -> IO (Ptr CChar)
    convertValues SqlNull = return nullPtr
    convertValues (SqlDouble x) = do
        ptr <- malloc
        poke ptr x
        castPtr <$> switchhalfs (castPtr ptr)
    convertValues (SqlInt64 x) = do
        ptr <- malloc
        poke ptr x
        castPtr <$> switchhalfs (castPtr ptr)
{-
          convertValues y@(SqlZonedTime _) = convfunc (SqlString $ 
                                                "TIMESTAMP WITH TIME ZONE '" ++ 
                                                fromSql y ++ "'")
-}
    convertValues y@(SqlUTCTime _) = convertValues (SqlZonedTime (fromSql y))
    convertValues y@(SqlEpochTime _) = convertValues (SqlZonedTime (fromSql y))
    convertValues (SqlByteString x) = cstrUtf8BString (cleanUpBSNulls x)
    convertValues x =
        cstrUtf8BString (fromSql x)
    freeValues :: Ptr CChar -> IO ()
    freeValues x =
        if x == nullPtr
            then return ()
            else free x
    switchhalfs :: Ptr Word8 -> IO (Ptr Word8)
    switchhalfs inPtr = do
        outPtr <- mallocBytes 8
        forM_ [0 .. 7] $ \ i ->
            poke (advancePtr outPtr i) =<< peek (advancePtr inPtr (7 - i))
        free inPtr
        return outPtr

cleanUpBSNulls :: B.ByteString -> B.ByteString
cleanUpBSNulls bs | 0 `B.notElem` bs = bs
                  | otherwise = B.concatMap convfunc bs
  where convfunc 0 = bsForNull
        convfunc x = B.singleton x
        bsForNull = BCHAR8.pack "\\000"

withAnyArr0 :: (a -> IO (Ptr b)) -- ^ Function that transforms input data into pointer
            -> (Ptr b -> IO ())  -- ^ Function that frees generated data
            -> [a]               -- ^ List of input data
            -> (Ptr (Ptr b) -> IO c) -- ^ Action to run with the C array
            -> IO c             -- ^ Return value
withAnyArr0 input2ptract freeact inp action =
    bracket (mapM input2ptract inp)
            (\clist -> mapM_ freeact clist)
            (\clist -> withArray0 nullPtr clist action)

cstrUtf8BString :: B.ByteString -> IO CString
cstrUtf8BString bs = do
    B.unsafeUseAsCStringLen bs $ \(s,len) -> do
        res <- mallocBytes (len+1)
        -- copy in
        copyBytes res s len
        -- null terminate
        poke (plusPtr res len) (0::CChar)
        -- return ptr
        return res

genericUnwrap :: ForeignPtr (Ptr a) -> (Ptr a -> IO b) -> IO b
genericUnwrap fptr action = withForeignPtr fptr (\structptr ->
    do objptr <- #{peek finalizeonce, encapobj} structptr
       action objptr
                                                )
          
foreign import ccall unsafe "libpq-fe.h PQerrorMessage"
  pqerrorMessage :: Ptr CConn -> IO CString

