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
import Foreign.Storable
import Foreign.Marshal.Array
import Foreign.Marshal.Alloc
import Foreign.Marshal.Utils
import Control.Applicative
import Control.Monad (when)
import Data.Word
import Data.Foldable (forM_)
import Data.Monoid
import Numeric
import Text.Printf
import qualified Data.ByteString.UTF8 as BUTF8
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BCHAR8
#ifndef __HUGS__
-- Hugs includes this in Data.ByteString
import qualified Data.ByteString.Unsafe as B
#endif

#include "hdbc-postgresql-helper.h"


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

revertBytes :: Int -> Ptr Word8 -> IO (Ptr Word8)
revertBytes n inPtr = do
    outPtr <- mallocBytes n
    forM_ [0 .. pred n] $ \ i ->
        poke (advancePtr outPtr i) =<< peek (advancePtr inPtr (pred n - i))
    free inPtr
    return outPtr

-- | encode to postgresql's hex format, see
--   http://www.postgresql.org/docs/9.2/static/datatype-binary.html,
--   section 8.4.1
toHex :: B.ByteString -> B.ByteString
toHex a = mconcat (BCHAR8.pack "\\x" : map hex (B.unpack a))
  where
    hex :: Word8 -> B.ByteString
    hex = BCHAR8.pack . printf "%02x"

-- | decode from postgresql's hex format, see also 'toHex'
fromHex :: B.ByteString -> Either String B.ByteString
fromHex input = do
    let (backslashX, hexDigits) = splitAt 2 $ BCHAR8.unpack input
    when (backslashX /= "\\x") $
        err ("expected \"\\x\", not: " ++ show (B.take 8 input))
    B.pack <$> inner hexDigits
  where
    inner :: [Char] -> Either String [Word8]
    inner [] = return []
    inner (a : b : r) =
        case readHex (a : b : []) of
            ((n, []) : _) -> do
                ns <- inner r
                return (n : ns)
            _ -> err ("unreadable hex digits: " ++ show ([a, b]))
    inner _ = err "uneven number of digits"

    err :: String -> Either String a
    err msg = Left ("error during decoding of hexadecimal bytestring: " ++ msg)

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

