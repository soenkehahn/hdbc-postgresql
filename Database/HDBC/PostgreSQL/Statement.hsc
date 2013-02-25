-- -*- mode: haskell; -*-
{-# CFILES hdbc-postgresql-helper.c #-}
-- Above line for hugs

module Database.HDBC.PostgreSQL.Statement where
import Database.HDBC.Types
import Database.HDBC
import Database.HDBC.PostgreSQL.Types
import Database.HDBC.PostgreSQL.Utils
import Foreign.C.Types
import Foreign.ForeignPtr
import Foreign.Ptr
import Foreign.Storable
import Control.Concurrent.MVar
import Foreign.C.String
import Foreign.Marshal.Alloc
import Foreign.Marshal.Array
import Control.Applicative ((<$>))
import Control.Monad (liftM, when)
import Data.List
import Data.Foldable (forM_)
import Data.Traversable (forM)
import Data.Word
import Data.Ratio
import qualified Data.ByteString as B
import qualified Data.ByteString.UTF8 as BUTF8
import Database.HDBC.PostgreSQL.Parser(convertSQL)
import Database.HDBC.DriverUtils
import Database.HDBC.PostgreSQL.PTypeConv
import Data.Time.Format
import System.Locale

#include "pgtypes.h"


l :: Monad m => t -> m ()
l _ = return ()
--l m = hPutStrLn stderr ("\n" ++ m)

#include <libpq-fe.h>

data SState =
    SState { stomv :: MVar (Maybe Stmt),
             nextrowmv :: MVar (CInt), -- -1 for no next row (empty); otherwise, next row to read.
             dbo :: Conn,
             squery :: String,
             -- Contains the query for the db, possibly with missing arguments.
             -- These parameters are postgres-style, i.e. $1, $2, ...
             -- (This is used when executing unprepared queries.)
             preparedStatement :: Maybe PreparedStatement,
             -- Contains Nothing in case of unprepared queries.
             -- Contains the name of the prepared statement, if that exists.
             coldefmv :: MVar [(String, SqlColDesc)]}

data PreparedStatement =
    PreparedStatement
        String -- statement name
        [#{type Oid}] -- types as inferred by postgres


-- | Initializes and returns an unprepared 'Statement'.
newSth :: Conn -> ChildList -> String -> IO Statement
newSth indbo mchildren query =
    do l "in newSth"
       newstomv <- newMVar Nothing
       newnextrowmv <- newMVar (-1)
       newcoldefmv <- newMVar []
       usequery <- case convertSQL query of
                      Left errstr -> throwSqlError $ SqlError
                                      {seState = "",
                                       seNativeError = (-1),
                                       seErrorMsg = "hdbc prepare: " ++
                                                    show errstr}
                      Right converted -> return converted
       let sstate = SState {stomv = newstomv, nextrowmv = newnextrowmv,
                            dbo = indbo, squery = usequery,
                            preparedStatement = Nothing,
                            coldefmv = newcoldefmv}
       let retval =
                Statement {execute = fexecute sstate,
                           executeMany = fexecutemany sstate,
                           executeRaw = fexecuteRaw sstate,
                           finish = public_ffinish sstate,
                           fetchRow = ffetchrow sstate,
                           originalQuery = query,
                           getColumnNames = fgetColumnNames sstate,
                           describeResult = fdescribeResult sstate}
       addChild mchildren retval
       return retval


-- * prepared queries

-- | Initializes and returns a prepared 'Statement'.
-- Duplicates most of the code from newSth, except for the call to
-- 'prepareQuery'.
--
-- prepared statements in postgresql can only be
-- SELECT, INSERT, UPDATE, DELETE, or VALUES.
-- As 'quickQuery' is implemented internally using prepared
-- statements (in the HDBC package), this constraint applies also for
-- 'quickQuery'. Use 'run' for other statements.
newPreparedSth :: Conn -> ChildList -> String -> IO Statement
newPreparedSth indbo mchildren query =
    do l "in newPreparedSth"
       newstomv <- newMVar Nothing
       newnextrowmv <- newMVar (-1)
       newcoldefmv <- newMVar []
       usequery <- case convertSQL query of
                      Left errstr -> throwSqlError $ SqlError
                                      {seState = "",
                                       seNativeError = (-1),
                                       seErrorMsg = "hdbc prepare: " ++
                                                    show errstr}
                      Right x -> return x
       preparedStmt <- prepareStatement indbo mchildren usequery
       let sstate = SState {stomv = newstomv, nextrowmv = newnextrowmv,
                            dbo = indbo, squery = usequery,
                            preparedStatement = Just preparedStmt,
                            coldefmv = newcoldefmv}
       let retval =
                Statement {execute = fexecute sstate,
                           executeMany = fexecutemany sstate,
                           executeRaw = fexecuteRaw sstate,
                           finish = public_ffinish sstate,
                           fetchRow = ffetchrow sstate,
                           originalQuery = query,
                           getColumnNames = fgetColumnNames sstate,
                           describeResult = fdescribeResult sstate}
       addChild mchildren retval
       return retval

prepareStatement :: Conn -> ChildList -> String -> IO PreparedStatement
prepareStatement conn mchildren query = do
    allPreparedStatements <- getAllPreparedStatements
    let stmtName = head $ filter (\ n -> not (n `elem` allPreparedStatements))
            (map (\ i -> "hdbcpreparedstatement" ++ show i) [0 :: Integer ..])
    let prepareQuery = "PREPARE " ++ stmtName ++ " AS " ++ query ++ ";"
    withConnLocked conn $ \cconn ->
        B.useAsCString (BUTF8.fromString prepareQuery) $ \ cquery ->
            do l "in prepareQuery"
               resptr <- pqexec cconn cquery
               throwSqlErrorOnError cconn =<< (pqresultStatus resptr :: IO Word32)
               return ()
    types <- getStatementTypes conn stmtName
    return $ PreparedStatement stmtName types
  where
    -- Returns the names of all registered prepared queries.
    getAllPreparedStatements :: IO [String]
    getAllPreparedStatements =
        map convert <$> quickQueryUnprepared
            "SELECT name FROM pg_prepared_statements;" []
      where
        convert :: [SqlValue] -> String
        convert [name] = fromSql name
        convert _ = error "hdbc: prepareStatement: sql error"

    -- Internal strict version of quickQuery using unprepared statements.
    quickQueryUnprepared :: String -> [SqlValue] -> IO [[SqlValue]]
    quickQueryUnprepared q arguments = do
        sstate <- newSth conn mchildren q
        -- FIXME: error handling
        _ <- execute sstate arguments
        fetchAllRows sstate
    
    throwSqlErrorOnError :: Ptr CConn -> Word32 -> IO ()
    throwSqlErrorOnError cconn status = case status of
        #{const PGRES_COMMAND_OK} -> return ()
        _ -> do
            errormsg  <- peekCStringUTF8 =<< pqerrorMessage cconn
            statusmsg <- peekCStringUTF8 =<< pqresStatus status
            throwSqlError $ SqlError { seState = "E"
                                     , seNativeError = fromIntegral status
                                     , seErrorMsg = "execute: " ++ statusmsg ++
                                                    ": " ++ errormsg}

getStatementTypes :: Conn -> String -> IO [#{type Oid}]
getStatementTypes conn stmtName =
    withConnLocked conn $ \ cconn ->
    B.useAsCString (BUTF8.fromString stmtName) $ \ cStmtName -> do
        pgresult <- pqdescribePrepared cconn cStmtName
        nparams <- pqnparams pgresult
        forM [0 .. pred nparams] $ \ i ->
            pqparamtype pgresult i

fgetColumnNames :: SState -> IO [(String)]
fgetColumnNames sstate =
    do c <- readMVar (coldefmv sstate)
       return (map fst c)

fdescribeResult :: SState -> IO [(String, SqlColDesc)]
fdescribeResult sstate =
    readMVar (coldefmv sstate)

{- For now, we try to just  handle things as simply as possible.
FIXME lots of room for improvement here (types, etc). -}
fexecute :: (Num a, Read a) => SState -> [SqlValue] -> IO a
fexecute sstate args =
    withConnLocked (dbo sstate) $ \ cconn ->
    withCQuery $ \ cqueryinformation ->
    withArray (replicate len nullPtr) $ \ cvalues ->
    withArray (replicate len 0) $ \ clengths ->
    withArray (replicate len 0) $ \ cformats ->
    fexecuteOnArrays sstate cconn cqueryinformation cvalues clengths cformats args
  where
    len = length args

    withCQuery :: (CQueryInformation -> IO a) -> IO a
    withCQuery action = case preparedStatement sstate of
        Nothing ->
            B.useAsCString (BUTF8.fromString (squery sstate)) $ \ cquery ->
            withArray (replicate len 0) $ \ ctypes ->
            action (CQuery cquery ctypes)
        Just (PreparedStatement stmtName types) ->
            B.useAsCString (BUTF8.fromString stmtName) $ \ cStmtName ->
            action (CPreparedStatement cStmtName types)


{- | Differs from fexecute in that it does not prepare its input
   query, and the input query may contain multiple statements.  This
   is useful for issuing DDL or DML commands. -}
fexecuteRaw :: SState -> IO ()
fexecuteRaw sstate =
    withConnLocked (dbo sstate) $ \cconn ->
        do l "in fexecute"
           public_ffinish sstate    -- Sets nextrowmv to -1
           resptr <- case preparedStatement sstate of
                Nothing ->
                    B.useAsCString (BUTF8.fromString (squery sstate)) $ \cquery ->
                    pqexec cconn cquery
                Just (PreparedStatement stmtName _types) ->
                    withArray [] $ \ cEmptyArgs ->
                    B.useAsCString (BUTF8.fromString stmtName) $ \cStmtName ->
                    pqexecPrepared cconn cStmtName
                        0 cEmptyArgs nullPtr nullPtr 0
           _ <- handleResultStatus cconn resptr sstate =<< pqresultStatus resptr :: IO Int
           return ()

handleResultStatus :: (Num a, Read a) => Ptr CConn -> WrappedCStmt -> SState -> ResultStatus -> IO a
handleResultStatus cconn resptr sstate status =
    case status of
      #{const PGRES_EMPTY_QUERY} ->
          do l $ "PGRES_EMPTY_QUERY: " ++ squery sstate
             pqclear_raw resptr
             _ <- swapMVar (coldefmv sstate) []
             return 0
      #{const PGRES_COMMAND_OK} ->
          do l $ "PGRES_COMMAND_OK: " ++ squery sstate
             rowscs <- pqcmdTuples resptr
             rows <- peekCString rowscs
             pqclear_raw resptr
             _ <- swapMVar (coldefmv sstate) []
             return $ case rows of
                        "" -> 0
                        x -> read x
      #{const PGRES_TUPLES_OK} ->
          do l $ "PGRES_TUPLES_OK: " ++ squery sstate
             _ <- fgetcoldef resptr >>= swapMVar (coldefmv sstate)
             numrows <- pqntuples resptr
             if numrows < 1 then (pqclear_raw resptr >> return 0) else
                 do
                   wrappedptr <- withRawConn (dbo sstate)
                                 (\rawconn -> wrapstmt resptr rawconn)
                   fresptr <- newForeignPtr pqclearptr wrappedptr
                   _ <- swapMVar (nextrowmv sstate) 0
                   _ <- swapMVar (stomv sstate) (Just fresptr)
                   return 0
      _ | resptr == nullPtr -> do
              l $ "PGRES ERROR: " ++ squery sstate
              errormsg  <- peekCStringUTF8 =<< pqerrorMessage cconn
              statusmsg <- peekCStringUTF8 =<< pqresStatus status

              throwSqlError $ SqlError { seState = "E"
                                       , seNativeError = fromIntegral status
                                       , seErrorMsg = "execute: " ++ statusmsg ++
                                                      ": " ++ errormsg}

      _ -> do l $ "PGRES ERROR: " ++ squery sstate
              errormsg  <- peekCStringUTF8 =<< pqresultErrorMessage resptr
              statusmsg <- peekCStringUTF8 =<< pqresStatus status
              state     <- peekCStringUTF8 =<<
                            pqresultErrorField resptr #{const PG_DIAG_SQLSTATE}

              pqclear_raw resptr
              throwSqlError $ SqlError { seState = state
                                       , seNativeError = fromIntegral status
                                       , seErrorMsg = "execute: " ++ statusmsg ++
                                                      ": " ++ errormsg}

peekCStringUTF8 :: CString -> IO String
-- Marshal a NUL terminated C string into a Haskell string, decoding it
-- with UTF8.
peekCStringUTF8 str
   | str == nullPtr  = return ""
   | otherwise       = fmap BUTF8.toString (B.packCString str)



{- General algorithm: find out how many columns we have, check the type
of each to see if it's NULL.  If it's not, fetch it as text and return that.
-}

ffetchrow :: SState -> IO (Maybe [SqlValue])
ffetchrow sstate = modifyMVar (nextrowmv sstate) dofetchrow
    where dofetchrow (-1) = l "ffr -1" >> return ((-1), Nothing)
          dofetchrow nextrow = modifyMVar (stomv sstate) $ \stmt ->
             case stmt of
               Nothing -> l "ffr nos" >> return (stmt, ((-1), Nothing))
               Just cmstmt -> withStmt cmstmt $ \cstmt ->
                 do l $ "ffetchrow: " ++ show nextrow
                    numrows <- pqntuples cstmt
                    l $ "numrows: " ++ show numrows
                    if nextrow >= numrows
                       then do l "no more rows"
                               -- Don't use public_ffinish here
                               ffinish cmstmt
                               return (Nothing, ((-1), Nothing))
                       else do l "getting stuff"
                               ncols <- pqnfields cstmt
                               res <- mapM (getCol cstmt nextrow)
                                      [0..(ncols - 1)]
                               return (stmt, (nextrow + 1, Just res))
          getCol p row icol =
             do isnull <- pqgetisnull p row icol
                if isnull /= 0
                   then return SqlNull
                   else do text <- pqgetvalue p row icol
                           coltype <- liftM oidToColType $ pqftype p icol
                           s <- B.packCString text
                           makeSqlValue coltype s



fgetcoldef :: Ptr CStmt -> IO [(String, SqlColDesc)]
fgetcoldef cstmt =
    do ncols <- pqnfields cstmt
       mapM desccol [0..(ncols - 1)]
    where desccol i =
              do colname <- peekCStringUTF8 =<< pqfname cstmt i 
                 coltype <- pqftype cstmt i
                 --coloctets <- pqfsize
                 let coldef = oidToColDef coltype
                 return (colname, coldef)

-- FIXME: needs a faster algorithm.
fexecutemany :: SState -> [[SqlValue]] -> IO ()
fexecutemany _ [] = return ()
fexecutemany sstate rows =
    withConnLocked (dbo sstate) $ \ cconn ->
    case preparedStatement sstate of
        Nothing -> throwSqlError $ SqlError "" (-1)
            "internal error: fexecutemany should never be called on non-prepared statements"
        Just (PreparedStatement stmtName types) ->
            let len = length types
            in B.useAsCString (BUTF8.fromString stmtName) $ \ cStmtName ->
               withArray (replicate len nullPtr) $ \ cvalues ->
               withArray (replicate len 0) $ \ clengths ->
               withArray (replicate len 0) $ \ cformats -> do
                   let processRow :: [SqlValue] -> IO Int
                       processRow = fexecuteOnArrays sstate cconn
                                           (CPreparedStatement cStmtName types)
                                           cvalues clengths cformats
                   mapM_ processRow rows

-- Left for use with PQexecParams, Right for PQexecPrepared
data CQueryInformation
    = CQuery CString (Ptr #{type Oid})
        -- For non-prepared statements.
        -- (executeMany is not possible.)
        -- 'fexecuteOnArrays' has to write the types into the c-array
        -- corresponding to the processed values.
    | CPreparedStatement CString [#{type Oid}]
        -- For prepared statements.
        -- 'fexecuteOnArrays' has to check that the processed values
        -- match the types in this list.

-- Converts the given SqlValues to CChar arrays;
-- pokes the corresponding types (for non-prepared statements) or
-- checks type correctness (for prepared statements);
-- writes format and length information and
-- executes either pqexecParams or pqexecPrepared.
fexecuteOnArrays :: (Read a, Num a) =>
       SState -> Ptr CConn
    -> CQueryInformation
    -> Ptr (Ptr CChar) -> Ptr CInt -> Ptr CInt
    -> [SqlValue] -> IO a
fexecuteOnArrays sstate cconn cqueryinformation cvalues clengths cformats row = do
    converted <- mapM convert row
    checkOrPokeTypes converted
    -- poke the rest
    forM_ (zip [0 ..] converted) $ \ (i, (_ctype, cvalue, clength, cformat)) -> do
        poke (advancePtr cvalues i) cvalue
        poke (advancePtr clengths i) clength
        poke (advancePtr cformats i) cformat

    public_ffinish sstate    -- Sets nextrowmv to -1
    resptr <- case cqueryinformation of
        CQuery cquery ctypes ->
            pqexecParams cconn cquery
                (genericLength row) ctypes cvalues clengths cformats 0
        CPreparedStatement cStmtName _types ->
            pqexecPrepared cconn cStmtName
                (genericLength row) cvalues clengths cformats 0
    freeCValues
    handleResultStatus cconn resptr sstate =<< pqresultStatus resptr
  where
    checkOrPokeTypes :: [(#{type Oid}, Ptr CChar, CInt, CInt)] -> IO ()
    checkOrPokeTypes converted = case cqueryinformation of
        CQuery _query ctypes ->
            -- un-prepared query: types get poked
            forM_ (zip [0 ..] converted) $ \ (i, (ctype, _, _, _)) ->
                poke (advancePtr ctypes i) ctype
        CPreparedStatement _cStmtName types ->
            -- prepared statement: check types
            forM_ (zip types converted) $ \ x -> case x of
                (_       , (_, _, _, 0)) -> return ()
                (expected, (ctype, _, _, _)) ->
                    when (expected /= ctype) $
                        throwSqlError $ SqlError "" (-1)
                            ("type mismatch during execution of prepared query. \n" ++
                             "query: " ++ squery sstate ++ "\n" ++
                             "given values: " ++ show row)

    convert :: SqlValue -> IO (#{type Oid}, Ptr CChar, CInt, CInt)
    convert SqlNull = return (0, nullPtr, 0, 0)
    convert (SqlDouble d) = do
        vPtr <- malloc
        poke vPtr d
        networkEndian <- revertBytes 8 (castPtr vPtr)
        return (#{const PG_TYPE_FLOAT8}, castPtr networkEndian, 8, 1)
    convert (SqlInt64 i) = do
        vPtr <- malloc
        poke vPtr i
        networkEndian <- revertBytes 8 (castPtr vPtr)
        return (#{const PG_TYPE_INT8}, castPtr networkEndian, 8, 1)
    convert (SqlInt32 i) = do
        vPtr <- malloc
        poke vPtr i
        networkEndian <- revertBytes 4 (castPtr vPtr)
        return (#{const PG_TYPE_INT4}, castPtr networkEndian, 4, 1)
    convert (SqlByteString x) = do
        vPtr <- cstrUtf8BString (toHex x)
        return (0, vPtr, 0, 0)
    convert x@(SqlUTCTime _) = convert (SqlZonedTime (fromSql x))
    convert x@(SqlEpochTime _) = convert (SqlZonedTime (fromSql x))
    convert x = do
        vPtr <- cstrUtf8BString (fromSql x)
        return (0, vPtr, 0, 0)

    freeCValues :: IO ()
    freeCValues = forM_ [0 .. pred (length row)] $ \ i -> do
        ptr <- peek (advancePtr cvalues i)
        when (ptr /= nullPtr) $
            free ptr


-- Finish and change state
public_ffinish :: SState -> IO ()
public_ffinish sstate =
    do l "public_ffinish"
       _ <- swapMVar (nextrowmv sstate) (-1)
       modifyMVar_ (stomv sstate) worker
    where worker Nothing = return Nothing
          worker (Just sth) = ffinish sth >> return Nothing

ffinish :: Stmt -> IO ()
ffinish p = withRawStmt p $ pqclear

foreign import ccall unsafe "libpq-fe.h PQresultStatus"
  pqresultStatus :: (Ptr CStmt) -> IO #{type ExecStatusType}

foreign import ccall safe "libpq-fe.h PQexecParams"
  pqexecParams :: (Ptr CConn) -> CString -> CInt ->
                  (Ptr #{type Oid}) ->
                  (Ptr CString) ->
                  (Ptr CInt) ->
                  (Ptr CInt) ->
                  CInt ->
                  IO (Ptr CStmt)

-- We cannot just use pqexecParams to execute "EXECUTE"-statements for
-- prepared statements since that does not support parameters
-- ($1, $2, ...). See here:
-- http://postgresql.1045698.n5.nabble.com/Executing-prepared-statements-via-bind-params-td4496507.html
foreign import ccall safe "libpq-fe.h PQexecPrepared"
  pqexecPrepared :: (Ptr CConn) -> CString -> CInt ->
                    (Ptr CString) ->
                    (Ptr CInt) ->
                    (Ptr CInt) ->
                    CInt ->
                    IO (Ptr CStmt)

foreign import ccall safe "libpq-fe.h PQdescribePrepared"
  pqdescribePrepared :: Ptr CConn -> CString -> IO (Ptr PQdescribePreparedResult)

foreign import ccall safe "libpq-fe.h PQexec"
  pqexec :: (Ptr CConn) -> CString -> IO (Ptr CStmt)

foreign import ccall unsafe "hdbc-postgresql-helper.h PQclear_app"
  pqclear :: Ptr WrappedCStmt -> IO ()

foreign import ccall unsafe "hdbc-postgresql-helper.h &PQclear_finalizer"
  pqclearptr :: FunPtr (Ptr WrappedCStmt -> IO ())

foreign import ccall unsafe "libpq-fe.h PQclear"
  pqclear_raw :: Ptr CStmt -> IO ()

foreign import ccall unsafe "hdbc-postgresql-helper.h wrapobjpg"
  wrapstmt :: Ptr CStmt -> Ptr WrappedCConn -> IO (Ptr WrappedCStmt)

foreign import ccall unsafe "libpq-fe.h PQcmdTuples"
  pqcmdTuples :: Ptr CStmt -> IO CString
foreign import ccall unsafe "libpq-fe.h PQresStatus"
  pqresStatus :: #{type ExecStatusType} -> IO CString

foreign import ccall unsafe "libpq-fe.h PQresultErrorMessage"
  pqresultErrorMessage :: (Ptr CStmt) -> IO CString

foreign import ccall unsafe "libpq-fe.h PQresultErrorField"
  pqresultErrorField :: (Ptr CStmt) -> CInt -> IO CString

foreign import ccall unsafe "libpq-fe.h PQntuples"
  pqntuples :: Ptr CStmt -> IO CInt

foreign import ccall unsafe "libpq-fe.h PQnfields"
  pqnfields :: Ptr CStmt -> IO CInt

foreign import ccall unsafe "libpq-fe.h PQgetisnull"
  pqgetisnull :: Ptr CStmt -> CInt -> CInt -> IO CInt

foreign import ccall unsafe "libpq-fe.h PQgetvalue"
  pqgetvalue :: Ptr CStmt -> CInt -> CInt -> IO CString

foreign import ccall unsafe "libpq-fe.h PQfname"
  pqfname :: Ptr CStmt -> CInt -> IO CString

foreign import ccall unsafe "libpq-fe.h PQftype"
  pqftype :: Ptr CStmt -> CInt -> IO #{type Oid}

foreign import ccall unsafe "libpq-fe.h PQnparams"
  pqnparams :: Ptr PQdescribePreparedResult -> IO Int

foreign import ccall unsafe "libpq-fe.h PQparamtype"
  pqparamtype :: Ptr PQdescribePreparedResult -> Int -> IO #{type Oid}

-- SqlValue construction function and helpers

-- Make a SqlValue for the passed column type and string value, where it is assumed that the value represented is not the Sql null value.
-- The IO Monad is required only to obtain the local timezone for interpreting date/time values without an explicit timezone.
makeSqlValue :: SqlTypeId -> B.ByteString -> IO SqlValue
makeSqlValue sqltypeid bstrval =
    let strval = BUTF8.toString bstrval
    in
    case sqltypeid of

      tid | tid == SqlCharT        ||
            tid == SqlVarCharT     ||
            tid == SqlLongVarCharT ||
            tid == SqlWCharT       ||
            tid == SqlWVarCharT    ||
            tid == SqlWLongVarCharT  -> return $ SqlByteString bstrval

      tid | tid == SqlDecimalT ||
            tid == SqlNumericT   -> return $ SqlRational (makeRationalFromDecimal strval)

      tid | tid == SqlSmallIntT ||
            tid == SqlTinyIntT  ||
            tid == SqlIntegerT     -> return $ SqlInt32 (read strval)

      SqlBigIntT -> return $ SqlInteger (read strval)

      tid | tid == SqlRealT   ||
            tid == SqlFloatT  ||
            tid == SqlDoubleT   -> return $ SqlDouble (read strval)

      SqlBitT -> return $ case strval of
                   't':_ -> SqlBool True
                   'f':_ -> SqlBool False
                   'T':_ -> SqlBool True -- the rest of these are here "just in case", since they are legal as input
                   'y':_ -> SqlBool True
                   'Y':_ -> SqlBool True
                   "1"   -> SqlBool True
                   _     -> SqlBool False

      -- Dates and Date/Times
      tid | tid == SqlDateT -> return $ SqlLocalDate (fromSql (toSql strval))
      tid | tid == SqlTimestampWithZoneT -> return $ SqlZonedTime (fromSql (toSql (fixString strval)))

          -- SqlUTCDateTimeT not actually generated by PostgreSQL

      tid | tid == SqlTimestampT   ||
            tid == SqlUTCDateTimeT   -> return $ SqlLocalTime (fromSql (toSql strval))

      -- Times without dates
      tid | tid == SqlTimeT    ||
            tid == SqlUTCTimeT   -> return $ SqlLocalTimeOfDay (fromSql (toSql strval))

      tid | tid == SqlTimeWithZoneT ->
              (let (a, b) = case (parseTime defaultTimeLocale "%T%Q %z" timestr,
                                  parseTime defaultTimeLocale "%T%Q %z" timestr) of
                                (Just x, Just y) -> (x, y)
                                x -> error $ "PostgreSQL Statement.hsc: Couldn't parse " ++ strval ++ " as SqlZonedLocalTimeOfDay: " ++ show x
                   timestr = fixString strval
               in return $ SqlZonedLocalTimeOfDay a b)

      SqlIntervalT _ -> return $ SqlDiffTime $ fromRational $
                         case split ':' strval of
                           [h, m, s] -> toRational (((read h)::Integer) * 60 * 60 +
                                                    ((read m)::Integer) * 60) +
                                        toRational ((read s)::Double)
                           _ -> error $ "PostgreSQL Statement.hsc: Couldn't parse interval: " ++ strval

      -- TODO: For now we just map the binary types to SqlByteStrings. New SqlValue constructors are needed to handle these.
      tid | tid == SqlBinaryT        ||
            tid == SqlVarBinaryT     ||
            tid == SqlLongVarBinaryT    -> case fromHex bstrval of
                Left msg -> throwSqlError $ SqlError "" (-1) msg
                Right bs -> return $ SqlByteString bs

      SqlGUIDT -> return $ SqlByteString bstrval

      SqlUnknownT _ -> return $ SqlByteString bstrval
      _ -> error $ "PostgreSQL Statement.hsc: unknown typeid: " ++ show sqltypeid

-- Convert "15:33:01.536+00" to "15:33:01.536 +0000"
fixString :: String -> String
fixString s =
    let (strbase, zone) = splitAt (length s - 3) s
    in
      if (head zone) == '-' || (head zone) == '+'
         then strbase ++ " " ++ zone ++ "00"
         else -- It wasn't in the expected format; don't touch.
              s


-- Make a rational number from a decimal string representation of the number.
makeRationalFromDecimal :: String -> Rational
makeRationalFromDecimal s =
    case elemIndex '.' s of
      Nothing -> toRational ((read s)::Integer)
      Just dotix ->
        let (nstr,'.':dstr) = splitAt dotix s
            num = (read $ nstr ++ dstr)::Integer
            den = 10^((genericLength dstr) :: Integer)
        in
          num % den

split :: Char -> String -> [String]
split delim inp =
    lines . map (\x -> if x == delim then '\n' else x) $ inp
