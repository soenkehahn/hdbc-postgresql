

import Control.Exception
import Database.HDBC
import Database.HDBC.PostgreSQL
import Criterion.Main
import SpecificDB


main = do
    defaultMain $
        bench "mapM_ execute" insertWithMapMExecute :
        bench "executeMany" insertWithExecuteMany :
        []

ts :: [[SqlValue]]
ts = replicate 500 [SqlString "teststring", SqlInt64 23, SqlDouble 51.3]

setUp :: IO Connection
setUp = do
    db <- connectDB
    runRaw db "DROP TABLE IF EXISTS testtable;"
    runRaw db "CREATE TABLE testtable (a text, b bigint, c double precision);"
    commit db
    return db

tearDown :: Connection -> IO ()
tearDown db = do
    runRaw db "DROP TABLE testtable;"
    commit db
    disconnect db

insertWithMapMExecute :: IO ()
insertWithMapMExecute =
    bracket setUp tearDown $ \ db -> do
        stmt <- prepare db "INSERT INTO testtable (a, b, c) VALUES (?, ?, ?);"
        mapM (execute stmt) ts
        commit db

insertWithExecuteMany :: IO ()
insertWithExecuteMany =
    bracket setUp tearDown $ \ db -> do
        stmt <- prepare db "INSERT INTO testtable (a, b, c) VALUES (?, ?, ?);"
        executeMany stmt ts
        commit db
