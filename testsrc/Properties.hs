{-# language ScopedTypeVariables, FlexibleContexts #-}

module Properties where


import Control.Applicative
import Control.Exception
import Database.HDBC
import Data.ByteString (ByteString, pack, unpack)
import Data.Convertible
import Data.Foldable (forM_)
import Data.Int
import Data.List
import Data.Set as Set (Set, empty, insert, fromList, toList)
import Test.HUnit
import Test.HUnit.Tools
import Test.QuickCheck
import Test.QuickCheck.Property
import SpecificDB


containsNull :: Maybe String -> Bool
containsNull = maybe False (elem '\NUL')

epsilonEquals :: (Ord a, Num a) => a -> Maybe a -> Maybe a -> Bool
epsilonEquals epsilon (Just a) (Just b) =
    abs (a - b) <= epsilon
epsilonEquals _ Nothing Nothing = True
epsilonEquals _ _ _ = False

tests :: Test
tests = TestList (map (\ (name, property) -> qctest name property) properties)

properties :: [(String, Property)]
properties =
    ("identityString", identity (P :: Phantom (Maybe String)) "text" (not . containsNull) (==)) :
    ("identityManyString", identityMany (P :: Phantom (Maybe String)) "text" (not . containsNull) (==)) :
    ("identityBytestring", identity (P :: Phantom (Maybe ByteString)) "bytea" (const True) (==)) :
    ("identityManyBytestring", identityMany (P :: Phantom (Maybe ByteString)) "bytea" (const True) (==)) :
    ("identityDouble", identity (P :: Phantom (Maybe Double)) "double precision" (const True) (epsilonEquals 0.00000001)) :
    ("identityManyDouble", identityMany (P :: Phantom (Maybe Double)) "double precision" (const True) (epsilonEquals 0.00000001)) :
    ("identityInt64", identity (P :: Phantom (Maybe Int64)) "bigint" (const True) (==)) :
    ("identityManyInt64", identityMany (P :: Phantom (Maybe Int64)) "bigint" (const True) (==)) :
    ("identityInt32", identity (P :: Phantom (Maybe Int32)) "integer" (const True) (==)) :
    ("identityManyInt32", identityMany (P :: Phantom (Maybe Int32)) "integer" (const True) (==)) :
    []


-- * utils

data Phantom a = P

equals :: (Show a) => (a -> a -> Bool) -> a -> a -> Property
equals (~=) a b =
    printTestCase (show a ++ " /~= " ++ show b)
        (a ~= b)

equalSets :: (Show a, Ord a) => (a -> a -> Bool) -> Set a -> Set a -> Property
equalSets (~=) a b =
    printTestCase (show a ++ " /~= " ++ show b) $
    inner (sort $ Set.toList a) (sort $ Set.toList b)
  where
    inner [] [] = True
    inner (a : ra) (b : rb) = (a ~= b) && inner ra rb
    inner _ _ = False

instance Arbitrary ByteString where
    arbitrary = pack <$> arbitrary
    shrink xs = pack <$> shrink (unpack xs)



-- * generic properties

-- | Tests if a value (that fulfills a given precondition) can be put in
-- the database and read back and if that output is equal to the input
-- (using a given equality function).
identity :: forall a . (Arbitrary a, Show a, Eq a, Convertible a SqlValue, Convertible SqlValue a) =>
    Phantom a -> String -> (a -> Bool) -> (a -> a -> Bool) -> Property
identity _ sqlTypeName pred (~=) =
    property $ \ (a :: a) ->
    (pred a) ==>
    morallyDubiousIOProperty $
        -- connect to the db
        bracket connectDB disconnect $ \ db ->
        -- create a temporary test table
        bracket
            (quickQuery' db ("CREATE TABLE testtable (testfield " ++ sqlTypeName ++ ");") [])
            (const $ quickQuery' db "DROP TABLE testtable;" []) $
            const $ do
                quickQuery' db "INSERT INTO testtable VALUES (?);" [toSql a]
                [[result]] <- quickQuery' db "SELECT * FROM testtable;" []
                return $ (equals (~=) a (fromSql result))

-- | Like identity, but with multiple values (in multiple rows).
identityMany :: forall a . (Arbitrary a, Show a, Ord a, Convertible a SqlValue, Convertible SqlValue a) =>
    Phantom a -> String -> (a -> Bool) -> (a -> a -> Bool) -> Property
identityMany _ sqlTypeName pred (~=) =
    property $ \ (xs :: [a]) ->
    (all pred xs) ==>
    morallyDubiousIOProperty $
        -- connect to the db
        bracket connectDB disconnect $ \ db ->
        -- create a temporary test table
        bracket
            (quickQuery' db ("CREATE TABLE testtable (testfield " ++ sqlTypeName ++ ");") [])
            (const $ quickQuery' db "DROP TABLE testtable;" []) $
            const $ do
                stmt <- prepare db "INSERT INTO testtable VALUES (?);"
                executeMany stmt $ map ((: []) . toSql) xs
                results <- quickQuery' db "SELECT testfield FROM testtable;" []
                return $ (equalSets (~=) (Set.fromList xs) (convert results))
  where
    convert :: [[SqlValue]] -> Set a
    convert ([a] : r) = Set.insert (fromSql a) (convert r)
    convert [] = Set.empty
