{-# language ScopedTypeVariables, FlexibleContexts #-}

module Properties where


import Control.Applicative
import Control.Exception
import Database.HDBC
import Data.ByteString (ByteString, pack, unpack)
import Data.Convertible
import Data.Int
import Test.HUnit
import Test.HUnit.Tools
import Test.QuickCheck
import Test.QuickCheck.Property
import SpecificDB


tests :: Test
tests = TestList $
    qctest "identityString" identityString :
    qctest "identityBytestring" identityBytestring :
    qctest "identityDouble" identityDouble :
    qctest "identityInt64" identityInt64 :
    qctest "identityInt32" identityInt32 :
    []


-- * utils

data Phantom a = P

epsilonEquals :: (Ord a, Num a) => a -> a -> a -> Bool
epsilonEquals epsilon a b =
    abs (a - b) <= epsilon

equals :: (Show a) => (a -> a -> Bool) -> a -> a -> Property
equals (~=) a b =
    printTestCase (show a ++ " /~= " ++ show b)
        (a ~= b)

instance Arbitrary ByteString where
    arbitrary = pack <$> arbitrary
    shrink xs = pack <$> shrink (unpack xs)



-- * properties

identityString :: Property
identityString = identity (P :: Phantom String) "text" (\ text -> not (elem '\NUL' text)) (==)

identityBytestring :: Property
identityBytestring = identity (P :: Phantom ByteString) "bytea" (const True) (==)

identityDouble :: Property
identityDouble = identity (P :: Phantom Double) "double precision" (const True) (epsilonEquals 0.00000001)

identityInt64 :: Property
identityInt64 = identity (P :: Phantom Int64) "bigint" (const True) (==)

identityInt32 :: Property
identityInt32 = identity (P :: Phantom Int32) "integer" (const True) (==)

-- | Tests if a value (that fulfills a given precondition) can be put in
--   the database and read back and if that output is equal to the input
--   (using a given equality function).
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
