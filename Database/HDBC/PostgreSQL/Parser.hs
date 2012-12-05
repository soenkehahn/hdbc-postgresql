{- -*- mode: haskell; -*- 
-}
{- PostgreSQL uses $1, $2, etc. instead of ? in query strings.  So we have to
do some basic parsing on these things to fix 'em up. -}


module Database.HDBC.PostgreSQL.Parser where

import Text.ParserCombinators.Parsec

escapeseq :: GenParser Char st String
escapeseq = (try $ string "''") <|>
            (try $ string "\\'")

literal :: GenParser Char st [Char]
literal = do _ <- char '\''
             s <- many (escapeseq <|> (noneOf "'" >>= (\x -> return [x])))
             _ <- char '\''
             return $ "'" ++ (concat s) ++ "'"

qidentifier :: GenParser Char st [Char]
qidentifier = do _ <- char '"'
                 s <- many (noneOf "\"")
                 _ <- char '"'
                 return $ "\"" ++ s ++ "\""

comment :: GenParser Char st [Char]
comment = ccomment <|> linecomment

ccomment :: GenParser Char st [Char]
ccomment = do _ <- string "/*"
              c <- manyTill ((try ccomment) <|> 
                             (anyChar >>= (\x -> return [x])))
                   (try (string "*/"))
              return $ "/*" ++ concat c ++ "*/"

linecomment :: GenParser Char st [Char]
linecomment = do _ <- string "--"
                 c <- many (noneOf "\n")
                 _ <- char '\n'
                 return $ "--" ++ c ++ "\n"

-- FIXME: handle pgsql dollar-quoted constants

qmark :: (Num st, Show st) => GenParser Char st [Char]
qmark = do _ <- char '?'
           n <- getState
           updateState (+1)
           return $ "$" ++ show n

escapedQmark :: GenParser Char st [Char]
escapedQmark = do _ <- try (char '\\' >> char '?')
                  return "?"

statement :: (Num st, Show st) => GenParser Char st [Char]
statement = 
    do s <- many ((try escapedQmark) <|>
                  (try qmark) <|>
                  (try comment) <|>
                  (try literal) <|>
                  (try qidentifier) <|>
                  (anyChar >>= (\x -> return [x])))
       return $ concat s

-- | replaces (unescaped) question marks with postgres-style
-- numbered parameters ("$1", "$2", ...).
-- Also, returns the number of replaced parameters.
convertSQL :: String -> Either ParseError (String, Int)
convertSQL input =
    fmap (\ (state, result) -> (result, fromIntegral $ pred state)) $
    runStateParser statement (1::Integer) "" input

runStateParser :: GenParser Char st String -> st -> String -> String -> Either ParseError (st, String)
runStateParser p initial source input = runParser wrapped initial source input
  where
    wrapped = do
        r <- p
        n <- getState
        return (n, r)
