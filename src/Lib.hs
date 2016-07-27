{-# LANGUAGE OverloadedStrings #-}

module Lib

where

import           Control.Applicative
import           Control.Monad
import           Data.Aeson
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import           Data.Functor
import           Data.Monoid
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import           Database.Redis as R

-- | Trie configuration
-- Initialize key prefix for creating trie in redis
data RTrieConfig = RTrieConfig { indexKey :: T.Text
                               , metaKey :: T.Text
                               } deriving Show


prefixes :: T.Text -> [[T.Text]]
prefixes = map (tail . T.inits) . T.words . T.strip

suffixes :: T.Text -> [[T.Text]]
suffixes = map (init . T.tails) . T.words . T.strip


-- | Add search key, an unique identifier and value to trie set.
-- Add each sub key to a sorted set with value of the ID.
addKeys :: (RedisCtx m f, Applicative f) => (T.Text -> [[T.Text]]) -> RTrieConfig -> T.Text -> T.Text -> m (f Integer)
addKeys fixes cfg key id =
    let pps = fixes key
        bid = TE.encodeUtf8 id
        addtrie i p = do
          rs <- zadd (TE.encodeUtf8 $ indexKey cfg <> p) [(0.0, bid)]
          pure $ (+) <$> i <*> rs
        addtries ps = foldM addtrie (pure 0) ps
        addtriesS s ps = do
          rs <- addtries ps
          pure $ (+) <$> s <*> rs
    in foldM addtriesS (pure 0) pps




addValue :: (ToJSON a, RedisCtx m f, Applicative f) => RTrieConfig -> T.Text -> a -> m (f Bool)
addValue cfg id val = hset mk bid (BL.toStrict $ encode val)
    where
      mk = TE.encodeUtf8 $ metaKey cfg
      bid = TE.encodeUtf8 id

-- delKeys :: (RedisCtx m f, Applicative f) => (T.Text -> [[T.Text]]) 

-- | Add all prefix keys for given key alongwith its id and value
addPrefixKeys :: (RedisCtx m f, ToJSON a, Applicative f) => RTrieConfig -> T.Text -> T.Text -> a -> m (f Integer)
addPrefixKeys cfg key id val = do
  num <- addKeys prefixes cfg key id
  status <- addValue cfg id val
  return $ num <* status


-- | Add all suffix keys for given key alongwith its id and value
addSuffixKeys :: (RedisCtx m f, ToJSON a, Applicative f) => RTrieConfig -> T.Text -> T.Text -> a -> m (f Integer)
addSuffixKeys cfg key id val = do
  num <- addKeys suffixes cfg key id
  status <- addValue cfg id val
  return $ num <* status

-- | Add all prefix and suffixes for the given key alongwith its id and values
addPrefixSuffixKeys :: (RedisCtx m f, ToJSON a, Applicative f) => RTrieConfig -> T.Text -> T.Text -> a -> m (f Integer)
addPrefixSuffixKeys cfg k id v = do
    sp <- addKeys prefixes cfg k id
    ss <- addKeys suffixes cfg k id
    status <- addValue cfg id v
    return $ ( (+) <$> sp <*> ss ) <* status

getAllKeys :: RedisCtx m f => RTrieConfig -> m (f [B.ByteString])
getAllKeys cfg = keys (TE.encodeUtf8 $ indexKey cfg <> "*")

delAllKeys :: (Applicative f, RedisCtx m f) => [B.ByteString] -> m (f Integer)
delAllKeys ks = del ks


--delKeys :: Connection -> RTrieConfig -> IO Integer
delKeys conn cfg = runRedis conn $ do
                     keys <- getAllKeys cfg
                     let allkeys = (mk :) <$> keys
                     case allkeys of
                       Right ks -> do
                                    trs <- multiExec (delAllKeys ks)
                                    case trs of
                                      TxSuccess x -> return (Right x)
                                      TxAborted   -> return $ Left (R.Error "Transaction aborted")
                                      TxError s   -> return $ Left (R.Error $ B8.pack s)
                       Left  rp -> return $ Left rp
    where
      mk = TE.encodeUtf8 $ metaKey cfg
  

--searchKeys :: (RedisCtx m f, FromJSON a, Applicative f) => RTrieConfig -> T.Text -> Integer -> Integer -> m (f [a])
searchSubKeys cfg k index limit =
    let sk = TE.encodeUtf8 $ T.strip k
        mk = TE.encodeUtf8 $ metaKey cfg
    in zrevrange sk index limit

getIDs 
  
