{-# LANGUAGE OverloadedStrings #-}

module Lib

where

import           Control.Applicative
import           Control.Monad
import           Data.Aeson
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import           Data.Functor
import           Data.Monoid
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import           Database.Redis

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
addKeys :: (RedisCtx m f, ToJSON a, Applicative f) => (T.Text -> [[T.Text]]) -> RTrieConfig -> T.Text -> T.Text -> a -> m (f Integer)
addKeys fixes cfg key id val =
    let pps = fixes key
        bid = TE.encodeUtf8 id
        mk = TE.encodeUtf8 $ metaKey cfg
        addtrie i p = do
          rs <- zadd (TE.encodeUtf8 $ indexKey cfg <> p) [(0.0, bid)]
          pure $ (+) <$> i <*> rs
        addtries ps = foldM addtrie (pure 0) ps
        addtriesS s ps = do
          rs <- addtries ps
          pure $ (+) <$> s <*> rs
    in do
      s <- foldM addtriesS (pure 0) pps
      hset mk bid (BL.toStrict $ encode val)
      return s

-- | Add all prefix keys for given key alongwith its id and value
addPrefixKeys :: (RedisCtx m f, ToJSON a, Applicative f) => RTrieConfig -> T.Text -> T.Text -> a -> m (f Integer)
addPrefixKeys = addKeys prefixes


-- | Add all suffix keys for given key alongwith its id and value
addSuffixKeys :: (RedisCtx m f, ToJSON a, Applicative f) => RTrieConfig -> T.Text -> T.Text -> a -> m (f Integer)
addSuffixKeys = addKeys suffixes

-- | Add all prefix and suffixes for the given key alongwith its id and values
addPrefixSuffixKeys :: (RedisCtx m f, ToJSON a, Applicative f) => RTrieConfig -> T.Text -> T.Text -> a -> m (f Integer)
addPrefixSuffixKeys cfg k id v = do
    sp <- addPrefixKeys cfg k id v
    ss <- addSuffixKeys cfg k id v
    return $ (+) <$> sp <*> ss

-- | 
-- searchKeys :: (RedisCtx m f, FromJSON a, Applicative f) => RTrieConfig -> T.Text -> Integer -> Integer -> m (f [a])
searchkeys cfg k index limit =
    let sk = TE.encodeUtf8 $ T.strip k
        mk = TE.encodeUtf8 $ metaKey cfg
    in do
      ss <- zrevrange sk index limit
      hmget mk <$> ss
  
