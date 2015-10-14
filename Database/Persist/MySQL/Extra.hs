{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE ConstraintKinds           #-}
{-# LANGUAGE OverloadedStrings         #-}
module Database.Persist.MySQL.Extra
  ( rawSqlSource
  , selectKeysBy
  , insertOrUpdateMany_
  , insertOrUpdate_
  , insertOrUpdateUniqueMany_
  , insertOrUpdateUniqueMany
  , insertOrUpdateUnique_
  --, insertOrUpdateUnique
  , repsertUniqueMany_
  , repsertUniqueMany
  , repsertUnique_
  , repsertUnique
  , insertMany_
  , SqlWaitException
  , SqlPriority (..)
  , DupUpdate (..)
  ) where

import           Control.Arrow                 (left)
import           Control.Applicative           ((<$>), (<*>))
import           Control.Exception             (Exception, throwIO, bracket)
import           Control.Monad                 (forM_, when)
import           Control.Monad.Catch           (MonadThrow)
import           Control.Monad.IO.Class        (MonadIO (..), liftIO)
import           Control.Monad.Trans           (lift)
import           Control.Monad.Trans.Reader    (ReaderT)
import           Control.Monad.Trans.Resource  (MonadResource, MonadResourceBase)
import           Control.Monad.Trans.Control   (MonadBaseControl, liftBaseOp)
import           Control.Monad.Reader          (MonadReader, ask)
import           Data.Conduit
import qualified Data.Conduit.List             as CL
import           Data.IORef
import           Data.List                     hiding (insert, insertBy,
                                                maximum, minimum)
import           Data.List.Split               (chunksOf)
import           Data.Map                      (Map)
import qualified Data.Map                      as M
import           Data.Maybe
import           Data.Monoid                   ((<>))
import           Data.Proxy
import           Data.Text                     (Text)
import qualified Data.Text                     as T
import           Data.Typeable                 (Typeable)
import           Database.Persist.Class hiding (insertMany_)
import           Database.Persist.Sql hiding (insertMany_)
--import           Database.Persist.Types
import           Control.Concurrent.MVar.Lifted
--import Control.Exception.Lifted (bracket, )
import           Prelude                       hiding (head, init, last, tail)
import           Safe
import           System.IO.Unsafe              (unsafePerformIO)

-- | SqlPriority is similar to LOW PRIORITY option with on myisam table, but a lock is used on the server itself
--   and will throw an exception when the number of queued queries exceeds some limit
data SqlWaitException = SqlWaitException Text
  deriving (Show, Typeable)
data SqlPriority = LowPriority | NormalPriority -- TODO: Delayed (allows the thread to continue while a new thread waits for lock access)

-- | Field to use when updating a duplicate entity
-- TODO: insertOrUpdate with [Update val / EntityField]
data DupUpdate record = forall typ. PersistField typ => DupUpdateField (EntityField record typ)

instance Exception SqlWaitException
--instance Error SqlWaitException where
--  strMsg = PersistError . pack


-- Use these semaphores to implement low priority updates in mysql with InnoDB tables
updateSems :: IORef (Map Text (MVar (), IORef Int))
{-# NOINLINE updateSems #-}
updateSems = unsafePerformIO $ newIORef M.empty

-- The maximum number of waiting updates. Whenever LowPriority inserts/updates are used, these methods will throw an exception whenever the maximum number of queries become enqueued.
-- (This could happen during periods of high traffic or in a denial-of-service attack)
maxWaitingQueries :: Int
maxWaitingQueries = 20

-- | Execute a raw SQL statement and return its results as a
--   Source.
rawSqlSource :: (MonadResource m, MonadReader SqlBackend m, RawSql a)
       => Text             -- ^ SQL statement, possibly with placeholders.
       -> [PersistValue]   -- ^ Values to fill the placeholders.
       -> Source m a
rawSqlSource stmt = run
    where
      getType :: (x -> Source m a) -> a
      getType = error "rawSqlSource.getType"

      x = getType run

      process :: (RawSql a) => [PersistValue] -> Either Text a
      process = rawSqlProcessRow

      -- withStmt' :: (MonadResource m) => [Text] -> [PersistValue] -> Source m [PersistValue]
      withStmt' colSubsts params = rawQuery sql params
          where
            sql = T.concat $ makeSubsts colSubsts $ T.splitOn placeholder stmt
            placeholder = "??"
            makeSubsts (s:ss) (t:ts) = t : s : makeSubsts ss ts
            makeSubsts []     []     = []
            makeSubsts []     ts     = [T.intercalate placeholder ts]
            makeSubsts ss     []     = error (concat err)
                where
                  err = [ "rawsql: there are still ", show (length ss)
                        , "'??' placeholder substitutions to be made "
                        , "but all '??' placeholders have already been "
                        , "consumed.  Please read 'rawSql's documentation "
                        , "on how '??' placeholders work."
                        ]

      -- run :: (RawSql a, MonadSqlPersist m, MonadResource m) => [PersistValue] -> Source m a
      run params = do
        conn <- ask
        let (colCount, colSubsts) = rawSqlCols (connEscapeName conn) x
        withStmt' colSubsts params $= getRow colCount

      getRow :: (RawSql a, MonadResource m) => Int -> Conduit [PersistValue] m a
      getRow colCount = awaitForever $ \row -> do
        if colCount == length row
        then getter row >>= yield
        else fail $ concat
              [ "rawSql: wrong number of columns, got "
              , show (length row), " but expected ", show colCount
              , " (", rawSqlColCountReason x, ")."
              ]

      getter :: (RawSql a, MonadResource m) => [PersistValue] -> m a
      getter row = case process row of
                    Left err -> fail (T.unpack err)
                    Right r  -> return r

--selectKeysBy :: (MonadResourceBase m, PersistEntity val, PersistEntityBackend val ~ PersistMonadBackend m, MonadSqlPersist m, MonadLogger m) =>
--                [Unique val] -> [SelectOpt val] -> Source m (Key val)
--selectKeysBy []    _    = CL.sourceList []
--selectKeysBy uniqs opts = do
--  conn <- lift askSqlConn
--  if map (orderClause False conn) orders /= []
--  then error "ORDER BY clause is not supported by selectKeysBy, use selectKeysByUnordered instead"
--  else do
--    let esc        = connEscapeName conn
--        cols       = case entityPrimary t of
--                     Just pdef -> T.intercalate "," $ map (esc . snd) $ primaryFields pdef
--                     Nothing   -> esc $ entityID t
--        wher uniq  = " WHERE (" <> (flip T.snoc ')' . wherKey . map snd $ persistUniqueToFieldNames uniq)
--        wherKey fs = T.intercalate " AND " $ map ((<> " <=> ?") . esc)  fs
--        sql        = connLimitOffset conn (limit,offset) True $
--                     T.intercalate "\n UNION ALL " $ map (\uniq -> "SELECT " <> cols <> " FROM " <> (esc $ entityDB t) <> wher uniq) uniqs
--        vals       = concatMap persistUniqueToValues uniqs
--    rawQuery sql vals $= CL.mapM parse
--    where
--      t                       = entityDef $ proxyFromUniqs uniqs
--      (limit, offset, orders) = limitOffsetOrder opts

--      --parse :: [PersistValue] -> [Key val]
--      parse xs = case entityPrimary t of
--                    Nothing ->
--                      case xs of
--                         [PersistInt64 x] -> return $ Key $ PersistInt64 x
--                         [PersistDouble x] -> return $ Key $ PersistInt64 (truncate x) -- oracle returns Double
--                         _ -> liftIO $ throwIO $ PersistMarshalError $ "Unexpected in selectKeysBy False: " <> T.pack (show xs)
--                    Just pdef ->
--                         let pks = map fst $ primaryFields pdef
--                             keyvals = map snd $ filter (\(a, _) -> let ret=isJust (find (== a) pks) in ret) $ zip (map fieldHaskell $ entityFields t) xs
--                         in return $ Key $ PersistList keyvals

selectKeysBy
  :: (MonadReader SqlBackend m,ToBackendKey SqlBackend val,MonadResource m,PersistEntity val)
  => [Unique val] -> Source m (Maybe (Key val))
selectKeysBy []     = CL.sourceList []
selectKeysBy uniqs  = do
  conn <- ask
  let esc        = connEscapeName conn
      cols       = case entityPrimary t of
                   Just pdef -> T.intercalate "," $ map (esc . fieldDB) $ compositeFields pdef
                   Nothing   -> esc $ fieldDB $ entityId t
      wher uniq  = " WHERE (" <> (flip T.snoc ')' . wherKey . map snd $ persistUniqueToFieldNames uniq)
      wherKey fs = T.intercalate " AND " $ map ((<> " <=> ?") . esc)  fs
  forM_ uniqs $ \uniq -> do
    let sql        = "SELECT " <> cols <> " FROM " <> (esc $ entityDB t) <> wher uniq
        vals       = persistUniqueToValues uniq
    r <- lift $ listToMaybe <$> (rawQuery sql vals $= CL.mapM parse $$ CL.consume)
    yield r
  --forM_ uniqs $ \uniq -> do
  --  let sql        = "SELECT " <> cols <> " FROM " <> (esc $ entityDB t) <> wher uniq
  --      vals       = persistUniqueToValues uniq
  --  rawQuery sql vals $= CL.mapM parse
  where
    proxy = proxyFromUniqs uniqs
    t = entityDef proxy

    --parse :: [PersistValue] -> [Key val]
    parse xs = case entityPrimary t of
                  Nothing ->
                    case xs of
                       [PersistInt64 x] -> return $ toSqlKey x
                       [PersistDouble x] -> return $ toSqlKey (truncate x) -- oracle returns Double
                       _ -> liftIO $ throwIO $ PersistMarshalError $ "Unexpected in selectKeysBy False: " <> T.pack (show xs)
                  Just pdef ->
                       let pks = map fieldHaskell $ compositeFields pdef
                           keyvals = map snd $ filter
                                        (\(a, _) -> let ret = isJust (find (== a) pks) in ret) $
                                        zip (map fieldHaskell $ entityFields t) xs
                       in case keyvals of
                             [PersistInt64 x] -> return $ toSqlKey x
                             [PersistDouble x] -> return $ toSqlKey (truncate x) -- oracle returns Double
                             _ -> liftIO $ throwIO $ PersistMarshalError $ "Unexpected in selectKeysBy False: " <> T.pack (show xs)

-- | Insert or update values in the database (when a duplicate primary key already exists)
insertOrUpdateMany_'
  :: (MonadIO m,MonadThrow m,MonadBaseControl IO m,PersistEntity r,PersistEntityBackend r ~ SqlBackend)
  => SqlPriority
  -> [Entity r]
  -> [FieldDef]
  -> Control.Monad.Trans.Reader.ReaderT SqlBackend m ()
insertOrUpdateMany_' _        [] _ = return ()
insertOrUpdateMany_' priority es []   = withPriority priority (entityDB . entityDef $ proxyFromEntities es) $ mapM_ (\(Entity key val) -> insertKey key val) es
insertOrUpdateMany_' priority es ufs  = withPriority priority (entityDB t) $ do
  conn <- ask
  let esc           = connEscapeName conn
      insertFields  = esc (fieldDB (entityId t)) : map (esc . fieldDB) (entityFields t)
      cols          = T.intercalate (T.singleton ',') insertFields
      placeholders  = replicateQ $ length insertFields
      updateFields  = map (esc . fieldDB) ufs
      updateCols    = (T.intercalate ", ") $ map (\name -> name <> "=VALUES(" <> name <> ")") updateFields

  rawExecute ( "INSERT INTO "
            <> esc (entityDB t)
            <> " ("
            <> cols
            <> ") VALUES ("
            <> T.intercalate "),(" (replicate (length es) placeholders)
            <> ") ON DUPLICATE KEY UPDATE "
            <> updateCols
            ) $ concatMap (\e -> keyToValues (entityKey e) ++ (map toPersistValue . toPersistFields  $ entityVal e)) es
  where
    t = entityDef $ proxyFromEntities es

    replicateQ :: Int -> Text
    replicateQ = T.intersperse ',' . (flip T.replicate $ T.singleton '?')

insertOrUpdateMany_
  :: (PersistEntity t,PersistEntity r,MonadBaseControl IO m,MonadThrow m,MonadIO m,PersistEntityBackend r ~ SqlBackend)
  => SqlPriority
  -> Int
  -> Bool
  -> [Entity r]
  -> [DupUpdate t]
  -> ReaderT SqlBackend m ()
insertOrUpdateMany_ priority chunk commitChunks rs ufs = mapM_ insertOrUpdateChunk $ chunksOf chunk rs
  where
    fs = map dupUpdateFieldDef ufs

    -- insertOrUpdateChunk :: (MonadResourceBase m, PersistEntity val, PersistStore m) =>
    --                        [Entity val] -> m ()
    insertOrUpdateChunk rs' = do
      insertOrUpdateMany_' priority rs' fs
      when commitChunks transactionSave

insertOrUpdate_
  :: (MonadIO m,MonadThrow m,MonadBaseControl IO m,PersistEntity r,PersistEntity t,PersistEntityBackend r ~ SqlBackend)
  => Entity r -> [DupUpdate t] -> ReaderT SqlBackend (ReaderT SqlBackend m) ()
insertOrUpdate_ r = insertOrUpdateMany_ NormalPriority 1 False [r]

-- | Insert or update values in the database (when a duplicate already exists)
insertOrUpdateUniqueMany_'
  :: (MonadIO m,MonadThrow m,MonadBaseControl IO m,PersistEntity record,PersistEntityBackend record ~ SqlBackend)
  => SqlPriority -> [record] -> [FieldDef] -> ReaderT SqlBackend m ()
insertOrUpdateUniqueMany_' _ [] _   = return ()
insertOrUpdateUniqueMany_' priority rs []  = withPriority priority (entityDB . entityDef $ proxyFromRecords rs) $ mapM_ insertUnique rs -- TODO: insertUniqueMany
insertOrUpdateUniqueMany_' priority rs ufs = withPriority priority (entityDB t) $ do
  conn <- ask
  let esc           = connEscapeName conn
      insertFields  = map (esc . fieldDB) $ entityFields t
      cols          = T.intercalate (T.singleton ',') insertFields
      placeholders  = replicateQ $ length insertFields
      updateFields  = map (esc . fieldDB) ufs
      updateCols    = (T.intercalate ", ") $ map (\name -> name <> "=VALUES(" <> name <> ")") updateFields

  rawExecute ( "INSERT INTO "
            <> esc (entityDB t)
            <> " ("
            <> cols
            <> ") VALUES ("
            <> T.intercalate "),(" (replicate (length rs) placeholders)
            <> ") ON DUPLICATE KEY UPDATE "
            <> updateCols
            ) $ concatMap (map toPersistValue . toPersistFields) rs
  where
    t = entityDef $ proxyFromRecords rs

    replicateQ :: Int -> Text
    replicateQ = T.intersperse ',' . (flip T.replicate $ T.singleton '?')

insertOrUpdateUniqueMany_
  :: (MonadIO m,MonadThrow m,MonadBaseControl IO m,PersistEntity record,PersistEntity t,PersistEntityBackend record ~ SqlBackend)
  => SqlPriority
  -> Int
  -> Bool
  -> [record]
  -> [DupUpdate t]
  -> ReaderT SqlBackend m ()
insertOrUpdateUniqueMany_ priority chunk commitChunks rs ufs =
  forM_ (chunksOf chunk rs) $ \rs' -> do
    insertOrUpdateUniqueMany_' priority rs' (map dupUpdateFieldDef ufs)
    when commitChunks transactionSave

insertOrUpdateUnique_ :: (MonadIO m,MonadThrow m,MonadBaseControl IO m,PersistEntity record,PersistEntity t,PersistEntityBackend record ~ SqlBackend)
                      => SqlPriority
                      -> record
                      -> [DupUpdate t]
                      -> ReaderT SqlBackend m ()
insertOrUpdateUnique_ priority r = insertOrUpdateUniqueMany_ priority 1 False [r]

insertOrUpdateUniqueMany'
  :: (ToBackendKey SqlBackend val,MonadResource m,MonadBaseControl IO m)
  => SqlPriority
  -> [val]
  -> [FieldDef]
  -> ConduitM () (Key val) (ReaderT SqlBackend m) ()
insertOrUpdateUniqueMany' _        [] _   = CL.sourceList []
insertOrUpdateUniqueMany' priority rs []  = do
  es <- lift $ withPriority priority (entityDB . entityDef $ proxyFromRecords rs) $ mapM insertBy rs
  CL.sourceList $ map (fromEither . left entityKey) es -- TODO: insertUniqueMany
  where
    fromEither (Left x) = x
    fromEither (Right x) = x
insertOrUpdateUniqueMany' priority rs ufs = do
  let uniqs = map (headNote "Could not find any unique keys to use with insertOrUpdate" . persistUniqueKeys) rs
  lift $ insertOrUpdateUniqueMany_' priority rs ufs
  (selectKeysBy uniqs $= CL.map fromJust)

insertOrUpdateUniqueMany
  :: (MonadResource m,ToBackendKey SqlBackend val,MonadBaseControl IO m,PersistEntity t)
  => SqlPriority
  -> Int
  -> Bool
  -> [val]
  -> [DupUpdate t]
  -> ConduitM () (Key val) (ReaderT SqlBackend m) ()
insertOrUpdateUniqueMany priority chunk commitChunks rs ufs = do
  mapM_ insertOrUpdateChunk (chunksOf chunk rs)
  where
    -- insertOrUpdateChunk :: (MonadResource m, MonadResourceBase m, PersistEntity val, PersistUnique m, PersistStore m) =>
    --                        [val] -> Source m (Key val)
    insertOrUpdateChunk rs' = do
      insertOrUpdateUniqueMany' priority rs' $ map dupUpdateFieldDef ufs
      when commitChunks (lift transactionSave)

-- | Replace or insert many records using uniqueness constraints instead of the entity key
repsertUniqueMany_ :: (MonadIO m,MonadThrow m,MonadBaseControl IO m,PersistEntity record,PersistEntityBackend record ~ SqlBackend)
                   => SqlPriority
                   -> Int
                   -> Bool
                   -> [record]
                   -> ReaderT SqlBackend m ()
repsertUniqueMany_ _        _     _            [] = return ()
repsertUniqueMany_ priority chunk commitChunks rs = mapM_ insertOrUpdateChunk $ chunksOf chunk rs
  where
    t = entityDef $ proxyFromRecords rs
    fs = entityFields t --TODO: Exclude unique fields since they are unnecesary

    -- insertOrUpdateChunk :: (MonadResourceBase m, PersistEntity val, PersistUnique m, PersistStore m) =>
    --                        [val] -> m ()
    insertOrUpdateChunk rs' = do
      insertOrUpdateUniqueMany_' priority rs' fs
      when commitChunks transactionSave

repsertUniqueMany
  :: (MonadResource m,ToBackendKey SqlBackend val,MonadBaseControl IO m)
  => SqlPriority
  -> Int
  -> Bool
  -> [val]
  -> ConduitM () (Key val) (ReaderT SqlBackend m) ()
repsertUniqueMany priority chunk commitChunks rs = do
  mapM_ insertOrUpdateChunk $ chunksOf chunk rs
  where
    t  = entityDef $ proxyFromRecords rs
    fs = entityFields t --TODO: Exclude unique fields since they are unnecesary

    -- TODO: Use replace into in the future
    -- insertOrUpdateChunk :: (MonadResource m, MonadResourceBase m, PersistEntity val, PersistUnique m, PersistStore m) =>
    --                        [val] -> Source m (Key val)
    insertOrUpdateChunk rs' = do
      insertOrUpdateUniqueMany' priority rs' fs
      when commitChunks (lift transactionSave)

-- | Replace or insert a record using uniqueness constraints instead of the entity key
repsertUnique_
  :: (MonadIO m,MonadThrow m,MonadBaseControl IO m,PersistEntity record,PersistEntityBackend record ~ SqlBackend)
  => record -> ReaderT SqlBackend m ()
repsertUnique_ r = repsertUniqueMany_ NormalPriority 1 False [r]

repsertUnique
  :: (MonadIO m,MonadThrow m,MonadBaseControl IO m,PersistEntity record,PersistEntityBackend record ~ SqlBackend)
  => record -> ReaderT SqlBackend m (Key record)
repsertUnique r = do
  repsertUnique_ r
  Just (Entity k _) <- getByValue r -- TODO: Speed this up (no need to return the entire entity for repsert)
  return k

-- | Insert many values into the database in large chunks
insertMany_'
  :: (MonadIO m,MonadThrow m,MonadBaseControl IO m,PersistEntity record)
  => SqlPriority -> Bool -> [record] -> ReaderT SqlBackend m ()
insertMany_' _        _            [] = return ()
insertMany_' priority ignoreErrors rs = withPriority priority (entityDB t) $ do
  conn <- ask
  let esc           = connEscapeName conn
      insertFields  = map (esc . fieldDB) (entityFields t)
      cols          = T.intercalate (T.singleton ',') insertFields
      placeholders  = replicateQ $ length insertFields

  rawExecute ( "INSERT "
            <> (if ignoreErrors then "IGNORE " else "")
            <> "INTO "
            <> esc (entityDB t)
            <> " ("
            <> cols
            <> ") VALUES ("
            <> T.intercalate "),(" (replicate (length rs) placeholders)
            <> ")"
            ) $ concatMap (map toPersistValue . toPersistFields) rs
  where
    t = entityDef $ proxyFromRecords rs

    replicateQ :: Int -> Text
    replicateQ = T.intersperse ',' . (flip T.replicate $ T.singleton '?')

insertMany_ :: (MonadIO m,MonadThrow m,MonadBaseControl IO m,PersistEntity record)
            => SqlPriority
            -> Bool
            -> Int
            -> Bool
            -> [record]
            -> ReaderT SqlBackend m ()
insertMany_ priority ignoreErrors chunk commitChunks rs = do
  mapM_ (\rs' -> insertMany_' priority ignoreErrors rs' >> when commitChunks transactionSave) $ chunksOf chunk rs

-- Helpers
-- See persistent/Database/Persist/Sql/Orphan/PersistQuery.hs
proxyFromUniqs :: PersistEntity val => [Unique val] -> Proxy val
proxyFromUniqs _ = Proxy

proxyFromRecords :: PersistEntity val => [val] -> Proxy val
proxyFromRecords _ = Proxy

proxyFromEntities :: PersistEntity val => [Entity val] -> Proxy val
proxyFromEntities _ = Proxy

-- keyFromProxy :: PersistEntity val => Proxy val -> m (Key val)
-- keyFromProxy _ = error "keyFromProxy must not be evaluated"

dupUpdateFieldDef :: PersistEntity t
                  => DupUpdate t -> FieldDef
dupUpdateFieldDef (DupUpdateField f) = persistFieldDef f

withPriority :: (MonadResourceBase m) => SqlPriority -> DBName -> m a -> m a
withPriority NormalPriority _      op = op
withPriority LowPriority    dbName op = do
  let s = unDBName dbName
  -- Make sure that the counter exists (returning the existing counter if it does)
  (lock, counter) <- liftIO $ do
    newSem <- (,) <$> newMVar () <*> newIORef 0
    atomicModifyIORef' updateSems $ \sems ->
      case s `M.lookup` sems of
        Just existingSem -> (sems, existingSem)
        Nothing          -> (M.insert s newSem sems, newSem)

  -- Lock the table and increment our counter (fail if too many queries are queued up)
  withMVar lock $ \_ -> do -- use withMVar to release the lock if any exceptions occur
    -- No need to be thread-safe with with the io refs since we're protecting this whole function with a lock
    let acquire   = do { c <- readIORef counter ; writeIORef counter (c + 1) ; return (c + 1) }
        release c = writeIORef counter (c - 1)
    liftBaseOp (bracket acquire release) $ \c -> do -- use bracket to reset the counter if any exceptions occur (see Control.Exception.Lifted (bracket))
      -- Fail if the maximum number of queries have been enqueued
      liftIO $ when (c >= maxWaitingQueries) . throwIO . SqlWaitException $ "Maximum " <> (T.pack . show) c  <> " of " <> (T.pack . show) maxWaitingQueries <> " insert/update queries queued on table `" <> s <> "`."
      -- Safely perform the query inside of the lock (any query exceptions will release resources)
      op
