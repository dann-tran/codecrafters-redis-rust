use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::{broadcast, Mutex};

use crate::{
    command::XReadStreamArg,
    db::{
        stream::{ReqStreamEntryID, StreamEntryID},
        RedisDb, RedisValueType,
    },
};

#[derive(Clone)]
pub(crate) struct RedisStore {
    databases: HashMap<u32, Arc<Mutex<RedisDb>>>,
    cur_db_num: u32,
}

impl RedisStore {
    pub(crate) fn new() -> Self {
        let mut databases = HashMap::new();
        databases.insert(0 as u32, RedisDb::new());
        Self::from(databases)
    }

    pub(crate) fn from(databases: HashMap<u32, RedisDb>) -> Self {
        let databases = databases
            .into_iter()
            .map(|(k, v)| (k, Arc::new(Mutex::new(v))))
            .collect::<HashMap<u32, Arc<Mutex<RedisDb>>>>();
        if let Some(&k) = &databases.keys().into_iter().min() {
            Self {
                databases,
                cur_db_num: k,
            }
        } else {
            Self::new()
        }
    }

    fn get_cur_db(&self) -> &Arc<Mutex<RedisDb>> {
        self.databases
            .get(&self.cur_db_num)
            .expect("Current db number is valid")
    }

    pub(crate) async fn get_stream_receiver(
        &self,
        key: &Vec<u8>,
    ) -> broadcast::Receiver<(StreamEntryID, HashMap<Vec<u8>, Vec<u8>>)> {
        self.get_cur_db().lock().await.get_stream_receiver(key)
    }

    pub(crate) async fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        self.get_cur_db().lock().await.get(key)
    }

    pub(crate) async fn set(&self, key: &Vec<u8>, value: Vec<u8>, px: Option<Duration>) {
        self.get_cur_db().lock().await.set(key, value, px)
    }

    pub(crate) async fn keys(&self) -> Vec<Vec<u8>> {
        self.get_cur_db().lock().await.keys()
    }

    pub(crate) async fn lookup_type(&self, key: &Vec<u8>) -> Option<RedisValueType> {
        self.get_cur_db().lock().await.lookup_type(key)
    }

    pub(crate) async fn xadd(
        &self,
        key: &Vec<u8>,
        entry_id: Option<ReqStreamEntryID>,
        data: HashMap<Vec<u8>, Vec<u8>>,
    ) -> anyhow::Result<StreamEntryID> {
        self.get_cur_db().lock().await.xadd(key, entry_id, data)
    }

    pub(crate) async fn xrange(
        &self,
        key: &Vec<u8>,
        start: StreamEntryID,
        end: StreamEntryID,
    ) -> Vec<(Vec<u8>, Vec<Vec<u8>>)> {
        self.get_cur_db().lock().await.xrange(key, start, end)
    }

    pub(crate) async fn xread(
        &self,
        args: &Vec<XReadStreamArg>,
    ) -> Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<Vec<u8>>)>)> {
        self.get_cur_db().lock().await.xread(args)
    }
}
