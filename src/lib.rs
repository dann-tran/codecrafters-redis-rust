use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

pub mod command;
pub mod resp;

pub struct DbValue {
    value: String,
    expiry: Option<u128>,
}

pub enum RedisRole {
    Master,
    Slave,
}

pub struct RedisInfo {
    pub role: RedisRole,
}

pub struct RedisState {
    pub info: RedisInfo,
    pub db: HashMap<String, DbValue>,
}

pub type StateWithMutex = Arc<Mutex<RedisState>>;

pub fn get_current_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}
