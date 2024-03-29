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
    pub master_replid: [u8; 40],
    pub master_repl_offset: usize,
}

impl RedisInfo {
    pub fn new(role: RedisRole) -> RedisInfo {
        RedisInfo {
            role,
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
                .as_bytes()
                .try_into()
                .expect("Valid 40-character string"),
            master_repl_offset: 0,
        }
    }
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
