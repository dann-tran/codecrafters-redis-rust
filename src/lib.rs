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

pub type Db = Arc<Mutex<HashMap<String, DbValue>>>;

pub fn get_current_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}
