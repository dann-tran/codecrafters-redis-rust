use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub mod command;
pub mod resp;

pub type Db = Arc<Mutex<HashMap<String, String>>>;
