use std::collections::HashMap;

use anyhow::Context;
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{
    get_current_ms,
    resp::{serialize, RespValue},
    DbValue, RedisRole, StateWithMutex,
};

pub enum InfoArg {
    Replication,
}

pub enum Command {
    Ping,
    Echo(Vec<u8>),
    Set {
        key: String,
        value: String,
        px: Option<usize>,
    },
    Get(String),
    Info(Option<InfoArg>),
}

pub async fn respond(socket: &mut TcpStream, state: &StateWithMutex, command: &Command) {
    let res = match command {
        Command::Ping => RespValue::SimpleString(String::from("PONG")),
        Command::Echo(val) => RespValue::BulkString(val.clone()),
        Command::Set { key, value, px } => {
            let mut state = state.lock().unwrap();
            let db = &mut state.db;
            db.insert(
                key.into(),
                DbValue {
                    value: value.into(),
                    expiry: px.map(|v| get_current_ms() + (v as u128)),
                },
            );
            RespValue::SimpleString("OK".into())
        }
        Command::Get(key) => {
            let mut state = state.lock().unwrap();
            let db = &mut state.db;
            match db.get(key) {
                Some(DbValue { value, expiry }) => match expiry {
                    Some(v) => {
                        if get_current_ms() > *v {
                            db.remove(key);
                            RespValue::NullBulkString
                        } else {
                            RespValue::BulkString(value.as_bytes().into())
                        }
                    }
                    None => RespValue::BulkString(value.as_bytes().into()),
                },
                None => RespValue::NullBulkString,
            }
        }
        Command::Info(_) => {
            let state = state.lock().unwrap();
            let info = &state.info;
            let mut info_map = HashMap::<Vec<u8>, Vec<u8>>::new();
            info_map.insert(
                b"role".to_vec(),
                match info.role {
                    RedisRole::Master => b"master".to_vec(),
                    RedisRole::Slave => b"slave".to_vec(),
                },
            );
            info_map.insert(b"master_replid".to_vec(), info.master_replid.into());
            info_map.insert(
                b"master_repl_offset".to_vec(),
                info.master_repl_offset.to_string().into(),
            );
            let lines = info_map.iter().fold(Vec::new(), |mut acc, (k, v)| {
                if !acc.is_empty() {
                    acc.push(b'\n');
                }
                acc.extend(k);
                acc.push(b':');
                acc.extend(v);
                acc
            });
            RespValue::BulkString(lines)
        }
    };
    let buf = serialize(&res);
    socket
        .write_all(&buf)
        .await
        .context("Send PONG response")
        .unwrap();
}
