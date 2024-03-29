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
            let mut role_info = b"role:".to_vec();
            let role_string = match info.role {
                RedisRole::Master => b"master".to_vec(),
                RedisRole::Slave => b"slave".to_vec(),
            };
            role_info.extend(&role_string);
            RespValue::BulkString(role_info)
        }
    };
    let buf = serialize(&res);
    socket
        .write_all(&buf)
        .await
        .context("Send PONG response")
        .unwrap();
}
