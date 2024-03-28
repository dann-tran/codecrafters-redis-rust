use anyhow::Context;
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{
    get_current_ms,
    resp::{serialize, RespValue},
    Db, DbValue,
};

pub enum Command {
    Ping,
    Echo(Vec<u8>),
    Set {
        key: String,
        value: String,
        px: Option<usize>,
    },
    Get(String),
}

pub async fn respond(socket: &mut TcpStream, db: &Db, command: &Command) {
    let res = match command {
        Command::Ping => RespValue::SimpleString(String::from("PONG")),
        Command::Echo(val) => RespValue::BulkString(val.clone()),
        Command::Set { key, value, px } => {
            let mut db = db.lock().unwrap();
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
            let mut db = db.lock().unwrap();
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
    };
    let buf = serialize(&res);
    socket
        .write_all(&buf)
        .await
        .context("Send PONG response")
        .unwrap();
}
