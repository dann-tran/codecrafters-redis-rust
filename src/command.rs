use anyhow::Context;
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{
    resp::{serialize, RespValue},
    Db,
};

pub trait RedisCommand {
    fn respond(&self, socket: &mut TcpStream) -> impl std::future::Future<Output = ()> + Send;
}

pub enum Command {
    Ping,
    Echo(Vec<u8>),
    Set { key: String, value: String },
    Get(String),
}

pub async fn respond(socket: &mut TcpStream, db: &Db, command: &Command) {
    let buf = match command {
        Command::Ping => {
            let simple_string = RespValue::SimpleString(String::from("PONG"));
            serialize(&simple_string)
        }
        Command::Echo(val) => serialize(&RespValue::BulkString(val.clone())),
        Command::Set { key, value } => {
            let mut db = db.lock().unwrap();
            db.insert(key.into(), value.into());
            serialize(&RespValue::SimpleString("OK".into()))
        }
        Command::Get(key) => {
            let db = db.lock().unwrap();
            match db.get(key) {
                Some(value) => serialize(&RespValue::BulkString(value.as_bytes().into())),
                None => serialize(&RespValue::NullBulkString),
            }
        }
    };
    socket
        .write_all(&buf)
        .await
        .context("Send PONG response")
        .unwrap();
}
