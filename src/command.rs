use anyhow::Context;
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::resp::{serialize, RespValue};

pub trait RedisCommand {
    fn respond(&self, socket: &mut TcpStream) -> impl std::future::Future<Output = ()> + Send;
}

pub enum Command {
    Ping,
    Echo(RespValue),
}

pub async fn respond(socket: &mut TcpStream, command: &Command) {
    match command {
        Command::Ping => {
            let simple_string = RespValue::SimpleString(String::from("PONG"));
            let buf = serialize(&simple_string);
            socket
                .write_all(&buf)
                .await
                .context("Send PONG response")
                .unwrap();
        }
        Command::Echo(val) => {
            let buf = serialize(val);
            socket
                .write_all(&buf)
                .await
                .context("Send ECHO response")
                .unwrap();
        }
    }
}
