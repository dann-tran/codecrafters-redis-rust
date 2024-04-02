use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

use crate::{
    command::Command,
    resp::RespValue,
    server::{handle_info, send_bulk_string, send_integer, send_simple_string},
};

use super::{MasterInfo, RedisServerHandler, RedisStore};

#[derive(Clone)]
pub struct MasterServer {
    master_info: MasterInfo,
    store: Arc<Mutex<RedisStore>>,
    repl_conns: Arc<Mutex<Vec<TcpStream>>>,
}

#[async_trait]
impl RedisServerHandler for MasterServer {
    async fn handle_conn(&mut self, mut socket: TcpStream) {
        let mut buf = [0u8; 1024];
        loop {
            socket
                .read(&mut buf)
                .await
                .context("Read from client")
                .unwrap();
            let (cmd, _) = Command::from_bytes(&buf).unwrap();

            let var_name = match cmd {
                Command::Ping => {
                    eprintln!("Handling PING from client");
                    send_simple_string(&mut socket, "PONG").await;
                }
                Command::Echo(val) => {
                    eprintln!("Handling ECHO from client");
                    send_bulk_string(&mut socket, &val).await;
                }
                Command::Set { key, value, px } => {
                    eprintln!("Handling SET from client");

                    let mut store = self.store.lock().await;
                    store.set(&key, &value, &px);
                    drop(store);

                    eprintln!("Propagate SET command to slaves");
                    let cmd = Command::Set {
                        key: key.clone(),
                        value: value.clone(),
                        px: px.clone(),
                    };
                    let buf = cmd.to_bytes();
                    let mut repl_conns = self.repl_conns.lock().await;
                    for conn in repl_conns.iter_mut() {
                        conn.write_all(&buf)
                            .await
                            .context("Propagate SET to slave")
                            .unwrap();
                        eprintln!("Propagated to {}", conn.peer_addr().unwrap());
                    }
                    drop(repl_conns);

                    send_simple_string(&mut socket, "OK").await;
                }
                Command::Get(key) => {
                    eprintln!("Handling GET from client");

                    let mut store = self.store.lock().await;
                    let value = store.get(&key);
                    drop(store);

                    let res = match value {
                        Some(x) => RespValue::BulkString(x.as_bytes().to_vec()),
                        None => RespValue::NullBulkString,
                    };
                    let buf = res.to_bytes();

                    socket
                        .write_all(&buf)
                        .await
                        .context("Send GET response")
                        .unwrap();
                }
                Command::Info(_) => {
                    eprintln!("Handling INFO from client");
                    handle_info(&mut socket, "master", &self.master_info).await;
                }
                Command::ReplConf(_) => {
                    eprintln!("Handling REPLCONF from client");

                    send_simple_string(&mut socket, "OK").await;
                }
                Command::PSync {
                    repl_id: _,
                    repl_offset: _,
                } => {
                    eprintln!("Handling PSYNC from client");

                    // FULLRESYNC <REPL_ID> 0
                    let res = vec![
                        "FULLRESYNC",
                        &self.master_info.repl_id.iter().collect::<String>(),
                        "0",
                    ]
                    .join(" ");
                    send_simple_string(&mut socket, &res).await;

                    let empty_rdb = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").expect("Valid HEX string");
                    let res = RespValue::BulkString(empty_rdb);
                    let buf = res.to_bytes();
                    let buf = &buf[..buf.len() - 2];

                    socket
                        .write_all(&buf)
                        .await
                        .context("Send empty RDB file")
                        .unwrap();

                    let mut repl_conns = self.repl_conns.lock().await;
                    repl_conns.push(socket);
                    drop(repl_conns);

                    break;
                }
                Command::Wait {
                    repl_num: _,
                    sec_num: _,
                } => {
                    eprintln!("Handling WAIT from client");
                    let repl_conns = self.repl_conns.lock().await;
                    let repl_num = repl_conns.len();
                    drop(repl_conns);
                    send_integer(&mut socket, repl_num as i64).await;
                }
            };
            var_name
        }
    }
}

impl MasterServer {
    pub fn new() -> Self {
        Self {
            master_info: MasterInfo::new(),
            store: Arc::new(Mutex::new(RedisStore::new())),
            repl_conns: Arc::new(Mutex::new(Vec::new())),
        }
    }
}
