use std::{collections::HashMap, sync::Arc};

use crate::resp::RespValue;
use crate::{command::Command, utils::get_current_ms};
use anyhow::Context;
use async_trait::async_trait;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

pub mod master;
pub mod replica;

#[derive(Clone)]
pub struct MasterInfo {
    repl_id: [char; 40],
    repl_offset: usize,
}

impl MasterInfo {
    pub fn new() -> Self {
        Self {
            repl_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
                .chars()
                .collect::<Vec<char>>()
                .try_into()
                .expect("40 characters"),
            repl_offset: 0,
        }
    }
}

pub struct RSValue {
    value: String,
    expiry: Option<u128>,
}

pub struct RedisStore(HashMap<String, RSValue>);

impl RedisStore {
    fn get(&mut self, key: &String) -> Option<String> {
        match self.0.get(key) {
            Some(RSValue { value, expiry }) => match expiry {
                Some(v) => {
                    if get_current_ms() > *v {
                        self.0.remove(key);
                        None
                    } else {
                        Some(value.clone())
                    }
                }
                None => Some(value.clone()),
            },
            None => None,
        }
    }

    fn set(&mut self, key: &String, value: &String, px: &Option<usize>) {
        self.0.insert(
            key.clone(),
            RSValue {
                value: value.clone(),
                expiry: px.map(|v| get_current_ms() + (v as u128)),
            },
        );
    }

    pub fn new() -> Self {
        Self(HashMap::new())
    }
}

pub trait RedisServerGetter {
    fn get_store(&self) -> &Arc<Mutex<RedisStore>>;
    fn get_role(&self) -> String;
    fn get_master_info(&self) -> &MasterInfo;
}

async fn send_simple_string(socket: &mut TcpStream, res: &str) {
    let buf = RespValue::SimpleString(res.into()).to_bytes();
    socket
        .write_all(&buf)
        .await
        .context(format!("Send {}", res))
        .unwrap();
}

async fn send_bulk_string(socket: &mut TcpStream, res: &[u8]) {
    let buf = RespValue::BulkString(res.into()).to_bytes();
    socket
        .write_all(&buf)
        .await
        .context(format!("Send {:?}", res))
        .unwrap();
}

#[async_trait]
pub trait RedisServerHandler: RedisServerGetter {
    async fn handle_conn(&mut self, mut socket: TcpStream) {
        let mut buf = [0u8; 1024];
        loop {
            socket
                .read(&mut buf)
                .await
                .context("Read from client")
                .unwrap();
            let (cmd, _) = Command::from_bytes(&buf).unwrap();

            match cmd {
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

                    self.handle_set(&mut socket, &key, &value, &px).await;
                }
                Command::Get(key) => {
                    eprintln!("Handling GET from client");

                    let mut store = self.get_store().lock().await;
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

                    let mut info_map = HashMap::<Vec<u8>, Vec<u8>>::new();
                    info_map.insert(b"role".to_vec(), self.get_role().as_bytes().to_vec());

                    let master_info = self.get_master_info();
                    info_map.insert(
                        b"master_replid".to_vec(),
                        master_info
                            .repl_id
                            .iter()
                            .collect::<String>()
                            .as_bytes()
                            .to_vec(),
                    );
                    info_map.insert(
                        b"master_repl_offset".to_vec(),
                        master_info.repl_offset.to_string().into(),
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

                    let res = RespValue::BulkString(lines);
                    let buf = res.to_bytes();

                    socket
                        .write_all(&buf)
                        .await
                        .context("Send INFO response")
                        .unwrap();
                }
                Command::ReplConf(_) => {
                    eprintln!("Handling REPLCONF from client");

                    self.handle_replconf(&mut socket).await;
                }
                Command::PSync {
                    repl_id: _,
                    repl_offset: _,
                } => {
                    eprintln!("Handling PSYNC from client");

                    self.handle_psync(socket).await;
                    break;
                }
            }
        }
    }

    async fn send_cmd(socket: &mut TcpStream, cmd: &Command) {
        let send_buf = cmd.to_bytes();
        socket
            .write_all(&send_buf)
            .await
            .context(format!("Send {:?}", cmd))
            .unwrap();
    }

    async fn handle_set(
        &self,
        socket: &mut TcpStream,
        key: &String,
        value: &String,
        px: &Option<usize>,
    );

    async fn handle_replconf(&self, socket: &mut TcpStream);
    async fn handle_psync(&self, socket: TcpStream);
}
