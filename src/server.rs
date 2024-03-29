use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    command::{Command, FromBytes},
    utils::get_current_ms,
};
use crate::{
    resp::{decode_array_of_bulkstrings, RespValue},
    ToBytes,
};
use anyhow::Context;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

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
    pub master_repl_id: [char; 40],
    pub master_repl_offset: usize,
}

impl RedisInfo {
    pub fn new(role: RedisRole) -> RedisInfo {
        RedisInfo {
            role,
            master_repl_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
                .chars()
                .collect::<Vec<char>>()
                .try_into()
                .expect("40 characters"),
            master_repl_offset: 0,
        }
    }
}

pub struct RedisState {
    pub info: RedisInfo,
    pub db: HashMap<String, DbValue>,
}

async fn recv_then_assert(buf: &mut [u8; 1024], socket: &mut TcpStream, expected: &Vec<u8>) {
    socket.read(buf).await.context("Read from client").unwrap();
    assert!(
        buf.starts_with(expected),
        "Expected: {:?}; Found: {:?}",
        expected,
        &buf[..expected.len()]
    );
}

impl RedisState {
    fn new(role: RedisRole) -> RedisState {
        let info = RedisInfo::new(role);
        Self {
            info,
            db: HashMap::new(),
        }
    }

    pub async fn new_slave(slave_port: u16, master_addr: &str) -> RedisState {
        let mut socket = TcpStream::connect(master_addr).await.unwrap();
        let mut recv_buf = [0u8; 1024];

        // Send PING
        let send_buf = Command::Ping.to_bytes();
        socket
            .write_all(&send_buf)
            .await
            .context("Send PING")
            .unwrap();

        // Receive PONG
        recv_then_assert(
            &mut recv_buf,
            &mut socket,
            &RespValue::SimpleString(String::from("PONG")).to_bytes(),
        )
        .await;

        // Send REPLCONF listening-port
        let send_buf = Command::ReplConf {
            listening_port: Some(slave_port),
            capa: vec![],
        }
        .to_bytes();
        socket
            .write_all(&send_buf)
            .await
            .context("Send REPLCONF listening-port")
            .unwrap();

        // Receive OK
        recv_then_assert(
            &mut recv_buf,
            &mut socket,
            &RespValue::SimpleString(String::from("OK")).to_bytes(),
        )
        .await;

        // Send REPLCONF capa
        let send_buf = Command::ReplConf {
            listening_port: None,
            capa: vec![String::from("psync2")],
        }
        .to_bytes();
        socket
            .write_all(&send_buf)
            .await
            .context("Send REPLCONF capa")
            .unwrap();

        // Receive OK
        recv_then_assert(
            &mut recv_buf,
            &mut socket,
            &RespValue::SimpleString(String::from("OK")).to_bytes(),
        )
        .await;

        // Send PSYNC
        let send_buf = Command::PSync {
            repl_id: None,
            repl_offset: None,
        }
        .to_bytes();
        socket
            .write_all(&send_buf)
            .await
            .context("Send PSYNC")
            .unwrap();

        Self::new(RedisRole::Slave)
    }

    pub fn new_master() -> RedisState {
        Self::new(RedisRole::Master)
    }
}

pub type StateWithMutex = Arc<Mutex<RedisState>>;

pub struct RedisServer {
    pub state: StateWithMutex,
    pub socket: TcpStream,
}

impl RedisServer {
    async fn respond(&mut self, command: &Command) {
        let res = match command {
            Command::Ping => RespValue::SimpleString(String::from("PONG")),
            Command::Echo(val) => RespValue::BulkString(val.clone()),
            Command::Set { key, value, px } => {
                let mut state = self.state.lock().unwrap();
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
                let mut state = self.state.lock().unwrap();
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
                let state = self.state.lock().unwrap();
                let info = &state.info;
                let mut info_map = HashMap::<Vec<u8>, Vec<u8>>::new();
                info_map.insert(
                    b"role".to_vec(),
                    match info.role {
                        RedisRole::Master => b"master".to_vec(),
                        RedisRole::Slave => b"slave".to_vec(),
                    },
                );
                info_map.insert(
                    b"master_replid".to_vec(),
                    info.master_repl_id
                        .iter()
                        .collect::<String>()
                        .as_bytes()
                        .to_vec(),
                );
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
            Command::ReplConf {
                listening_port: _,
                capa: _,
            } => RespValue::SimpleString(String::from("OK")),
            Command::PSync {
                repl_id: _,
                repl_offset: _,
            } => {
                let state = self.state.lock().unwrap();
                RespValue::SimpleString(
                    vec![
                        "FULLRESYNC",
                        &state.info.master_repl_id.iter().collect::<String>(),
                        "0",
                    ]
                    .join(" "),
                )
            }
        };
        let buf = res.to_bytes();
        self.socket
            .write_all(&buf)
            .await
            .context("Send PONG response")
            .unwrap();
    }

    pub async fn start(&mut self) {
        loop {
            let mut buf = [0u8; 1024];
            self.socket
                .read(&mut buf)
                .await
                .context("Read from client")
                .unwrap();

            let args = decode_array_of_bulkstrings(&buf);
            let cmd = Command::from_bytes(&args);
            self.respond(&cmd).await;
        }
    }
}
