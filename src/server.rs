use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::resp::{decode_array_of_bulkstrings, RespValue, ToBytes};
use crate::{
    command::{Command, InfoArg},
    utils::get_current_ms,
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
    pub master_replid: [u8; 40],
    pub master_repl_offset: usize,
}

impl RedisInfo {
    pub fn new(role: RedisRole) -> RedisInfo {
        RedisInfo {
            role,
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
                .as_bytes()
                .try_into()
                .expect("Valid 40-character string"),
            master_repl_offset: 0,
        }
    }
}

pub struct RedisState {
    pub info: RedisInfo,
    pub db: HashMap<String, DbValue>,
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

        // Send PING
        let req = RespValue::Array(vec![RespValue::BulkString("PING".into())]);
        let send_buf = req.to_bytes();
        socket.write_all(&send_buf).await.unwrap();

        // Receive PONG
        let mut recv_buf = [0u8; 1024];
        socket
            .read(&mut recv_buf)
            .await
            .context("Read from client")
            .unwrap();
        let expected_res = RespValue::SimpleString(String::from("PONG")).to_bytes();
        assert!(recv_buf.starts_with(&expected_res));

        // Send REPLCONF listening-port
        let buf = RespValue::Array(
            vec![
                b"REPLCONF".to_vec(),
                b"listening-port".to_vec(),
                slave_port.to_string().as_bytes().to_vec(),
            ]
            .iter()
            .map(|x| RespValue::BulkString(x.clone()))
            .collect(),
        )
        .to_bytes();
        socket.write_all(&buf).await.unwrap();

        // Receive OK
        socket
            .read(&mut recv_buf)
            .await
            .context("Read from client")
            .unwrap();
        let expected_res = RespValue::SimpleString(String::from("OK")).to_bytes();
        assert!(recv_buf.starts_with(&expected_res));

        // Send REPLCONF capa
        let buf = RespValue::Array(
            vec![b"REPLCONF".to_vec(), b"capa".to_vec(), b"psync2".to_vec()]
                .iter()
                .map(|x| RespValue::BulkString(x.clone()))
                .collect(),
        )
        .to_bytes();
        socket.write_all(&buf).await.unwrap();

        // Receive OK
        socket
            .read(&mut recv_buf)
            .await
            .context("Read from client")
            .unwrap();
        let expected_res = RespValue::SimpleString(String::from("OK")).to_bytes();
        assert!(recv_buf.starts_with(&expected_res));

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
            let mut args = args.iter();
            let verb = args
                .next()
                .expect("A command verb must be present")
                .to_ascii_lowercase();
            let cmd = match &verb[..] {
                b"ping" => Command::Ping,
                b"echo" => {
                    let val = args.next().expect("ECHO argument");
                    Command::Echo(val.clone())
                }
                b"get" => {
                    let val = args.next().expect("GET key");
                    Command::Get(String::from_utf8(val.clone()).expect("Key must be UTF-8"))
                }
                b"set" => {
                    let key = args.next().expect("SET key");
                    let value = args.next().expect("SET value");
                    let is_px_present = match args.next().map(|s| s.to_ascii_lowercase()) {
                        Some(c) => match &c[..] {
                            b"px" => true,
                            _ => panic!("Invalid SET arguments"),
                        },
                        None => false,
                    };
                    let px = match is_px_present {
                        true => {
                            let px = args.next().expect("expiry argument");
                            let px = String::from_utf8(px.clone()).expect("Valid string");
                            let px = px.parse::<usize>().expect("Valid number");
                            Some(px)
                        }
                        false => None,
                    };

                    Command::Set {
                        key: String::from_utf8(key.clone()).expect("Valid UTF-8 key"),
                        value: String::from_utf8(value.clone()).expect("Valid UTF-8 value"),
                        px: px,
                    }
                }
                b"info" => {
                    let info_arg =
                        args.next()
                            .map(|v| v.to_ascii_lowercase())
                            .map(|v| match &v[..] {
                                b"replication" => InfoArg::Replication,
                                _ => panic!("Invalid info argument {:?}", v),
                            });
                    Command::Info(info_arg)
                }
                _ => panic!("Unknown verb: {:?}", verb),
            };
            if args.next().is_some() {
                panic!("Unexpected arguments")
            }
            self.respond(&cmd).await;
        }
    }
}
