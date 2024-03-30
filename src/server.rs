use std::{collections::HashMap, sync::Arc};

use crate::{
    command::{Command, FromBytes},
    utils::get_current_ms,
};
use crate::{resp::RespValue, ToBytes};
use anyhow::Context;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
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
    pub slave_conns: Vec<TcpStream>,
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
            slave_conns: Vec::new(),
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
        let mut socket = TcpStream::connect(master_addr)
            .await
            .context("Connect to master")
            .unwrap();
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

pub struct RedisServer(pub StateWithMutex);

impl RedisServer {
    pub async fn bind(&mut self, mut socket: TcpStream) {
        loop {
            let mut buf = [0u8; 1024];
            socket
                .read(&mut buf)
                .await
                .context("Read from client")
                .unwrap();

            let cmd = Command::from_bytes(&buf);
            match cmd {
                Command::Ping => {
                    eprintln!("Handling PING");
                    let res = RespValue::SimpleString(String::from("PONG"));
                    let buf = res.to_bytes();

                    socket.write_all(&buf).await.context("Send PONG").unwrap();
                }
                Command::Echo(val) => {
                    eprintln!("Handling ECHO");
                    let res = RespValue::BulkString(val.clone());
                    let buf = res.to_bytes();

                    socket
                        .write_all(&buf)
                        .await
                        .context("Send ECHO response")
                        .unwrap();
                }
                Command::Set { key, value, px } => {
                    eprintln!("Handling SET");
                    let mut state = self.0.lock().await;

                    let db = &mut state.db;
                    db.insert(
                        key.clone(),
                        DbValue {
                            value: value.clone(),
                            expiry: px.map(|v| get_current_ms() + (v as u128)),
                        },
                    );

                    eprintln!("Propagate SET commands to slaves");
                    let cmd = Command::Set { key, value, px };
                    let buf = cmd.to_bytes();
                    for conn in state.info.slave_conns.iter_mut() {
                        eprintln!("Propagate to {}", conn.peer_addr().unwrap());
                        conn.write_all(&buf)
                            .await
                            .context("Propagate SET to slave")
                            .unwrap();
                    }

                    let res = RespValue::SimpleString("OK".into());
                    let buf = res.to_bytes();

                    socket.write_all(&buf).await.context("Send OK").unwrap();
                }
                Command::Get(key) => {
                    eprintln!("Handling GET");
                    let mut state = self.0.lock().await;
                    let db = &mut state.db;
                    let res = match db.get(&key) {
                        Some(DbValue { value, expiry }) => match expiry {
                            Some(v) => {
                                if get_current_ms() > *v {
                                    db.remove(&key);
                                    RespValue::NullBulkString
                                } else {
                                    RespValue::BulkString(value.as_bytes().into())
                                }
                            }
                            None => RespValue::BulkString(value.as_bytes().into()),
                        },
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
                    eprintln!("Handling INFO");
                    let state = self.0.lock().await;
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

                    let res = RespValue::BulkString(lines);
                    let buf = res.to_bytes();

                    socket
                        .write_all(&buf)
                        .await
                        .context("Send INFO response")
                        .unwrap();
                }
                Command::ReplConf {
                    listening_port: _,
                    capa: _,
                } => {
                    eprintln!("Handling REPLCONF");
                    let res = RespValue::SimpleString(String::from("OK"));
                    let buf = res.to_bytes();

                    socket.write_all(&buf).await.context("Send OK").unwrap();
                }
                Command::PSync {
                    repl_id: _,
                    repl_offset: _,
                } => {
                    eprintln!("Handling PSYNC");
                    let mut state = self.0.lock().await;
                    let res = RespValue::SimpleString(
                        vec![
                            "FULLRESYNC",
                            &state.info.master_repl_id.iter().collect::<String>(),
                            "0",
                        ]
                        .join(" "),
                    );
                    let buf = res.to_bytes();

                    socket
                        .write_all(&buf)
                        .await
                        .context("Send PSYNC response")
                        .unwrap();

                    let empty_rdb = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").expect("Valid HEX string");
                    let res = RespValue::BulkString(empty_rdb);
                    let buf = res.to_bytes();
                    let buf = &buf[..buf.len() - 2];

                    socket
                        .write_all(&buf)
                        .await
                        .context("Send empty RDB file")
                        .unwrap();

                    state.info.slave_conns.push(socket);
                    break;
                }
            };
        }
    }
}
