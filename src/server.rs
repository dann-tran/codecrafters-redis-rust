use std::{collections::HashMap, sync::Arc};

use crate::{
    command::{Command, FromBytes},
    utils::get_current_ms,
};
use crate::{resp::RespValue, ToBytes};
use anyhow::Context;
use async_trait::async_trait;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

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
        eprintln!("Handling SET");
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
    async fn bind(&mut self, mut socket: TcpStream) {
        loop {
            let cmd = Self::recv_cmd(&mut socket).await;
            match cmd {
                Command::Ping => {
                    eprintln!("Handling PING");
                    send_simple_string(&mut socket, "PONG").await;
                }
                Command::Echo(val) => {
                    eprintln!("Handling ECHO");
                    send_bulk_string(&mut socket, &val).await;
                }
                Command::Set { key, value, px } => {
                    self.handle_set(&mut socket, &key, &value, &px).await
                }
                Command::Get(key) => {
                    eprintln!("Handling GET");

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
                    eprintln!("Handling INFO");

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
                Command::ReplConf {
                    listening_port: _,
                    capa: _,
                } => self.handle_replconf(&mut socket).await,
                Command::PSync {
                    repl_id: _,
                    repl_offset: _,
                } => {
                    self.handle_psync(socket).await;
                    break;
                }
            }
        }
    }

    async fn recv_cmd(socket: &mut TcpStream) -> Command {
        let mut buf = [0u8; 1024];
        socket
            .read(&mut buf)
            .await
            .context("Read from client")
            .unwrap();

        Command::from_bytes(&buf)
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

#[derive(Clone)]
pub struct RedisMaster {
    pub(crate) master_info: MasterInfo,
    pub(crate) store: Arc<Mutex<RedisStore>>,
    pub(crate) repl_conns: Arc<Mutex<Vec<TcpStream>>>,
}

impl RedisServerGetter for RedisMaster {
    fn get_store(&self) -> &Arc<Mutex<RedisStore>> {
        &self.store
    }

    fn get_role(&self) -> String {
        "master".to_string()
    }

    fn get_master_info(&self) -> &MasterInfo {
        &self.master_info
    }
}

#[async_trait]
impl RedisServerHandler for RedisMaster {
    async fn handle_set(
        &self,
        socket: &mut TcpStream,
        key: &String,
        value: &String,
        px: &Option<usize>,
    ) {
        let mut store = self.store.lock().await;
        store.set(key, value, px);
        drop(store);

        eprintln!("Propagate SET commands to slaves");
        let cmd = Command::Set {
            key: key.clone(),
            value: value.clone(),
            px: px.clone(),
        };
        let buf = cmd.to_bytes();
        let mut repl_conns = self.repl_conns.lock().await;
        for conn in repl_conns.iter_mut() {
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

    async fn handle_replconf(&self, socket: &mut TcpStream) {
        eprintln!("Handling REPLCONF");
        send_simple_string(socket, "OK").await;
    }

    async fn handle_psync(&self, mut socket: TcpStream) {
        eprintln!("Handling PSYNC");

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
    }
}

impl RedisMaster {
    pub fn new() -> Self {
        Self {
            master_info: MasterInfo::new(),
            store: Arc::new(Mutex::new(RedisStore::new())),
            repl_conns: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[derive(Clone)]
pub struct RedisRepl {
    pub(crate) master_info: MasterInfo,
    pub(crate) store: Arc<Mutex<RedisStore>>,
    pub(crate) master_conn: Arc<Mutex<TcpStream>>,
}

impl RedisServerGetter for RedisRepl {
    fn get_store(&self) -> &Arc<Mutex<RedisStore>> {
        &self.store
    }

    fn get_role(&self) -> String {
        "slave".to_string()
    }

    fn get_master_info(&self) -> &MasterInfo {
        &self.master_info
    }
}

#[async_trait]
impl RedisServerHandler for RedisRepl {
    async fn handle_set(
        &self,
        socket: &mut TcpStream,
        key: &String,
        value: &String,
        px: &Option<usize>,
    ) {
        eprintln!("Handle SET");
        let mut store = self.store.lock().await;
        store.set(key, value, px);
        drop(store);

        let res = RespValue::SimpleString("OK".into());
        let buf = res.to_bytes();

        socket.write_all(&buf).await.context("Send OK").unwrap();
    }

    async fn handle_replconf(&self, _socket: &mut TcpStream) {
        panic!("Replication server does not expect REPLCONF command");
    }

    async fn handle_psync(&self, _socket: TcpStream) {
        panic!("Replication server does not expect PSYNC command");
    }
}

impl RedisRepl {
    async fn assert_recv_simple_string(buf: &mut [u8], socket: &mut TcpStream, expected: &str) {
        let expected_encoded = &RespValue::SimpleString(expected.to_string()).to_bytes();
        socket.read(buf).await.context("Read from client").unwrap();
        assert!(
            buf.starts_with(expected_encoded),
            "Expected: {:?}; Found: {:?}",
            expected_encoded,
            &buf[..expected_encoded.len()]
        );
    }

    pub async fn new(port: u16, master_addr: &str) -> Self {
        let mut socket = TcpStream::connect(master_addr)
            .await
            .context("Connect to master")
            .unwrap();
        let mut recv_buf = [0u8; 1024];

        // Send PING
        Self::send_cmd(&mut socket, &Command::Ping).await;
        Self::assert_recv_simple_string(&mut recv_buf, &mut socket, "PONG").await;

        // Send REPLCONF listening-port
        Self::send_cmd(
            &mut socket,
            &Command::ReplConf {
                listening_port: Some(port),
                capa: vec![],
            },
        )
        .await;
        Self::assert_recv_simple_string(&mut recv_buf, &mut socket, "OK").await;

        // Send REPLCONF capa
        Self::send_cmd(
            &mut socket,
            &Command::ReplConf {
                listening_port: None,
                capa: vec![String::from("psync2")],
            },
        )
        .await;
        Self::assert_recv_simple_string(&mut recv_buf, &mut socket, "OK").await;

        // Send PSYNC
        Self::send_cmd(
            &mut socket,
            &Command::PSync {
                repl_id: None,
                repl_offset: None,
            },
        )
        .await;

        Self {
            master_info: MasterInfo::new(),
            store: Arc::new(Mutex::new(RedisStore::new())),
            master_conn: Arc::new(Mutex::new(socket)),
        }
    }
}
