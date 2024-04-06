use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
    task::JoinSet,
    time,
};

use crate::{
    command::{Command, ConfigArg, ReplConfArg},
    resp::RespValue,
    server::{handle_info, send_bulk_string, send_cmd, send_integer, send_simple_string},
};

use super::{send_resp, MasterInfo, RedisServerHandler, RedisStore};

#[derive(Clone)]
struct ServerConfig {
    dir: Option<String>,
    dbfilename: Option<String>,
}

#[derive(Clone)]
pub struct MasterServer {
    config: ServerConfig,
    master_info: Arc<Mutex<MasterInfo>>,
    store: Arc<Mutex<RedisStore>>,
    repl_conns: Arc<Mutex<Vec<Arc<Mutex<TcpStream>>>>>,
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

                    let mut store = self.store.lock().await;
                    store.set(&key, &value, &px);
                    drop(store);

                    eprintln!("Propagate SET command to slaves");
                    let cmd = Command::Set {
                        key: key.clone(),
                        value: value.clone(),
                        px: px.clone(),
                    };

                    let mut master_info = self.master_info.lock().await;
                    master_info.repl_offset += cmd.to_bytes().len();
                    drop(master_info);

                    let replicas = self.repl_conns.lock().await;
                    for replica in replicas.iter() {
                        let mut conn = replica.lock().await;
                        send_cmd(&mut conn, &cmd).await;
                        eprintln!("Propagated to {}", conn.peer_addr().unwrap());
                    }
                    drop(replicas);

                    send_simple_string(&mut socket, "OK").await;
                }
                Command::Get(key) => {
                    eprintln!("Handling GET from client");

                    let mut store = self.store.lock().await;
                    let value = store.get(&key);
                    drop(store);

                    let res = match value {
                        Some(x) => RespValue::BulkString(x),
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
                    handle_info(
                        &mut socket,
                        "master",
                        &self.master_info.lock().await.clone(),
                    )
                    .await;
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
                    let master_info = self.master_info.lock().await;
                    let repl_id = master_info.repl_id;
                    drop(master_info);
                    let res =
                        vec!["FULLRESYNC", &repl_id.iter().collect::<String>(), "0"].join(" ");
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
                    repl_conns.push(Arc::new(Mutex::new(socket)));
                    drop(repl_conns);

                    break;
                }
                Command::Wait {
                    repl_ack_num,
                    timeout_dur,
                } => {
                    eprintln!("Handling WAIT from client");

                    let master_info = self.master_info.lock().await;
                    let master_repl_offset = master_info.repl_offset;
                    drop(master_info);
                    eprintln!("Master repl offset: {}", master_repl_offset);

                    let res_value = if master_repl_offset == 0 {
                        let replicas = self.repl_conns.lock().await;
                        let replica_num = replicas.len();
                        drop(replicas);
                        replica_num
                    } else {
                        let mut ack_num = 0usize;
                        let getack_cmd = Command::ReplConf(ReplConfArg::GetAck);

                        let getacks = async {
                            let mut join_set = JoinSet::new();

                            let repl_conns = self.repl_conns.lock().await;
                            for conn in repl_conns.iter() {
                                let conn = conn.clone();
                                let cmd = getack_cmd.clone();

                                join_set.spawn(async move {
                                    let mut conn = conn.lock().await;
                                    let peer_addr = conn.peer_addr().unwrap();
                                    // Send GETACK to replica
                                    eprintln!("Send GETACK to replica at {:?}", peer_addr);
                                    send_cmd(&mut conn, &cmd).await;

                                    // Read ACK from replica
                                    let mut buf = [0u8; 1024];
                                    conn.read(&mut buf)
                                        .await
                                        .context("Read from replica")
                                        .unwrap();
                                    drop(conn);

                                    let (cmd, _) = Command::from_bytes(&buf).unwrap();
                                    eprintln!(
                                        "Received from replica {:?} command: {:?}",
                                        peer_addr, cmd
                                    );

                                    if let Command::ReplConf(ReplConfArg::Ack(_)) = cmd {
                                        true
                                    } else {
                                        false
                                    }
                                });
                            }
                            drop(repl_conns);

                            while ack_num < repl_ack_num && !join_set.is_empty() {
                                if let Ok(ack) =
                                    join_set.join_next().await.expect("Join set is not empty")
                                {
                                    if ack {
                                        ack_num += 1;
                                    }
                                }
                            }
                        };
                        let _ = time::timeout(timeout_dur, getacks).await;
                        ack_num
                    };

                    send_integer(&mut socket, res_value as i64).await;
                }
                Command::Config(arg) => match arg {
                    ConfigArg::Get(key) => {
                        let value = match &key.to_ascii_lowercase()[..] {
                            "dir" => &self.config.dir,
                            "dbfilename" => &self.config.dbfilename,
                            s => panic!("Unexpected CONFIG GET key: {}", s),
                        };
                        let resp = RespValue::Array(vec![
                            RespValue::BulkString(key.as_bytes().to_vec()),
                            RespValue::BulkString(
                                (match value {
                                    Some(val) => val.as_bytes(),
                                    None => b"",
                                })
                                .to_vec(),
                            ),
                        ]);
                        send_resp(&mut socket, &resp).await;
                    }
                },
            };
        }
    }
}

impl MasterServer {
    pub fn new(dir: Option<String>, dbfilename: Option<String>) -> Self {
        Self {
            config: ServerConfig { dir, dbfilename },
            master_info: Arc::new(Mutex::new(MasterInfo::new())),
            store: Arc::new(Mutex::new(RedisStore::new())),
            repl_conns: Arc::new(Mutex::new(Vec::new())),
        }
    }
}
