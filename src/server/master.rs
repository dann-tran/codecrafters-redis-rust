use std::{collections::HashMap, path::Path, sync::Arc};

use anyhow::Context;
use async_trait::async_trait;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
    task::JoinSet,
    time,
};

use crate::{
    command::{Command, ConfigArg, ReplConfArg},
    db::RedisDb,
    rdb::parse_rdb,
    resp::RespValue,
    server::{
        handle_info, send_bulk_string, send_cmd, send_integer, send_simple_error,
        send_simple_string, store::RedisStore,
    },
};

use super::{
    handle_echo, handle_get, handle_ping, handle_type, send_resp, MasterInfo, RedisServerHandler,
};

#[derive(Clone)]
struct ServerConfig {
    dir: Option<String>,
    dbfilename: Option<String>,
}

#[derive(Clone)]
pub struct MasterServer {
    config: ServerConfig,
    master_info: Arc<Mutex<MasterInfo>>,
    store: RedisStore,
    repl_conns: Arc<Mutex<Vec<Arc<Mutex<TcpStream>>>>>,
}

impl MasterServer {
    pub async fn new(dir: Option<String>, dbfilename: Option<String>) -> Self {
        let databases = load_rdb(&dir, &dbfilename).await;

        Self {
            config: ServerConfig { dir, dbfilename },
            master_info: Arc::new(Mutex::new(MasterInfo::new())),
            store: databases.map_or_else(|| RedisStore::new(), |dbs| RedisStore::from(dbs)),
            repl_conns: Arc::new(Mutex::new(Vec::new())),
        }
    }
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
                    handle_ping(&mut socket).await;
                }
                Command::Echo(val) => {
                    handle_echo(&mut socket, &val).await;
                }
                Command::Set { key, value, px } => {
                    eprintln!("Handling SET from client");

                    self.store.set(&key, value.clone(), px.clone()).await;

                    eprintln!("Propagate SET command to slaves");
                    let cmd = Command::Set { key, value, px };

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
                    handle_get(&mut socket, &self.store, &key).await;
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
                Command::Keys => {
                    let keys = self.store.keys().await;
                    let resp = RespValue::Array(
                        keys.into_iter().map(|k| RespValue::BulkString(k)).collect(),
                    );
                    send_resp(&mut socket, &resp).await;
                }
                Command::LookupType(key) => {
                    handle_type(&mut socket, &self.store, &key).await;
                }
                Command::XAdd {
                    key,
                    entry_id,
                    data,
                } => {
                    eprintln!("Handling XADD");
                    match self.store.xadd(&key, entry_id, data).await {
                        Ok(res) => {
                            send_bulk_string(&mut socket, &res.as_bytes()).await;
                        }
                        Err(err) => {
                            send_simple_error(&mut socket, &err.to_string()).await;
                        }
                    }
                }
                Command::XRange { key, start, end } => {
                    eprintln!("Handling XRANGE");
                    let data = self.store.xrange(&key, start, end).await;
                    let resp = RespValue::Array(
                        data.into_iter()
                            .map(|(entry_id, entry_data)| {
                                RespValue::Array(vec![
                                    RespValue::BulkString(entry_id),
                                    RespValue::Array(
                                        entry_data
                                            .into_iter()
                                            .map(|x| RespValue::BulkString(x))
                                            .collect(),
                                    ),
                                ])
                            })
                            .collect(),
                    );
                    send_resp(&mut socket, &resp).await;
                }
                Command::XRead { block, streams } => {
                    eprintln!("Handling XREAD");

                    let mut data = self.store.xread(&streams).await;

                    if let Some(dur) = block {
                        if data.iter().all(|(_, entries)| entries.len() == 0) {
                            let block_read = async {
                                let mut join_set = JoinSet::new();
                                for arg in streams {
                                    let key = arg.key.clone();
                                    let mut receiver = self.store.get_stream_receiver(&key).await;
                                    join_set.spawn(
                                        async move { (key, receiver.recv().await.unwrap()) },
                                    );
                                }
                                join_set.join_next().await.expect("Join set is not empty")
                            };
                            if let Ok(res) = time::timeout(dur, block_read).await {
                                let (key, (entry_id, kvs)) = res.unwrap();
                                let (_, serialized_data) =
                                    data.iter_mut().find(|(k, _)| **k == key).unwrap();
                                let kvs_len = kvs.len();
                                let serialized_kvs = kvs.into_iter().fold(
                                    Vec::with_capacity(kvs_len * 2),
                                    |mut acc, (k, v)| {
                                        acc.push(k);
                                        acc.push(v);
                                        acc
                                    },
                                );
                                serialized_data.push((entry_id.as_bytes(), serialized_kvs));
                            }
                        }
                    }

                    let resp = if block != None
                        && data.iter().all(|(_, entries)| entries.len() == 0)
                    {
                        RespValue::NullBulkString
                    } else {
                        RespValue::Array(
                            data.into_iter()
                                .map(|(key, data)| {
                                    RespValue::Array(vec![
                                        RespValue::BulkString(key),
                                        RespValue::Array(
                                            data.into_iter()
                                                .map(|(entry_id, entry_data)| {
                                                    RespValue::Array(vec![
                                                        RespValue::BulkString(entry_id),
                                                        RespValue::Array(
                                                            entry_data
                                                                .into_iter()
                                                                .map(|x| RespValue::BulkString(x))
                                                                .collect(),
                                                        ),
                                                    ])
                                                })
                                                .collect(),
                                        ),
                                    ])
                                })
                                .collect(),
                        )
                    };
                    send_resp(&mut socket, &resp).await;
                }
            };
        }
    }
}

async fn load_rdb(
    dir: &Option<String>,
    dbfilename: &Option<String>,
) -> Option<HashMap<u32, RedisDb>> {
    if let (Some(dir), Some(dbfilename)) = (dir, dbfilename) {
        if let Ok(bytes) = fs::read(Path::new(dir).join(dbfilename)).await {
            if let Ok(rdb) = parse_rdb(&bytes) {
                return Some(rdb.databases);
            }
        }
    }
    eprintln!("Unable to parse RDB file");
    None
}
