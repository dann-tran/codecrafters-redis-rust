use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::Mutex};

use crate::{command::Command, resp::RespValue, server::send_simple_string};

use super::{MasterInfo, RedisServerGetter, RedisServerHandler, RedisStore};

#[derive(Clone)]
pub struct MasterServer {
    master_info: MasterInfo,
    store: Arc<Mutex<RedisStore>>,
    repl_conns: Arc<Mutex<Vec<TcpStream>>>,
}

impl RedisServerGetter for MasterServer {
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
impl RedisServerHandler for MasterServer {
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

        send_simple_string(socket, "OK").await;
    }

    async fn handle_replconf(&self, socket: &mut TcpStream) {
        send_simple_string(socket, "OK").await;
    }

    async fn handle_psync(&self, mut socket: TcpStream) {
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
