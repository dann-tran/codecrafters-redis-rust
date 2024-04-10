use std::collections::HashMap;

use crate::command::Command;
use crate::db::stream::StreamEntryID;
use crate::resp::RespValue;
use anyhow::Context;
use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use tokio::{io::AsyncWriteExt, net::TcpStream};

use self::store::RedisStore;

pub mod master;
pub mod replica;
mod store;

#[derive(Clone)]
pub(crate) struct MasterInfo {
    pub(crate) repl_id: [char; 40],
    pub(crate) repl_offset: usize,
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

async fn send_resp(socket: &mut TcpStream, value: &RespValue) {
    let buf = value.to_bytes();
    socket
        .write_all(&buf)
        .await
        .context(format!("Send {:?}", &value))
        .unwrap();
}

async fn send_simple_string(socket: &mut TcpStream, res: &str) {
    send_resp(socket, &RespValue::SimpleString(res.into())).await;
}

async fn send_bulk_string(socket: &mut TcpStream, res: &[u8]) {
    send_resp(socket, &RespValue::BulkString(res.into())).await;
}

async fn send_integer(socket: &mut TcpStream, res: i64) {
    send_resp(socket, &RespValue::Integer(res)).await;
}

async fn send_cmd(socket: &mut TcpStream, cmd: &Command) {
    let send_buf = cmd.to_bytes();
    socket
        .write_all(&send_buf)
        .await
        .context(format!("Send {:?}", cmd))
        .unwrap();
}

async fn assert_recv_simple_string(buf: &mut [u8], socket: &mut TcpStream, expected: &str) {
    socket.read(buf).await.context("Read from client").unwrap();
    let expected_encoded = &RespValue::SimpleString(expected.to_string()).to_bytes();
    assert!(
        buf.starts_with(expected_encoded),
        "Expected: {:?}; Found: {:?}",
        expected_encoded,
        &buf[..expected_encoded.len()]
    );
}

#[async_trait]
pub trait RedisServerHandler {
    async fn handle_conn(&mut self, mut socket: TcpStream);
}

async fn handle_info(socket: &mut TcpStream, role: &str, master_info: &MasterInfo) {
    let mut info_map = HashMap::<Vec<u8>, Vec<u8>>::new();
    info_map.insert(b"role".to_vec(), role.as_bytes().to_vec());

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
    let lines = info_map
        .into_iter()
        .fold(Vec::new(), |mut acc, (mut k, mut v)| {
            if !acc.is_empty() {
                acc.push(b'\n');
            }
            acc.append(&mut k);
            acc.push(b':');
            acc.append(&mut v);
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

async fn handle_get(socket: &mut TcpStream, store: &RedisStore, key: &Vec<u8>) {
    eprintln!("Handling GET from client");

    let value = store.get(key).await;

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

async fn handle_ping(socket: &mut TcpStream) {
    eprintln!("Handling PING from client");
    send_simple_string(socket, "PONG").await;
}

async fn handle_echo(socket: &mut TcpStream, val: &Vec<u8>) {
    eprintln!("Handling ECHO from client");
    send_bulk_string(socket, val).await;
}

async fn handle_type(socket: &mut TcpStream, store: &RedisStore, key: &Vec<u8>) {
    let res = store
        .lookup_type(key)
        .await
        .map_or("none".to_string(), |t| t.to_string());
    send_simple_string(socket, &res).await;
}

async fn handle_xadd(
    socket: &mut TcpStream,
    store: &RedisStore,
    key: &Vec<u8>,
    entry_id: &StreamEntryID,
    data: HashMap<Vec<u8>, Vec<u8>>,
) {
    eprintln!("Handling XADD");
    store.xadd(key, entry_id, data).await;
    send_bulk_string(socket, &entry_id.as_bytes()).await;
}
