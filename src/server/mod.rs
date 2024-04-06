use std::collections::HashMap;

use crate::resp::RespValue;
use crate::{command::Command, utils::get_current_ms};
use anyhow::Context;
use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use tokio::{io::AsyncWriteExt, net::TcpStream};

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
    value: Vec<u8>, // supports only binary string values
    expiry: Option<u128>,
}

pub struct RedisStore(HashMap<Vec<u8>, RSValue>);

impl RedisStore {
    fn get(&mut self, key: &Vec<u8>) -> Option<Vec<u8>> {
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

    fn set(&mut self, key: &Vec<u8>, value: &Vec<u8>, px: &Option<usize>) {
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
