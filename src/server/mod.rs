use std::collections::HashMap;

use crate::command::Command;
use crate::model::MasterInfo;
use crate::resp::RespValue;
use anyhow::Context;
use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use tokio::{io::AsyncWriteExt, net::TcpStream};

pub mod master;
pub mod replica;
pub mod store;

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
