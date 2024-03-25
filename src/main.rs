use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Context;
use redis_starter_rust::command::{respond, Command};
use redis_starter_rust::resp::{decode, RespValue};
use redis_starter_rust::Db;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

async fn handler(mut socket: TcpStream, db: Db) {
    // let expected_req = b"*1\r\n$4\r\nping\r\n";
    loop {
        let mut buf = [0u8; 1024];
        socket
            .read(&mut buf)
            .await
            .context("Read from client")
            .unwrap();

        let (req, _) = decode(&buf);
        match req {
            RespValue::Array(vec) => {
                let mut values = vec.iter();
                let cmd = match values.next() {
                    Some(RespValue::BulkString(c)) => {
                        let c = String::from_utf8(c.clone())
                            .expect("Invalid command string")
                            .to_ascii_lowercase();
                        let c = c.as_str();
                        match c {
                            "ping" => Command::Ping,
                            "echo" => {
                                let val = match values.next() {
                                    Some(RespValue::BulkString(v)) => v,
                                    _ => {
                                        panic!("Invalid echo argument")
                                    }
                                };
                                Command::Echo(val.clone())
                            }
                            "get" => {
                                let val = match values.next() {
                                    Some(RespValue::BulkString(v)) => v,
                                    _ => {
                                        panic!("Invalid get argument")
                                    }
                                };
                                Command::Get(
                                    String::from_utf8(val.clone()).expect("Key must be UTF-8"),
                                )
                            }
                            "set" => {
                                let key = match values.next() {
                                    Some(RespValue::BulkString(v)) => v,
                                    _ => {
                                        panic!("Invalid key")
                                    }
                                };
                                let value = match values.next() {
                                    Some(RespValue::BulkString(v)) => v,
                                    _ => {
                                        panic!("Invalid value")
                                    }
                                };
                                Command::Set {
                                    key: String::from_utf8(key.clone()).expect("Valid UTF-8 key"),
                                    value: String::from_utf8(value.clone())
                                        .expect("Valid UTF-8 value"),
                                }
                            }
                            _ => {
                                panic!("Unrecognised command")
                            }
                        }
                    }
                    _ => {
                        panic!("Invalid")
                    }
                };
                respond(&mut socket, &db, &cmd).await;
            }
            _ => {
                panic!("Invalid request")
            }
        }
    }
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .context("List at 127.0.0.1:6379")
        .unwrap();

    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener
            .accept()
            .await
            .context("Accept connection")
            .unwrap();
        let db = db.clone();
        tokio::spawn(async move { handler(socket, db).await });
    }
}
