use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Context;
use redis_starter_rust::command::{respond, Command};
use redis_starter_rust::resp::decode_array_of_bulkstrings;
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

        let args = decode_array_of_bulkstrings(&buf);
        let mut args = args.iter();
        let verb = args.next().expect("A command verb must be present");
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
                let px = args
                    .next()
                    .map(|s| s.to_ascii_lowercase())
                    .filter(|s| s == b"px")
                    .and_then(|_| args.next())
                    .and_then(|s| String::from_utf8(s.clone()).ok())
                    .and_then(|s| s.parse::<usize>().ok());

                Command::Set {
                    key: String::from_utf8(key.clone()).expect("Valid UTF-8 key"),
                    value: String::from_utf8(value.clone()).expect("Valid UTF-8 value"),
                    px: px,
                }
            }
            _ => panic!("Unknown verb: {:?}", verb),
        };
        respond(&mut socket, &db, &cmd).await;
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
