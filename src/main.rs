use core::panic;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};

use anyhow::Context;
use clap::Parser;
use redis_starter_rust::command::{respond, Command, InfoArg};
use redis_starter_rust::resp::decode_array_of_bulkstrings;
use redis_starter_rust::{RedisInfo, RedisRole, RedisState, StateWithMutex};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

async fn handler(mut socket: TcpStream, db: StateWithMutex) {
    loop {
        let mut buf = [0u8; 1024];
        socket
            .read(&mut buf)
            .await
            .context("Read from client")
            .unwrap();

        let args = decode_array_of_bulkstrings(&buf);
        let mut args = args.iter();
        let verb = args
            .next()
            .expect("A command verb must be present")
            .to_ascii_lowercase();
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
                let is_px_present = match args.next().map(|s| s.to_ascii_lowercase()) {
                    Some(c) => match &c[..] {
                        b"px" => true,
                        _ => panic!("Invalid SET arguments"),
                    },
                    None => false,
                };
                let px = match is_px_present {
                    true => {
                        let px = args.next().expect("expiry argument");
                        let px = String::from_utf8(px.clone()).expect("Valid string");
                        let px = px.parse::<usize>().expect("Valid number");
                        Some(px)
                    }
                    false => None,
                };

                Command::Set {
                    key: String::from_utf8(key.clone()).expect("Valid UTF-8 key"),
                    value: String::from_utf8(value.clone()).expect("Valid UTF-8 value"),
                    px: px,
                }
            }
            b"info" => {
                let info_arg = args
                    .next()
                    .map(|v| v.to_ascii_lowercase())
                    .map(|v| match &v[..] {
                        b"replication" => InfoArg::Replication,
                        _ => panic!("Invalid info argument {:?}", v),
                    });
                Command::Info(info_arg)
            }
            _ => panic!("Unknown verb: {:?}", verb),
        };
        if args.next().is_some() {
            panic!("Unexpected arguments")
        }
        respond(&mut socket, &db, &cmd).await;
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long, default_value_t = 6379)]
    port: u16,
    #[arg(long, num_args = 2, value_delimiter = ' ')]
    replicaof: Option<Vec<String>>,
}

#[tokio::main]
async fn main() {
    let Cli { port, replicaof } = Cli::parse();

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    let listener = TcpListener::bind(&addr)
        .await
        .context(format!("Listen at {}", addr))
        .unwrap();

    let role = match replicaof {
        Some(v) => {
            let (host, remaining) = v.split_first().expect("Host argument");
            let (port, remaning) = v.split_first().expect("Port argument");
            RedisRole::Slave
        }
        None => RedisRole::Master,
    };
    let info = RedisInfo { role };
    let state = RedisState {
        info,
        db: HashMap::new(),
    };

    let db = Arc::new(Mutex::new(state));

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
