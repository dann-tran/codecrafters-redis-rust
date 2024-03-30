use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use redis_starter_rust::server::{RedisServer, RedisState};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

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

    let state = match replicaof {
        Some(v) => {
            let (master_host, remaining) = v.split_first().expect("Host argument");
            let (master_port, remaining) = remaining.split_first().expect("Port argument");
            assert!(remaining.is_empty());

            let addr = format!("{}:{}", master_host, master_port);
            RedisState::new_slave(port, &addr).await
        }
        None => RedisState::new_master(),
    };

    let state = Arc::new(Mutex::new(state));

    loop {
        let (socket, _) = listener
            .accept()
            .await
            .context("Accept connection")
            .unwrap();
        let state = state.clone();
        tokio::spawn(async move { RedisServer { socket, state }.start().await });
    }
}
