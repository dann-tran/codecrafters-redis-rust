use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::Context;
use clap::Parser;
use redis_starter_rust::server::{
    master::MasterServer, replica::ReplicaServer, RedisServerHandler,
};
use tokio::net::{TcpListener, TcpStream};

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
    match replicaof {
        Some(v) => {
            let (master_host, remaining) = v.split_first().expect("Host argument");
            let (master_port, remaining) = remaining.split_first().expect("Port argument");
            assert!(remaining.is_empty());

            let master_addr = format!("{}:{}", master_host, master_port);
            let master_conn = TcpStream::connect(master_addr)
                .await
                .context("Connect to master")
                .unwrap();
            let server = ReplicaServer::new(port, master_conn).await.unwrap();

            let listener = TcpListener::bind(&addr)
                .await
                .context(format!("Listen at {}", addr))
                .unwrap();

            loop {
                let (socket, _) = listener
                    .accept()
                    .await
                    .context("Accept connection")
                    .unwrap();
                eprintln!("Accept conn from {}", socket.peer_addr().unwrap());

                let mut server = server.clone();
                tokio::spawn(async move { server.handle_conn(socket).await });
            }
        }
        None => {
            let server = MasterServer::new();

            let listener = TcpListener::bind(&addr)
                .await
                .context(format!("Listen at {}", addr))
                .unwrap();

            loop {
                let (socket, _) = listener
                    .accept()
                    .await
                    .context("Accept connection")
                    .unwrap();
                eprintln!("Accept conn from {}", socket.peer_addr().unwrap());

                let mut server = server.clone();
                tokio::spawn(async move { server.handle_conn(socket).await });
            }
        }
    }
}
