use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use tokio::{io::AsyncReadExt, net::TcpStream, sync::Mutex};

use crate::{
    command::{Command, ReplConfArg},
    rdb::{parse_rdb, Rdb},
    resp::{decode, RespValue},
    server::{handle_info, send_cmd},
    utils::{bytes2usize, split_by_clrf},
};

use super::{
    assert_recv_simple_string, handle_echo, handle_get, handle_ping, handle_type, handle_xadd,
    store::RedisStore, MasterInfo, RedisServerHandler,
};

#[derive(Clone)]
pub struct ReplicaServer {
    master_info: MasterInfo,
    store: RedisStore,
    offset: Arc<Mutex<usize>>,
}

#[async_trait]
impl RedisServerHandler for ReplicaServer {
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
                Command::Get(key) => {
                    handle_get(&mut socket, &self.store, &key).await;
                }
                Command::Info(_) => {
                    eprintln!("Handling INFO from client");
                    handle_info(&mut socket, "slave", &self.master_info).await;
                }
                Command::LookupType(key) => {
                    handle_type(&mut socket, &self.store, &key).await;
                }
                Command::XAdd {
                    key,
                    entry_id,
                    data,
                } => {
                    handle_xadd(&mut socket, &self.store, &key, &entry_id, data).await;
                }
                c => {
                    panic!("Unsupported command: {:?}", c);
                }
            }
        }
    }
}

impl ReplicaServer {
    fn parse_fullresync(buf: &[u8]) -> anyhow::Result<(MasterInfo, &[u8])> {
        let (val, remaining) = decode(&buf).context("Parse FULLRESYNC response from master")?;
        let text = match val {
            RespValue::SimpleString(x) => x,
            o => return Err(anyhow::anyhow!("Unexpected value: {:?}", o)),
        };

        let args = text.split(" ").collect::<Vec<&str>>();

        let (arg, args) = args.split_first().context("Extract FULLRESYNC verb")?;
        if arg.to_ascii_lowercase() != "fullresync" {
            return Err(anyhow::anyhow!("Command is not FULLRESYNC but: {}", arg));
        }

        let (repl_id, args) = args.split_first().context("Extract REPL_ID")?;
        let repl_id: [char; 40] = match repl_id.chars().collect::<Vec<char>>().try_into() {
            Ok(x) => x,
            Err(_) => return Err(anyhow::anyhow!("Replication ID must be 40 chars long")),
        };

        let (repl_offset, args) = args.split_first().context("Extract replication offset")?;
        let repl_offset = repl_offset
            .parse::<usize>()
            .context("Parse replication offset to usize")?;

        assert!(args.is_empty());

        Ok((
            MasterInfo {
                repl_id,
                repl_offset,
            },
            remaining,
        ))
    }

    fn parse_rdb(buf: &[u8]) -> anyhow::Result<(Rdb, &[u8])> {
        // $<length>\r\n<contents>
        if !buf.starts_with(b"$") {
            return Err(anyhow::anyhow!(
                "Expect RDB to start with '$', found: {:?}",
                buf
            ));
        }

        let (length, remaining) = split_by_clrf(&buf[1..]).context("Extract length bytes")?;
        let length = bytes2usize(&length)?;

        let (rdb, remaining) = remaining.split_at(length);
        let rdb = parse_rdb(rdb)?;
        Ok((rdb, remaining))
    }

    async fn handle_cmd_from_master(
        &mut self,
        recv_buf: &[u8],
        buf_len: usize,
        socket: &mut TcpStream,
    ) -> anyhow::Result<()> {
        let mut ptr = 0;
        while ptr < buf_len {
            let (cmd, remaining_bytes) = Command::from_bytes(&recv_buf[ptr..])?;
            let offset_delta = recv_buf.len() - remaining_bytes.len() - ptr;
            ptr += offset_delta;

            match cmd {
                Command::Set { key, value, px } => {
                    eprintln!("Handling SET propagation from master");
                    self.store.set(&key, &value, &px).await;
                }
                Command::ReplConf(ReplConfArg::GetAck) => {
                    eprintln!("Handling REPLCONF GETACK * from master");

                    let offset = self.offset.lock().await;
                    let cmd = Command::ReplConf(ReplConfArg::Ack(*offset));
                    drop(offset);
                    send_cmd(socket, &cmd).await;
                }
                _ => {
                    // Ignore all other commands
                }
            }

            let mut offset = self.offset.lock().await;
            *offset += offset_delta;
            drop(offset);
        }

        Ok(())
    }

    pub async fn new(port: u16, mut socket: TcpStream) -> anyhow::Result<Self> {
        let mut recv_buf = [0u8; 1024];

        // Send PING
        send_cmd(&mut socket, &Command::Ping).await;
        assert_recv_simple_string(&mut recv_buf, &mut socket, "PONG").await;

        // Send REPLCONF listening-port
        send_cmd(
            &mut socket,
            &Command::ReplConf(ReplConfArg::ListeningPort(port)),
        )
        .await;
        assert_recv_simple_string(&mut recv_buf, &mut socket, "OK").await;

        // Send REPLCONF capa
        send_cmd(
            &mut socket,
            &Command::ReplConf(ReplConfArg::Capa(vec![String::from("psync2")])),
        )
        .await;
        assert_recv_simple_string(&mut recv_buf, &mut socket, "OK").await;

        // Send PSYNC
        send_cmd(
            &mut socket,
            &Command::PSync {
                repl_id: None,
                repl_offset: None,
            },
        )
        .await;
        // receive FULLRESYNC <REPL_ID> 0 & RDB file
        let mut n = socket
            .read(&mut recv_buf)
            .await
            .context("Read from master")
            .unwrap();
        let (master_info, mut remaining) = Self::parse_fullresync(&recv_buf)?;
        if recv_buf.len() - remaining.len() >= n {
            let _n = socket
                .read(&mut recv_buf)
                .await
                .context("Read from master")
                .unwrap();
            remaining = &recv_buf[..];
            n = _n;
        }
        let (rdb, remaining) = Self::parse_rdb(&remaining)?;

        let mut server = Self {
            master_info: master_info,
            store: RedisStore::from(rdb.databases),
            offset: Arc::new(Mutex::new(0)),
        };

        // Handle additional commands from master, if any
        let ptr = recv_buf.len() - remaining.len();
        server
            .handle_cmd_from_master(&remaining, n - ptr, &mut socket)
            .await?;

        // spawn a watcher to master socket here
        let mut master_watcher = server.clone();
        tokio::spawn(async move { master_watcher.watch_master(socket).await });

        Ok(server)
    }

    pub async fn watch_master(&mut self, mut socket: TcpStream) {
        let mut recv_buf = [0u8; 1024];
        loop {
            let n = socket
                .read(&mut recv_buf)
                .await
                .context("Read from master")
                .unwrap();

            match self
                .handle_cmd_from_master(&mut recv_buf, n, &mut socket)
                .await
            {
                Ok(_) => {
                    continue;
                }
                Err(_) => todo!(), // Read more bytes into buffer
            };
        }
    }
}
