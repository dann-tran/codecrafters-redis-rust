use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use tokio::{io::AsyncReadExt, net::TcpStream, sync::Mutex};

use crate::{
    command::{Command, ReplConfArg},
    resp::{decode, RespValue},
    server::send_simple_string,
    utils::{bytes2usize, split_by_clrf},
};

use super::{MasterInfo, RedisServerGetter, RedisServerHandler, RedisStore};

#[derive(Clone)]
pub struct ReplicaServer {
    pub(crate) master_info: MasterInfo,
    pub(crate) store: Arc<Mutex<RedisStore>>,
}

impl RedisServerGetter for ReplicaServer {
    fn get_store(&self) -> &Arc<Mutex<RedisStore>> {
        &self.store
    }

    fn get_role(&self) -> String {
        "slave".to_string()
    }

    fn get_master_info(&self) -> &MasterInfo {
        &self.master_info
    }
}

#[async_trait]
impl RedisServerHandler for ReplicaServer {
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

        send_simple_string(socket, "OK").await;
    }

    async fn handle_replconf(&self, _socket: &mut TcpStream) {
        panic!("Replication server does not expect REPLCONF command from client");
    }

    async fn handle_psync(&self, _socket: TcpStream) {
        panic!("Replication server does not expect PSYNC command");
    }
}

impl ReplicaServer {
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

    async fn handle_cmd_from_master(
        &self,
        recv_buf: &[u8],
        n: usize,
        socket: &mut TcpStream,
    ) -> anyhow::Result<()> {
        let mut ptr = 0;

        while ptr < n {
            let (cmd, remaining_bytes) = Command::from_bytes(&recv_buf[ptr..])?;

            ptr = recv_buf.len() - remaining_bytes.len();

            match cmd {
                Command::Set { key, value, px } => {
                    eprintln!("Handling SET propagation from master");

                    let mut store = self.store.lock().await;
                    store.set(&key, &value, &px);
                    drop(store);
                }
                Command::ReplConf(ReplConfArg::GetAck) => {
                    eprintln!("Handling REPLCONF GETACK * from master");

                    let cmd = Command::ReplConf(ReplConfArg::Ack(0));
                    Self::send_cmd(socket, &cmd).await;
                }
                c => panic!("Unexpected command from master: {:?}", c),
            }
        }

        Ok(())
    }

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

    fn parse_rdb(buf: &[u8]) -> anyhow::Result<(&[u8], &[u8])> {
        // $<length>\r\n<contents>
        if !buf.starts_with(b"$") {
            return Err(anyhow::anyhow!(
                "Expect RDB to start with '$', found: {:?}",
                buf
            ));
        }

        let (length, remaining) = split_by_clrf(&buf[1..]).context("Extract length bytes")?;
        let length = bytes2usize(&length)?;

        Ok(remaining.split_at(length))
    }

    pub async fn new(port: u16, mut socket: TcpStream) -> anyhow::Result<Self> {
        let mut recv_buf = [0u8; 1024];

        // Send PING
        Self::send_cmd(&mut socket, &Command::Ping).await;
        Self::assert_recv_simple_string(&mut recv_buf, &mut socket, "PONG").await;

        // Send REPLCONF listening-port
        Self::send_cmd(
            &mut socket,
            &Command::ReplConf(ReplConfArg::ListeningPort(port)),
        )
        .await;
        Self::assert_recv_simple_string(&mut recv_buf, &mut socket, "OK").await;

        // Send REPLCONF capa
        Self::send_cmd(
            &mut socket,
            &Command::ReplConf(ReplConfArg::Capa(vec![String::from("psync2")])),
        )
        .await;
        Self::assert_recv_simple_string(&mut recv_buf, &mut socket, "OK").await;

        // Send PSYNC
        Self::send_cmd(
            &mut socket,
            &Command::PSync {
                repl_id: None,
                repl_offset: None,
            },
        )
        .await;
        // receive FULLRESYNC <REPL_ID> 0 & RDB file
        let n = socket
            .read(&mut recv_buf)
            .await
            .context("Read from master")
            .unwrap();
        let (master_info, remaining) = Self::parse_fullresync(&recv_buf)?;
        let (_, remaining) = Self::parse_rdb(&remaining)?;

        let server = Self {
            master_info: master_info,
            store: Arc::new(Mutex::new(RedisStore::new())),
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
                Ok(_) => todo!(),
                Err(_) => todo!(), // Read more bytes into buffer
            };
        }
    }
}
