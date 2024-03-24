use anyhow::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn handler(socket: &mut TcpStream) {
    let expected_req = b"*1\r\n$4\r\nping\r\n";
    let mut remaining_n: usize = 0;
    loop {
        let mut req = [0u8; 1024];
        let buf = b"+PONG\r\n";
        let n = socket
            .read(&mut req)
            .await
            .context("Read from client")
            .unwrap();
        remaining_n += n;
        if remaining_n >= expected_req.len() {
            socket
                .write_all(buf)
                .await
                .context("Send PONG response")
                .unwrap();
            remaining_n -= expected_req.len();
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

    loop {
        let (mut socket, _) = listener
            .accept()
            .await
            .context("Accept connection")
            .unwrap();
        tokio::spawn(async move { handler(&mut socket).await });
    }
}
