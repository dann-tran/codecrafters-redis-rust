use std::{io::Write, net::TcpListener};

use anyhow::Context;

fn main() -> anyhow::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        let mut stream = stream.context("Accept TCP connection from client")?;
        let buf = b"+PONG\r\n";
        stream.write_all(buf).context("Send PONG response")?;
    }

    Ok(())
}
