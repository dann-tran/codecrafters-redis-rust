use std::{
    io::{Read, Write},
    net::TcpListener,
};

use anyhow::Context;

fn main() -> anyhow::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        let mut stream = stream.context("Accept TCP connection from client")?;
        let expected_req = b"*1\r\n$4\r\nping\r\n";
        let mut remaining_n: usize = 0;
        loop {
            let mut req = [0u8; 1024];
            let buf = b"+PONG\r\n";
            let n = stream.read(&mut req).context("Received bytes")?;
            remaining_n += n;
            if remaining_n >= expected_req.len() {
                stream.write_all(buf).context("Send PONG response")?;
                remaining_n -= expected_req.len();
            }
        }
    }

    Ok(())
}
