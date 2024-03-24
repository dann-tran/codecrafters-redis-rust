use anyhow::Context;
use redis_starter_rust::command::{respond, Command};
use redis_starter_rust::resp::{decode, RespValue};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

async fn handler(socket: &mut TcpStream) {
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
                            "echo" => Command::Echo(
                                values
                                    .next()
                                    .expect("An echo value must be present")
                                    .clone(),
                            ),
                            _ => {
                                panic!("Unrecognised command")
                            }
                        }
                    }
                    _ => {
                        panic!("Invalid")
                    }
                };
                respond(socket, &cmd).await;
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

    loop {
        let (mut socket, _) = listener
            .accept()
            .await
            .context("Accept connection")
            .unwrap();
        tokio::spawn(async move { handler(&mut socket).await });
    }
}
