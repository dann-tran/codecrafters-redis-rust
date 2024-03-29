use crate::{resp::RespValue, ToBytes};

pub(crate) enum InfoArg {
    Replication,
}

pub(crate) enum Command {
    Ping,
    Echo(Vec<u8>),
    Set {
        key: String,
        value: String,
        px: Option<usize>,
    },
    Get(String),
    Info(Option<InfoArg>),
    ReplConf {
        listening_port: Option<u16>,
        capa: Vec<String>,
    },
}

impl ToBytes for Command {
    fn to_bytes(&self) -> Vec<u8> {
        let args = match self {
            Command::Ping => vec![b"PING".to_vec()],
            Command::Echo(arg) => vec![b"ECHO".to_vec(), arg.clone()],
            Command::Set { key, value, px } => {
                let mut vec = vec![
                    b"SET".to_vec(),
                    key.as_bytes().to_vec(),
                    value.as_bytes().to_vec(),
                ];
                match px {
                    Some(val) => {
                        vec.push(val.to_string().as_bytes().to_vec());
                    }
                    _ => {}
                }
                vec
            }
            Command::Get(key) => vec![b"GET".to_vec(), key.as_bytes().to_vec()],
            Command::Info(info_arg) => {
                let mut vec = vec![b"INFO".to_vec()];
                match info_arg {
                    Some(_) => {
                        vec.push(b"replication".to_vec());
                    }
                    None => {}
                }
                vec
            }
            Command::ReplConf {
                listening_port,
                capa,
            } => {
                let mut vec = vec![b"REPLCONF".to_vec()];
                match listening_port {
                    Some(port) => {
                        let mut port_arg = vec![
                            b"listening-port".to_vec(),
                            port.to_string().as_bytes().to_vec(),
                        ];
                        vec.append(&mut port_arg);
                    }
                    None => {}
                };
                capa.iter().for_each(|c| {
                    let mut capa_arg = vec![b"capa".to_vec(), c.as_bytes().to_vec()];
                    vec.append(&mut capa_arg);
                });
                vec
            }
        };
        let args = args
            .iter()
            .map(|v| RespValue::BulkString(v.clone()))
            .collect::<Vec<RespValue>>();
        RespValue::Array(args).to_bytes()
    }
}
