use anyhow::Context;

use crate::resp::{decode_array_of_bulkstrings, RespValue};

#[derive(Debug)]
pub enum InfoArg {
    Replication,
}

#[derive(Debug)]
pub enum ReplConfArg {
    ListeningPort(u16),
    Capa(Vec<String>),
    GetAck,
    Ack(usize),
}

#[derive(Debug)]
pub enum Command {
    Ping,
    Echo(Vec<u8>),
    Set {
        key: String,
        value: String,
        px: Option<usize>,
    },
    Get(String),
    Info(Option<InfoArg>),
    ReplConf(ReplConfArg),
    PSync {
        repl_id: Option<[char; 40]>,
        repl_offset: Option<usize>,
    },
    Wait {
        repl_num: usize,
        sec_num: u64,
    },
}

impl Command {
    pub fn to_bytes(&self) -> Vec<u8> {
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
                        let mut px_args = vec![b"px".to_vec(), val.to_string().as_bytes().to_vec()];
                        vec.append(&mut px_args);
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
            Command::ReplConf(arg) => match arg {
                ReplConfArg::ListeningPort(port) => vec![
                    b"REPLCONF".to_vec(),
                    b"listening-port".to_vec(),
                    port.to_string().as_bytes().to_vec(),
                ],
                ReplConfArg::Capa(capas) => {
                    capas
                        .iter()
                        .fold(vec![b"REPLCONF".to_vec()], |mut acc, capa| {
                            acc.push(b"capa".to_vec());
                            acc.push(capa.as_bytes().to_vec());
                            acc
                        })
                }

                ReplConfArg::GetAck => todo!(),
                ReplConfArg::Ack(offset) => vec![
                    b"REPLCONF".to_vec(),
                    b"ACK".to_vec(),
                    offset.to_string().as_bytes().to_vec(),
                ],
            },
            Command::PSync {
                repl_id,
                repl_offset,
            } => {
                let mut vec = Vec::with_capacity(3);
                vec.push(b"PSYNC".to_vec());

                let repl_id = match repl_id {
                    Some(id) => id.iter().collect::<String>().as_bytes().to_vec(),
                    None => b"?".to_vec(),
                };
                vec.push(repl_id);

                let repl_offset = match repl_offset {
                    Some(offset) => offset.to_string().as_bytes().to_vec(),
                    None => b"-1".to_vec(),
                };
                vec.push(repl_offset);

                vec
            }
            Command::Wait {
                repl_num: _,
                sec_num: _,
            } => todo!(),
        };
        let args = args
            .iter()
            .map(|v| RespValue::BulkString(v.clone()))
            .collect::<Vec<RespValue>>();
        RespValue::Array(args).to_bytes()
    }

    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<(Self, &[u8])> {
        let (args, remaining_bytes) = decode_array_of_bulkstrings(bytes)?;

        let (verb, mut remaining) = args.split_first().expect("Command verb must be present");

        let cmd = match &verb.to_ascii_lowercase()[..] {
            b"ping" => Command::Ping,
            b"echo" => {
                let (val, _remaning) = remaining.split_first().expect("ECHO argument");
                remaining = _remaning;
                Command::Echo(val.clone())
            }
            b"get" => {
                let (val, _remaining) = remaining.split_first().expect("GET key");
                remaining = _remaining;
                Command::Get(String::from_utf8(val.clone()).expect("Key must be UTF-8"))
            }
            b"set" => {
                let (key, _remaining) = remaining.split_first().expect("SET key");
                let (value, _remaining) = _remaining.split_first().expect("SET value");

                let (is_px_present, _remaining) = match _remaining.split_first() {
                    Some((px_key, __remaining)) => match &px_key.to_ascii_lowercase()[..] {
                        b"px" => (true, __remaining),
                        arg => return Err(anyhow::anyhow!("Invalid SET argument: {:?}", arg)),
                    },
                    None => (false, _remaining),
                };
                let (px, _remaining) = match is_px_present {
                    true => {
                        let (px, __remaining) = _remaining.split_first().expect("expiry argument");
                        let px = String::from_utf8(px.clone()).expect("Valid string");
                        let px = px.parse::<usize>().expect("Valid number");
                        (Some(px), __remaining)
                    }
                    false => (None, _remaining),
                };

                remaining = _remaining;

                Command::Set {
                    key: String::from_utf8(key.clone()).expect("Valid UTF-8 key"),
                    value: String::from_utf8(value.clone()).expect("Valid UTF-8 value"),
                    px,
                }
            }
            b"info" => {
                let (info_arg, _remaining) = match remaining.split_first() {
                    Some((v, _remaining)) => {
                        let v = v.to_ascii_lowercase();
                        let v = match &v[..] {
                            b"replication" => InfoArg::Replication,
                            _ => return Err(anyhow::anyhow!("Invalid info argument {:?}", v)),
                        };
                        (Some(v), _remaining)
                    }
                    None => (None, remaining),
                };
                remaining = _remaining;
                Command::Info(info_arg)
            }
            b"replconf" => {
                let (arg, mut _remaining) =
                    remaining.split_first().expect("First REPLCONF argument");

                let replconf_arg = match &arg.to_ascii_lowercase()[..] {
                    b"listening-port" => {
                        let (port, __remaining) = _remaining
                            .split_first()
                            .expect("Listening port must be present");
                        let port = std::str::from_utf8(port)
                            .expect("Valid UTF-8 string for listening port")
                            .parse::<u16>()
                            .expect("Valid u16 port number");
                        _remaining = __remaining;
                        ReplConfArg::ListeningPort(port)
                    }
                    b"capa" => {
                        let mut capas = Vec::new();
                        let (capa, mut __remaining) = _remaining
                            .split_first()
                            .expect("Capability argument must be present");
                        let _capa = std::str::from_utf8(capa)
                            .expect("Valid UTF-8 string for capability argument")
                            .to_string();
                        capas.push(_capa);
                        _remaining = __remaining;

                        loop {
                            let (arg, __remaining) = match _remaining.split_first() {
                                Some(x) => x,
                                None => {
                                    break;
                                }
                            };
                            assert_eq!(&arg[..], b"capa");
                            let (capa, mut __remaining) = __remaining
                                .split_first()
                                .expect("Capability argument must be present");
                            let _capa = std::str::from_utf8(capa)
                                .expect("Valid UTF-8 string for capability argument")
                                .to_string();
                            capas.push(_capa);
                            _remaining = __remaining;
                        }
                        ReplConfArg::Capa(capas)
                    }
                    b"getack" => {
                        let (arg, __remaining) =
                            _remaining.split_first().expect("REPLCONF GETACK argument");
                        assert_eq!(&arg[..], b"*");
                        _remaining = __remaining;
                        ReplConfArg::GetAck
                    }
                    a => {
                        return Err(anyhow::anyhow!("Unrecognised REPLCONF argument: {:?}", a));
                    }
                };
                remaining = _remaining;

                Command::ReplConf(replconf_arg)
            }
            b"psync" => {
                let (repl_id, _remaining) = remaining
                    .split_first()
                    .expect("Replication ID must be present");
                let repl_id = match &repl_id[..] {
                    b"?" => None,
                    bytes => Some(
                        std::str::from_utf8(&bytes)
                            .expect("Replication ID must be valid UTF-8 chars")
                            .chars()
                            .collect::<Vec<char>>()
                            .try_into()
                            .expect("Replication ID must be 40 characters long"),
                    ),
                };

                let (repl_offset, _remaining) = _remaining
                    .split_first()
                    .expect("Replication offset must be present");
                let repl_offset = match &repl_offset[..] {
                    b"-1" => None,
                    bytes => Some(
                        std::str::from_utf8(&bytes)
                            .expect("Replication offset must be valid UTF-8 string")
                            .parse::<usize>()
                            .expect("Replication offset must be a valid number"),
                    ),
                };

                remaining = _remaining;

                Command::PSync {
                    repl_id,
                    repl_offset,
                }
            }
            b"wait" => {
                let (repl_num, _remaining) = remaining
                    .split_first()
                    .context("Retrieve number of replicas")?;
                let repl_num = std::str::from_utf8(repl_num)
                    .context("Parse replica number using UTF-8 encoding")?
                    .parse::<usize>()
                    .context("Parse replica number from string")?;

                let (sec_num, _remaining) = _remaining
                    .split_first()
                    .context("Retrieve number of seconds")?;
                let sec_num = std::str::from_utf8(sec_num)
                    .context("Parse number of seconds using UTF-8 encoding")?
                    .parse::<u64>()
                    .context("Parse number of seconds from string")?;

                remaining = _remaining;

                Command::Wait { repl_num, sec_num }
            }
            v => return Err(anyhow::anyhow!("Unknown verb: {:?}", v)),
        };

        if !remaining.is_empty() {
            return Err(anyhow::anyhow!("Unexpected arguments: {:?}", remaining));
        }

        Ok((cmd, remaining_bytes))
    }
}
