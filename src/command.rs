use crate::resp::{decode_array_of_bulkstrings, RespValue};

#[derive(Debug)]
pub enum InfoArg {
    Replication,
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
    ReplConf {
        listening_port: Option<u16>,
        capa: Vec<String>,
    },
    PSync {
        repl_id: Option<[char; 40]>,
        repl_offset: Option<usize>,
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
        };
        let args = args
            .iter()
            .map(|v| RespValue::BulkString(v.clone()))
            .collect::<Vec<RespValue>>();
        RespValue::Array(args).to_bytes()
    }

    pub fn from_bytes(bytes: &[u8]) -> (Self, &[u8]) {
        let (args, remaining_bytes) = decode_array_of_bulkstrings(bytes);

        let (verb, mut remaining) = args.split_first().expect("Command verb must be present");
        let verb = verb.to_ascii_lowercase();

        let cmd = match &verb[..] {
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
                        _ => panic!("Invalid SET arguments"),
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
                            _ => panic!("Invalid info argument {:?}", v),
                        };
                        (Some(v), _remaining)
                    }
                    None => (None, remaining),
                };
                remaining = _remaining;
                Command::Info(info_arg)
            }
            b"replconf" => {
                let mut listening_port = None;
                let mut capa = Vec::new();

                loop {
                    let (arg, mut _remaining) = match remaining.split_first() {
                        Some((arg, _remaining)) => (arg, _remaining),
                        None => break,
                    };
                    match &arg[..] {
                        b"listening-port" => {
                            let (port, __remaining) = _remaining
                                .split_first()
                                .expect("Listening port must be present");
                            let port = std::str::from_utf8(port)
                                .expect("Valid UTF-8 string for listening port")
                                .parse::<u16>()
                                .expect("Valid u16 port number");
                            listening_port = Some(port);
                            _remaining = __remaining;
                        }
                        b"capa" => {
                            let (_capa, __remaining) = _remaining
                                .split_first()
                                .expect("Capability argument must be present");
                            let _capa = std::str::from_utf8(_capa)
                                .expect("Valid UTF-8 string for capability argument")
                                .to_string();
                            capa.push(_capa);
                            _remaining = __remaining;
                        }
                        c => {
                            panic!("Unknown REPLCONF argument: {:?}", c);
                        }
                    };
                    remaining = _remaining;
                }

                Command::ReplConf {
                    listening_port,
                    capa,
                }
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
            _ => panic!("Unknown verb: {:?}", verb),
        };

        if !remaining.is_empty() {
            panic!("Unexpected arguments")
        }

        (cmd, remaining_bytes)
    }
}
