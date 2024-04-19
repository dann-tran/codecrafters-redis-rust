use std::{collections::HashMap, time::Duration};

use anyhow::Context;

use crate::{
    db::stream::ReqStreamEntryID,
    resp::{decode_array_of_bulkstrings, RespValue},
};

#[derive(Debug, Clone)]
pub(crate) enum InfoArg {
    Replication,
}

#[derive(Debug, Clone)]
pub(crate) enum ReplConfArg {
    ListeningPort(u16),
    Capa(Vec<String>),
    GetAck,
    Ack(usize),
}

#[derive(Debug, Clone)]
pub(crate) enum ConfigArg {
    Get(String),
}

// TODO: remove Clone trait
#[derive(Debug, Clone)]
pub(crate) enum Command {
    Ping,
    Echo(Vec<u8>),
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        px: Option<Duration>,
    },
    Get(Vec<u8>),
    Info(Option<InfoArg>),
    ReplConf(ReplConfArg),
    PSync {
        repl_id: Option<[char; 40]>,
        repl_offset: Option<usize>,
    },
    Wait {
        repl_ack_num: usize,
        timeout_dur: Duration,
    },
    Config(ConfigArg),
    Keys,
    LookupType(Vec<u8>),
    XAdd {
        key: Vec<u8>,
        entry_id: Option<ReqStreamEntryID>,
        data: HashMap<Vec<u8>, Vec<u8>>,
    },
}

impl Command {
    pub fn to_bytes(&self) -> Vec<u8> {
        let args = match self {
            Command::Ping => vec![b"PING".to_vec()],
            Command::Echo(arg) => vec![b"ECHO".to_vec(), arg.clone()],
            Command::Set { key, value, px } => {
                let mut vec = vec![b"SET".to_vec(), key.clone(), value.clone()];
                match px {
                    Some(val) => {
                        let mut px_args = vec![
                            b"px".to_vec(),
                            val.as_millis().to_string().as_bytes().to_vec(),
                        ];
                        vec.append(&mut px_args);
                    }
                    _ => {}
                }
                vec
            }
            Command::Get(key) => vec![b"GET".to_vec(), key.clone()],
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
                ReplConfArg::GetAck => {
                    vec![b"REPLCONF".to_vec(), b"GETACK".to_vec(), b"*".to_vec()]
                }
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
            _ => todo!(),
        };
        let args = args
            .into_iter()
            .map(|v| RespValue::BulkString(v))
            .collect::<Vec<RespValue>>();
        RespValue::Array(args).to_bytes()
    }

    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<(Self, &[u8])> {
        let (args, remaining_bytes) = decode_array_of_bulkstrings(bytes)?;

        let (verb, mut remaining) = args.split_first().context("Extract command verb")?;

        let cmd = match &verb.to_ascii_lowercase()[..] {
            b"ping" => Command::Ping,
            b"echo" => {
                let (val, _remaning) = remaining.split_first().context("Extract ECHO argument")?;
                remaining = _remaning;
                Command::Echo(val.clone())
            }
            b"get" => {
                let (val, _remaining) = remaining.split_first().context("Extract GET key")?;
                remaining = _remaining;
                Command::Get(val.clone())
            }
            b"set" => {
                let (key, _remaining) = remaining.split_first().context("Extract SET key")?;
                let (value, _remaining) = _remaining.split_first().context("Extract SET value")?;

                let (is_px_present, _remaining) = match _remaining.split_first() {
                    Some((px_key, __remaining)) => match &px_key.to_ascii_lowercase()[..] {
                        b"px" => (true, __remaining),
                        arg => return Err(anyhow::anyhow!("Invalid SET argument: {:?}", arg)),
                    },
                    None => (false, _remaining),
                };
                let (px, _remaining) = match is_px_present {
                    true => {
                        let (px, __remaining) = _remaining
                            .split_first()
                            .context("Extract expiry argument")?;
                        let px = std::str::from_utf8(px)
                            .context("UTF-8 decode expiry string")?
                            .parse::<u64>()
                            .context("Parse expiry string to number")?;
                        (Some(px), __remaining)
                    }
                    false => (None, _remaining),
                };

                remaining = _remaining;

                Command::Set {
                    key: key.clone(),
                    value: value.clone(),
                    px: px.map(|millis| Duration::from_millis(millis)),
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
                let (arg, mut _remaining) = remaining
                    .split_first()
                    .context("Extract REPLCONF argument")?;

                let replconf_arg = match &arg.to_ascii_lowercase()[..] {
                    b"listening-port" => {
                        let (port, __remaining) = _remaining
                            .split_first()
                            .context("Extract listening port argument")?;
                        let port = std::str::from_utf8(port)
                            .context("UTF-8 decode listening port")?
                            .parse::<u16>()
                            .context("Parse listening port string to number")?;
                        _remaining = __remaining;
                        ReplConfArg::ListeningPort(port)
                    }
                    b"capa" => {
                        let mut capas = Vec::new();
                        let (capa, mut __remaining) = _remaining
                            .split_first()
                            .context("Extract capability argument")?;
                        let _capa = std::str::from_utf8(capa)
                            .context("UTF-8 decode capability argument")?
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
                                .context("Extract capability argument")?;
                            let _capa = std::str::from_utf8(capa)
                                .context("UTF-8 decode capability argument")?
                                .to_string();
                            capas.push(_capa);
                            _remaining = __remaining;
                        }
                        ReplConfArg::Capa(capas)
                    }
                    b"getack" => {
                        let (arg, __remaining) = _remaining
                            .split_first()
                            .context("Extract REPLCONF GETACK argument")?;
                        assert_eq!(&arg[..], b"*");
                        _remaining = __remaining;
                        ReplConfArg::GetAck
                    }
                    b"ack" => {
                        let (arg, __remaining) = _remaining
                            .split_first()
                            .context("Extract REPLCONF ACK offset")?;
                        let offset = std::str::from_utf8(arg)
                            .context("UTF-8 decode offset")?
                            .parse::<usize>()
                            .context("Parse offset from string to number")?;
                        _remaining = __remaining;
                        ReplConfArg::Ack(offset)
                    }
                    a => {
                        return Err(anyhow::anyhow!("Unrecognised REPLCONF argument: {:?}", a));
                    }
                };
                remaining = _remaining;

                Command::ReplConf(replconf_arg)
            }
            b"psync" => {
                let (repl_id, _remaining) =
                    remaining.split_first().context("Extract replication ID")?;
                let repl_id = match &repl_id[..] {
                    b"?" => None,
                    bytes => Some(
                        std::str::from_utf8(&bytes)
                            .context("UTF-8 decode replication ID")?
                            .chars()
                            .collect::<Vec<char>>()
                            .try_into()
                            .ok()
                            .context("Cast replication ID as 40-character array")?,
                    ),
                };

                let (repl_offset, _remaining) = _remaining
                    .split_first()
                    .context("Extract replication offset")?;
                let repl_offset = match &repl_offset[..] {
                    b"-1" => None,
                    bytes => Some(
                        std::str::from_utf8(&bytes)
                            .context("UTF-8 decode replication offset")?
                            .parse::<usize>()
                            .context("Parse replication offset from string to number")?,
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
                let timeout_dur = std::str::from_utf8(sec_num)
                    .context("Parse number of seconds using UTF-8 encoding")?
                    .parse::<u64>()
                    .context("Parse number of seconds from string")?;

                remaining = _remaining;

                Command::Wait {
                    repl_ack_num: repl_num,
                    timeout_dur: Duration::from_millis(timeout_dur),
                }
            }
            b"config" => {
                let (arg, _remaining) = remaining.split_first().context("Retrieve CONFIG arg")?;
                match &arg.to_ascii_lowercase()[..] {
                    b"get" => {
                        let (key, _remaining) =
                            _remaining.split_first().context("Extract CONFIG GET key")?;
                        let key = std::str::from_utf8(key).context("Parse CONFIG GET key")?;
                        remaining = _remaining;
                        Command::Config(ConfigArg::Get(key.to_string()))
                    }
                    s => panic!("Unrecognized CONFIG argument: {:?}", s),
                }
            }
            b"keys" => {
                let (_, _remaining) = remaining.split_first().context("Retrieve KEYS arg")?;
                remaining = _remaining;
                Command::Keys
            }
            b"type" => {
                let (val, _remaining) = remaining.split_first().context("Extract TYPE key")?;
                remaining = _remaining;
                Command::LookupType(val.clone())
            }
            b"xadd" => {
                let (key, _remaining) = remaining.split_first().context("Extract XADD key")?;
                let (entry_id, _remaining) =
                    _remaining.split_first().context("Extract entry ID")?;

                let entry_id = if *entry_id == vec![b'*'] {
                    None
                } else {
                    let (millis_bytes, seq_num_bytes) = entry_id.split_at(
                        entry_id
                            .iter()
                            .position(|&c| c == b'-')
                            .context("Find - in entry ID")?,
                    );

                    let mut millis = Vec::with_capacity(millis_bytes.len());
                    for &b in millis_bytes {
                        if b >= b'0' && b <= b'9' {
                            millis.push(b);
                        } else {
                            return Err(anyhow::anyhow!("Invalid entry ID: {:?}", entry_id));
                        }
                    }

                    let seq_num_bytes = &seq_num_bytes[1..];
                    let seq_num = if *seq_num_bytes == vec![b'*'] {
                        None
                    } else {
                        let mut seq_num = Vec::with_capacity(seq_num_bytes.len());
                        for &b in seq_num_bytes {
                            if b >= b'0' && b <= b'9' {
                                seq_num.push(b);
                            } else {
                                return Err(anyhow::anyhow!("Invalid entry ID: {:?}", entry_id));
                            }
                        }
                        Some(seq_num)
                    };

                    Some(ReqStreamEntryID {
                        millis: millis.to_vec(),
                        seq_num: seq_num.map(|x| x.to_vec()),
                    })
                };

                let data = HashMap::with_capacity(_remaining.len() / 2);
                let data = _remaining.chunks_exact(2).fold(data, |mut acc, chunk| {
                    acc.insert(chunk[0].clone(), chunk[1].clone());
                    acc
                });

                let (_, _remaining) = _remaining.split_at(data.len() * 2);
                remaining = _remaining;

                Command::XAdd {
                    key: key.clone(),
                    entry_id,
                    data,
                }
            }
            v => return Err(anyhow::anyhow!("Unknown verb: {:?}", v)),
        };

        if !remaining.is_empty() {
            return Err(anyhow::anyhow!("Unexpected arguments: {:?}", remaining));
        }

        Ok((cmd, remaining_bytes))
    }
}
