use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use bytes::Buf;

use crate::db::RedisDb;

static REDIS_MAGIC_STRING: &[u8; 5] = b"REDIS";

pub(crate) struct Rdb {
    // pub(crate) ver_num: u32,
    // pub(crate) aux: HashMap<Vec<u8>, Vec<u8>>,
    pub(crate) databases: HashMap<u32, RedisDb>,
}

const OPCODE_EOF: u8 = 0xFF;
const OPCODE_SELECTDB: u8 = 0xFE;
const OPCODE_EXPIRETIME: u8 = 0xFD;
const OPCODE_EXPIRETIMEMS: u8 = 0xFC;
const OPCODE_RESIZEDB: u8 = 0xFB;
const OPCODE_AUX: u8 = 0xFA;

#[derive(Debug)]
enum RdbLength {
    Length(u32),
    Format(u8),
}

fn extract_rdb_objlength(bytes: &[u8]) -> anyhow::Result<(RdbLength, &[u8])> {
    let (b0, mut remaining) = bytes
        .split_first()
        .context("Extract first byte of RDB length")?;

    let length = match b0 >> 6 {
        0 => RdbLength::Length(b0.to_le().into()),
        1 => {
            let (b1, _remaining) = remaining
                .split_first()
                .context("Extract second byte following 01 leading bits")?;
            remaining = _remaining;
            RdbLength::Length(((b0 % (1 << 6)) << 8 + b1).into())
        }
        2 => {
            let (b1, _remaining) = remaining
                .split_first()
                .context("Extract second byte following 10 leading bits")?;
            remaining = _remaining;
            RdbLength::Length(b1.to_le().into())
        }
        3 => RdbLength::Format(b0 % (1 << 6)),
        o => panic!("Examining only 2 bits, yet found: {}", o),
    };

    Ok((length, remaining))
}

fn extract_rdb_length(bytes: &[u8]) -> anyhow::Result<(u32, &[u8])> {
    let (objlength, remaining) = extract_rdb_objlength(bytes)?;
    match objlength {
        RdbLength::Length(l) => Ok((l, remaining)),
        o => Err(anyhow::anyhow!("Unexpected number encoding: {:?}", o)),
    }
}

fn extract_rdb_string(bytes: &[u8]) -> anyhow::Result<(Vec<u8>, &[u8])> {
    let (length, mut remaining) =
        extract_rdb_objlength(bytes).context("Extract length encoding")?;
    let v = match length {
        RdbLength::Length(l) => {
            let (val, _remaining) = remaining.split_at(l as usize);
            remaining = _remaining;
            val.to_vec()
        }
        RdbLength::Format(v) => match v {
            0..=2 => {
                let bytes_num = 1 << v;
                let (val, _remaining) = remaining.split_at(bytes_num);
                remaining = _remaining;
                val.to_vec()
            }
            3 => {
                let (clen, _remaining) =
                    extract_rdb_objlength(remaining).context("Extract compressed length")?;
                let clen = match clen {
                    RdbLength::Length(l) => l,
                    l => return Err(anyhow::anyhow!("Unexpected compressed length: {:?}", l)),
                };

                let (uclen, _remaining) =
                    extract_rdb_objlength(_remaining).context("Extract uncompressed length")?;
                let uclen = match uclen {
                    RdbLength::Length(l) => l,
                    l => return Err(anyhow::anyhow!("Unexpected uncompressed length: {:?}", l)),
                };

                let (compressed, _remaining) = _remaining.split_at(clen as usize);
                remaining = _remaining;

                lzf::decompress(&compressed, uclen as usize)
                    .ok()
                    .context("Decompress LZF")?
            }
            o => {
                return Err(anyhow::anyhow!(
                    "Unexpected special format for string: {}",
                    o
                ));
            }
        },
    };

    Ok((v, remaining))
}

pub fn parse_rdb(bytes: &[u8]) -> anyhow::Result<Rdb> {
    if !bytes.starts_with(REDIS_MAGIC_STRING) {
        return Err(anyhow::anyhow!(
            "Expect REDIS string, found: {:?}",
            &bytes[..10]
        ));
    }

    let bytes = &bytes[REDIS_MAGIC_STRING.len()..];
    let (mut ver_num, mut remaining) = bytes.split_at(4);
    let ver_num = ver_num.get_u32_le();
    let _ = ver_num;

    let mut aux = HashMap::new();
    let mut databases = HashMap::new();

    while let Some((opcode, mut _remaining)) = remaining.split_first() {
        match *opcode {
            OPCODE_EOF => {
                eprintln!("EOF");
                break;
            }
            OPCODE_SELECTDB => {
                eprintln!("SELECTDB");
                let (db_num, __remaining) =
                    extract_rdb_length(_remaining).context("Extract db number")?;
                _remaining = __remaining;

                let (mut nonexpire_table, mut expire_table) =
                    if _remaining.starts_with(&[OPCODE_RESIZEDB]) {
                        let (table_size, __remaining) = extract_rdb_length(&_remaining[1..])
                            .context("Extract hash table size")?;
                        let (expire_table_size, __remaining) = extract_rdb_length(__remaining)
                            .context("Extract expire hash table size")?;
                        _remaining = __remaining;

                        (
                            HashMap::with_capacity(table_size as usize),
                            HashMap::with_capacity(expire_table_size as usize),
                        )
                    } else {
                        (HashMap::new(), HashMap::new())
                    };

                eprintln!("Parsing KVs for db number: {}", db_num);
                while let Some((b, mut __remaining)) = _remaining.split_first() {
                    let since_unix_epoch = match *b {
                        OPCODE_EXPIRETIME => {
                            let secs = __remaining.get_u32_le();
                            _remaining = __remaining;
                            Some(Duration::from_secs(secs.into()))
                        }
                        OPCODE_EXPIRETIMEMS => {
                            let millis = __remaining.get_u64_le();
                            _remaining = __remaining;
                            Some(Duration::from_millis(millis))
                        }
                        OPCODE_SELECTDB | OPCODE_EOF => {
                            break;
                        }
                        _ => None,
                    };

                    let (value_type, __remaining) =
                        _remaining.split_first().context("Extract value type")?;
                    if *value_type != 0 {
                        return Err(anyhow::anyhow!(
                            "RDB parser currently only support string values, found: {}",
                            value_type
                        ));
                    }

                    let (key, __remaining) =
                        extract_rdb_string(__remaining).context("Extract key")?;
                    let (value, __remaining) =
                        extract_rdb_string(__remaining).context("Extract value")?;

                    _remaining = __remaining;

                    match since_unix_epoch {
                        Some(dur) => {
                            let expiry = UNIX_EPOCH + dur;
                            if expiry > SystemTime::now() {
                                expire_table.insert(key, (value, expiry));
                            }
                        }
                        None => {
                            nonexpire_table.insert(key, value);
                        }
                    }
                }

                databases.insert(
                    db_num,
                    RedisDb {
                        nonexpire_table,
                        expire_table,
                        streams: HashMap::new(),
                    },
                );
            }
            OPCODE_AUX => {
                eprintln!("AUX");
                let (key, __remaining) = extract_rdb_string(_remaining)?;
                let (val, __remaining) = extract_rdb_string(__remaining)?;
                aux.insert(key, val);
                _remaining = __remaining;
            }
            o => {
                return Err(anyhow::anyhow!(
                    "Unexpected opcode while parsing RDB: {:?}",
                    o
                ));
            }
        }
        remaining = _remaining;
    }

    Ok(Rdb {
        // ver_num,
        // aux,
        databases,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_rdb() {
        let bytes = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").expect("Valid HEX");
        let rdb = parse_rdb(&bytes).expect("Valid empty RDB");
        assert!(rdb.databases.is_empty());
    }

    #[test]
    fn test_parse_1kv_rdb() {
        let bytes = hex::decode("524544495330303131fa0972656469732d76657205372e322e34fa0a72656469732d62697473c040fa056374696d65c247561266fa08757365642d6d656dc2e0461100fa08616f662d62617365c000fe00fb010000056d796b6579056d7976616cff59eeb542a15e83f7").expect("Valid HEX");
        let rdb = parse_rdb(&bytes).expect("Valid RDB with 1 KV pair");
        eprintln!("Keys: {:?}", rdb.databases.keys());
        assert_eq!(rdb.databases.len(), 1);
    }
}
