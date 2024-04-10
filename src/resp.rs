use anyhow::Context;

use crate::utils::{bytes2usize, split_by_clrf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RespValue {
    SimpleString(String),
    BulkString(Vec<u8>),
    Array(Vec<RespValue>),
    NullBulkString,
    Integer(i64),
    SimpleError(String),
}

impl RespValue {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            RespValue::SimpleString(val) => format!("+{}\r\n", val).into_bytes(),
            RespValue::BulkString(vec) => {
                let mut bytes = Vec::new();
                bytes.push(b'$');
                bytes.extend(vec.len().to_string().into_bytes());
                bytes.extend(b"\r\n");
                bytes.extend(vec.iter());
                bytes.extend(b"\r\n");
                bytes
            }
            RespValue::Array(values) => {
                let mut bytes = Vec::new();
                bytes.push(b'*');
                bytes.extend(values.len().to_string().as_bytes());
                bytes.extend(b"\r\n");
                values.iter().for_each(|val| bytes.extend(val.to_bytes()));
                bytes
            }
            RespValue::NullBulkString => b"$-1\r\n".into(),
            RespValue::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            RespValue::SimpleError(s) => format!("-{}\r\n", s).into_bytes(),
        }
    }
}

pub(crate) fn decode(bytes: &[u8]) -> anyhow::Result<(RespValue, &[u8])> {
    match bytes
        .iter()
        .nth(0)
        .context("RESP-encoded must not be empty")?
    {
        b'+' => {
            // simple string
            let (data, bytes) =
                split_by_clrf(&bytes[1..]).context("Simple string terminates with CLRF")?;
            match String::from_utf8(data) {
                Ok(value) => Ok((RespValue::SimpleString(value), bytes)),
                Err(_) => Err(anyhow::anyhow!("Expect simple string to be UTF-8 encoded")),
            }
        }
        b'$' => {
            // bulk string
            let (length_bytes, bytes) = split_by_clrf(&bytes[1..])
                .context("Bulk string length bytes and data are delimited by CLRF")?;
            let length = bytes2usize(&length_bytes)?;
            let (data, bytes) =
                split_by_clrf(bytes).context("Bulk string data terminates with CLRF")?;
            if data.len() != length {
                return Err(anyhow::anyhow!("Inconsistent length"));
            }
            return Ok((RespValue::BulkString(data.into()), bytes));
        }
        b'*' => {
            // list
            let (vec_length_bytes, bytes) =
                split_by_clrf(&bytes[1..]).context("List length and data are delimited by CLRF")?;
            let vec_length = bytes2usize(&vec_length_bytes)?;
            let mut values = Vec::with_capacity(vec_length);
            let mut bytes = bytes;
            for _ in 0..vec_length {
                let (value, _bytes) = decode(bytes)?;
                values.push(value);
                bytes = _bytes;
            }
            return Ok((RespValue::Array(values), bytes));
        }
        b':' => {
            // integer
            let (value, bytes) =
                split_by_clrf(&bytes[1..]).context("Extract integer by CLRF terminal")?;
            let value = std::str::from_utf8(&value)
                .context("UTF-8 decode bytes for integer string")?
                .parse::<i64>()
                .context("Parse integer string")?;
            return Ok((RespValue::Integer(value), bytes));
        }
        _ => Err(anyhow::anyhow!("Invalid RESP-encoded value: {:?}", bytes)),
    }
}

pub(crate) fn decode_array_of_bulkstrings(bytes: &[u8]) -> anyhow::Result<(Vec<Vec<u8>>, &[u8])> {
    let (cmd, remaining) = decode(bytes)?;
    let values = match cmd {
        RespValue::Array(values) => values,
        o => return Err(anyhow::anyhow!("Command must be an array, found {:?}", o)),
    };

    let mut arr = Vec::with_capacity(values.len());
    for val in values {
        match val {
            RespValue::BulkString(x) => {
                arr.push(x);
            }
            o => {
                return Err(anyhow::anyhow!(
                    "Command elements must be bulk strings, found {:?}",
                    o
                ));
            }
        }
    }

    Ok((arr, remaining))
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_decode_simple_string() {
        let (actual, _) = decode(b"+OK\r\n").unwrap();
        let expected = RespValue::SimpleString(String::from("OK"));
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_bulk_string() {
        let (actual, _) = decode(b"$5\r\nhello\r\n").unwrap();
        let expected = RespValue::BulkString(b"hello".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_array() {
        let (actual, _) = decode(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n").unwrap();
        let expected = RespValue::Array(vec![
            RespValue::BulkString(b"hello".into()),
            RespValue::BulkString(b"world".into()),
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_encode_array() {
        let actual = RespValue::Array(vec![RespValue::BulkString(b"PING".into())]).to_bytes();
        assert_eq!(actual, b"*1\r\n$4\r\nPING\r\n");
    }
}
