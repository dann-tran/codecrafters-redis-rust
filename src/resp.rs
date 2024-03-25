#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    SimpleString(String),
    BulkString(Vec<u8>),
    Array(Vec<RespValue>),
    NullBulkString,
}

pub fn serialize(resp: &RespValue) -> Vec<u8> {
    match resp {
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
            values.iter().for_each(|val| bytes.extend(serialize(val)));
            bytes
        }
        RespValue::NullBulkString => b"$-1\r\n".into(),
    }
}

fn split_by_clrf(bytes: &[u8]) -> (Vec<u8>, &[u8]) {
    let data = bytes
        .iter()
        .take_while(|&&b| b != b'\r')
        .map(|&b| b)
        .collect::<Vec<u8>>();
    eprintln!("{}", std::str::from_utf8(&data).unwrap());
    if bytes[data.len() + 1] != b'\n' {
        panic!("Invalid clrf delimiter")
    }
    let bytes = &bytes[data.len() + 2..];
    return (data, bytes);
}

fn bytes2usize(bytes: &[u8]) -> usize {
    std::str::from_utf8(bytes)
        .expect("Valid UTF-8 string from bytes")
        .parse::<usize>()
        .expect("Valid number")
}

pub fn decode(bytes: &[u8]) -> (RespValue, &[u8]) {
    match bytes.iter().nth(0).expect("RESP-encoded must not be empty") {
        b'+' => {
            // simple string
            let (data, bytes) = split_by_clrf(&bytes[1..]);
            return (
                RespValue::SimpleString(String::from_utf8(data).expect("Invalid UTF-8 string")),
                bytes,
            );
        }
        b'$' => {
            // bulk string
            let (length_bytes, bytes) = split_by_clrf(&bytes[1..]);
            let length = bytes2usize(&length_bytes);
            let (data, bytes) = split_by_clrf(bytes);
            if data.len() != length {
                panic!("Inconsistent length")
            }
            return (RespValue::BulkString(data.into()), bytes);
        }
        b'*' => {
            let (vec_length_bytes, bytes) = split_by_clrf(&bytes[1..]);
            let vec_length = bytes2usize(&vec_length_bytes);
            let mut values = Vec::with_capacity(vec_length);
            let mut bytes = bytes;
            for _ in 0..vec_length {
                let (value, _bytes) = decode(bytes);
                values.push(value);
                bytes = _bytes;
            }
            return (RespValue::Array(values), bytes);
        }
        _ => {
            panic!("Invalid RESP-encoded value: {:?}", bytes)
        }
    }
}

// TODO: implement decode command
// pub fn decode_cmd(bytes: &[u8]) -> Vec<Vec<u8>>

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_decode_simple_string() {
        let (actual, _) = decode(b"+OK\r\n");
        let expected = RespValue::SimpleString(String::from("OK"));
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_bulk_string() {
        let (actual, _) = decode(b"$5\r\nhello\r\n");
        let expected = RespValue::BulkString(b"hello".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_array() {
        let (actual, _) = decode(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
        let expected = RespValue::Array(vec![
            RespValue::BulkString(b"hello".into()),
            RespValue::BulkString(b"world".into()),
        ]);
        assert_eq!(actual, expected);
    }
}
