use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;

pub fn split_by_clrf(bytes: &[u8]) -> Option<(Vec<u8>, &[u8])> {
    let mut split = bytes.splitn(2, |&b| b == b'\r');
    let data = split.next()?;
    let remaining = split.next()?;

    if !remaining.starts_with(b"\n") {
        return None;
    }

    return Some((data.into(), &remaining[1..]));
}

pub fn bytes2usize(bytes: &[u8]) -> anyhow::Result<usize> {
    std::str::from_utf8(bytes)
        .context("Decode UTF-8")?
        .parse::<usize>()
        .context("Parse from string to usize")
}

pub(crate) fn get_current_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}
