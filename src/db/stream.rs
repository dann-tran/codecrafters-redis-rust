use std::{
    cmp::Ordering,
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::trie::Trie;

#[derive(Debug, Clone)]
pub(crate) struct ReqStreamEntryID {
    pub(crate) millis: Vec<u8>,
    pub(crate) seq_num: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamEntryID {
    pub(crate) millis: Vec<u8>,
    pub(crate) seq_num: Vec<u8>,
}

impl StreamEntryID {
    pub(crate) fn as_bytes(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(self.millis.len() + 1 + self.seq_num.len());
        res.extend(&self.millis);
        res.push(b'-');
        res.extend(&self.seq_num);
        res
    }
}

fn cmp_vecu8(left: &Vec<u8>, right: &Vec<u8>) -> std::cmp::Ordering {
    match left.len().cmp(&right.len()) {
        Ordering::Equal => left.cmp(right),
        o => o,
    }
}

fn increment_vecu8(x: &mut Vec<u8>) {
    for c in x.iter_mut().rev() {
        if *c == b'9' {
            *c = b'0';
        } else {
            *c += 1;
            break;
        }
    }
}

fn make_stream_entry_id(
    req: Option<ReqStreamEntryID>,
    last_entry: &StreamEntryID,
) -> anyhow::Result<StreamEntryID> {
    match req {
        Some(req) => {
            if req.millis == vec![b'0'] {
                if let Some(seq_num) = &req.seq_num {
                    if *seq_num == vec![b'0'] {
                        return Err(anyhow::anyhow!(
                            "ERR The ID specified in XADD must be greater than 0-0"
                        ));
                    }
                }
            }

            let seq_num = match cmp_vecu8(&req.millis, &last_entry.millis) {
                Ordering::Less => {
                    return Err(anyhow::anyhow!(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    ));
                }
                Ordering::Equal => match req.seq_num {
                    Some(seq_num) => match cmp_vecu8(&seq_num, &last_entry.seq_num) {
                        Ordering::Greater => seq_num,
                        _ => {
                            return Err(anyhow::anyhow!(
                                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                                ));
                        }
                    },
                    None => {
                        let mut seq_num = last_entry.seq_num.clone();
                        increment_vecu8(&mut seq_num);
                        seq_num
                    }
                },
                Ordering::Greater => match req.seq_num {
                    Some(seq_num) => seq_num,
                    None => vec![b'0'],
                },
            };

            Ok(StreamEntryID {
                millis: req.millis,
                seq_num,
            })
        }
        None => {
            if last_entry.millis == vec![b'0'] && last_entry.seq_num == vec![b'0'] {
                Ok(StreamEntryID {
                    millis: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_millis()
                        .to_string()
                        .as_bytes()
                        .to_vec(),
                    seq_num: vec![b'0'],
                })
            } else {
                let mut seq_num = last_entry.seq_num.clone();
                increment_vecu8(&mut seq_num);
                Ok(StreamEntryID {
                    millis: last_entry.millis.clone(),
                    seq_num,
                })
            }
        }
    }
}

pub(crate) struct RedisStream {
    root: Trie<Trie<HashMap<Vec<u8>, Vec<u8>>>>,
    last_entry: StreamEntryID,
}

impl RedisStream {
    pub(crate) fn new() -> Self {
        Self {
            root: Trie::new(),
            last_entry: StreamEntryID {
                millis: vec![b'0'],
                seq_num: vec![b'0'],
            },
        }
    }

    pub(crate) fn insert(
        &mut self,
        entry_id: Option<ReqStreamEntryID>,
        data: HashMap<Vec<u8>, Vec<u8>>,
    ) -> anyhow::Result<StreamEntryID> {
        let entry_id = make_stream_entry_id(entry_id, &mut self.last_entry)?;

        if !self.root.contains_key(&entry_id.millis) {
            self.root.insert(&entry_id.millis, Trie::new());
        }

        let node = self.root.get_mut(&entry_id.millis).expect("Not None");
        node.insert(&entry_id.seq_num, data);
        self.last_entry = entry_id.clone();
        Ok(entry_id)
    }
}
