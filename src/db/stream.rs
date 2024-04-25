use std::{
    cmp::Ordering,
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use super::trie::Trie;

#[derive(Debug, Clone)]
pub(crate) struct ReqStreamEntryID {
    pub(crate) millis: u64,
    pub(crate) seq_num: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamEntryID {
    pub(crate) millis: u64,
    pub(crate) seq_num: u64,
}

impl StreamEntryID {
    pub(crate) fn as_bytes(&self) -> Vec<u8> {
        let mut millis = self.millis.to_string().as_bytes().to_vec();
        let mut seq_num = self.seq_num.to_string().as_bytes().to_vec();
        let mut res = Vec::with_capacity(millis.len() + 1 + seq_num.len());
        res.append(&mut millis);
        res.push(b'-');
        res.append(&mut seq_num);
        res
    }
}

fn make_stream_entry_id(
    req: Option<ReqStreamEntryID>,
    last_entry: &StreamEntryID,
) -> anyhow::Result<StreamEntryID> {
    match req {
        Some(req) => {
            if req.millis == 0 {
                if let Some(seq_num) = &req.seq_num {
                    if *seq_num == 0 {
                        return Err(anyhow::anyhow!(
                            "ERR The ID specified in XADD must be greater than 0-0"
                        ));
                    }
                }
            }

            let seq_num = match req.millis.cmp(&last_entry.millis) {
                Ordering::Less => {
                    return Err(anyhow::anyhow!(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    ));
                }
                Ordering::Equal => match req.seq_num {
                    Some(seq_num) => match seq_num.cmp(&last_entry.seq_num) {
                        Ordering::Greater => seq_num,
                        _ => {
                            return Err(anyhow::anyhow!(
                                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                                ));
                        }
                    },
                    None => last_entry.seq_num + 1,
                },
                Ordering::Greater => match req.seq_num {
                    Some(seq_num) => seq_num,
                    None => 0,
                },
            };

            Ok(StreamEntryID {
                millis: req.millis,
                seq_num,
            })
        }
        None => {
            if last_entry.millis == 0 && last_entry.seq_num == 0 {
                Ok(StreamEntryID {
                    millis: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_millis() as u64,
                    seq_num: 0,
                })
            } else {
                Ok(StreamEntryID {
                    millis: last_entry.millis,
                    seq_num: last_entry.seq_num + 1,
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
                millis: 0,
                seq_num: 0,
            },
        }
    }

    pub(crate) fn insert(
        &mut self,
        entry_id: Option<ReqStreamEntryID>,
        data: HashMap<Vec<u8>, Vec<u8>>,
    ) -> anyhow::Result<StreamEntryID> {
        let entry_id = make_stream_entry_id(entry_id, &mut self.last_entry)?;

        if !self.root.contains_key(entry_id.millis) {
            self.root.insert(entry_id.millis, Trie::new());
        }

        let node = self.root.get_mut(entry_id.millis).expect("Not None");
        node.insert(entry_id.seq_num, data);
        self.last_entry = entry_id.clone();
        Ok(entry_id)
    }

    pub(crate) fn xrange(
        &self,
        start: StreamEntryID,
        end: StreamEntryID,
    ) -> Vec<(Vec<u8>, Vec<Vec<u8>>)> {
        self.root
            .get_range_incl(start.millis, end.millis)
            .into_iter()
            .flat_map(|(millis, trie)| {
                let start_seq_num = if millis == end.millis {
                    u64::MIN
                } else {
                    start.seq_num
                };
                let end_seq_num = if millis == start.millis {
                    u64::MAX
                } else {
                    end.seq_num
                };
                let entries = trie.get_range_incl(start_seq_num, end_seq_num);

                entries
                    .into_iter()
                    .map(|(seq_num, v)| {
                        let entry_id = format!("{}-{}", millis.to_string(), seq_num.to_string())
                            .as_bytes()
                            .to_vec();

                        let mut kv_pairs = Vec::with_capacity(v.len() * 2);
                        for (k, v) in v.iter() {
                            kv_pairs.push(k.clone());
                            kv_pairs.push(v.clone());
                        }

                        (entry_id, kv_pairs)
                    })
                    .collect::<Vec<(Vec<u8>, Vec<Vec<u8>>)>>()
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    // use super::*;

    #[test]
    fn test_stream_xrange() {}
}
