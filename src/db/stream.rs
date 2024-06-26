use std::{
    cmp::Ordering,
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use super::trie::Trie;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReqStreamEntryID {
    pub(crate) millis: u64,
    pub(crate) seq_num: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
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
    if let Some(req) = req {
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
            Ordering::Equal => {
                if let Some(seq_num) = req.seq_num {
                    if seq_num.cmp(&last_entry.seq_num) == Ordering::Greater {
                        seq_num
                    } else {
                        return Err(anyhow::anyhow!(
                                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                                ));
                    }
                } else {
                    last_entry.seq_num + 1
                }
            }
            Ordering::Greater => req.seq_num.unwrap_or(0),
        };

        Ok(StreamEntryID {
            millis: req.millis,
            seq_num,
        })
    } else if last_entry.millis == 0 && last_entry.seq_num == 0 {
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
                let start_seq_num = if millis == start.millis {
                    start.seq_num
                } else {
                    u64::MIN
                };
                let end_seq_num = if millis == end.millis {
                    end.seq_num
                } else {
                    u64::MAX
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

    pub(crate) fn xread(&self, start: &Option<StreamEntryID>) -> Vec<(Vec<u8>, Vec<Vec<u8>>)> {
        if let Some(start) = start {
            let (millis, seq_num) = if start.seq_num == u64::MAX {
                (start.millis + 1, 0)
            } else {
                (start.millis, start.seq_num + 1)
            };
            let start = StreamEntryID { millis, seq_num };
            let end = StreamEntryID {
                millis: u64::MAX,
                seq_num: u64::MAX,
            };
            self.xrange(start, end)
        } else {
            vec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sample_stream() -> RedisStream {
        let mut stream = RedisStream::new();

        let _ = stream.insert(
            Some(ReqStreamEntryID {
                millis: 0,
                seq_num: Some(2),
            }),
            HashMap::from([
                (b"foo".to_vec(), b"0".to_vec()),
                (b"bar".to_vec(), b"2".to_vec()),
            ]),
        );
        let _ = stream.insert(
            Some(ReqStreamEntryID {
                millis: 0,
                seq_num: Some(3),
            }),
            HashMap::from([
                (b"foo".to_vec(), b"0".to_vec()),
                (b"bar".to_vec(), b"3".to_vec()),
            ]),
        );
        let _ = stream.insert(
            Some(ReqStreamEntryID {
                millis: 0,
                seq_num: Some(4),
            }),
            HashMap::from([
                (b"foo".to_vec(), b"0".to_vec()),
                (b"bar".to_vec(), b"4".to_vec()),
            ]),
        );
        stream
    }

    #[test]
    fn test_stream_xrange() {
        // Arrange
        let stream = make_sample_stream();
        let start = StreamEntryID {
            millis: 0,
            seq_num: 2,
        };
        let end = StreamEntryID {
            millis: 0,
            seq_num: 3,
        };

        let expected_ids = vec![b"0-2".to_vec(), b"0-3".to_vec()];

        // Act
        let actual = stream.xrange(start, end);
        let actual_ids = actual
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<Vec<u8>>>();

        // Assert
        assert_eq!(actual_ids, expected_ids);
    }

    #[test]
    fn test_stream_xrange_startmin() {
        // Arrange
        let stream = make_sample_stream();
        let start = StreamEntryID {
            millis: 0,
            seq_num: 0,
        };
        let end = StreamEntryID {
            millis: 0,
            seq_num: 2,
        };

        let expected_ids = vec![b"0-2".to_vec()];

        // Act
        let actual = stream.xrange(start, end);
        let actual_ids = actual
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<Vec<u8>>>();

        // Assert
        assert_eq!(actual_ids, expected_ids);
    }

    #[test]
    fn test_stream_xrange_endmax() {
        // Arrange
        let stream = make_sample_stream();
        let start = StreamEntryID {
            millis: 0,
            seq_num: 3,
        };
        let end = StreamEntryID {
            millis: u64::MAX,
            seq_num: u64::MAX,
        };

        let expected_ids = vec![b"0-3".to_vec(), b"0-4".to_vec()];

        // Act
        let actual = stream.xrange(start, end);
        let actual_ids = actual
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<Vec<u8>>>();

        // Assert
        assert_eq!(actual_ids, expected_ids);
    }
}
