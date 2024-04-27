use std::{
    collections::HashMap,
    fmt,
    time::{Duration, SystemTime},
};

use crate::command::XReadStreamArg;

use self::stream::{RedisStream, ReqStreamEntryID, StreamEntryID};

pub(crate) mod stream;
mod trie;

pub(crate) enum RedisValueType {
    String,
    Stream,
}

impl fmt::Display for RedisValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            RedisValueType::String => "string",
            RedisValueType::Stream => "stream",
        };
        write!(f, "{}", s)
    }
}

pub(crate) struct RedisDb {
    pub(crate) nonexpire_table: HashMap<Vec<u8>, Vec<u8>>,
    pub(crate) expire_table: HashMap<Vec<u8>, (Vec<u8>, SystemTime)>,
    pub(crate) streams: HashMap<Vec<u8>, RedisStream>,
}

impl RedisDb {
    pub fn new() -> Self {
        Self {
            nonexpire_table: HashMap::new(),
            expire_table: HashMap::new(),
            streams: HashMap::new(),
        }
    }

    pub(crate) fn get(&mut self, key: &Vec<u8>) -> Option<Vec<u8>> {
        match self.nonexpire_table.get(key) {
            Some(val) => {
                eprintln!("Found key in nonexpire table: {:?}", key);
                Some(val.clone())
            }
            None => match self.expire_table.get(key) {
                Some((val, expiry)) => {
                    eprintln!("Found key in expire table: {:?}", key);
                    if SystemTime::now() >= *expiry {
                        eprintln!("Key has expired");
                        self.expire_table.remove(key);
                        None
                    } else {
                        eprintln!("Key has not expired");
                        Some(val.clone())
                    }
                }
                None => {
                    eprintln!("Key not found");
                    None
                }
            },
        }
    }

    pub(crate) fn set(&mut self, key: &Vec<u8>, value: Vec<u8>, px: Option<Duration>) {
        let nonexpire_val = self.nonexpire_table.get_mut(key);
        if let Some(stored_val) = nonexpire_val {
            eprintln!("Key found in nonexpire table: {:?}", key);
            match px {
                Some(dur) => {
                    eprintln!(
                        "KV has an expiry; remove key from nonexpire and add to expire table."
                    );
                    self.nonexpire_table.remove(key);
                    self.expire_table
                        .insert(key.clone(), (value.clone(), SystemTime::now() + dur));
                }
                None => {
                    eprintln!("KV has no expiry; mutate value in-place.");
                    *stored_val = value.clone();
                }
            }
            return;
        }

        let expire_val = self.expire_table.get_mut(key);
        if let Some(stored_val) = expire_val {
            eprintln!("Key found in expire table: {:?}", key);
            match px {
                Some(dur) => {
                    eprintln!("KV has an expiry; mutate value and expiry in-place.");
                    *stored_val = (value.clone(), SystemTime::now() + dur);
                }
                None => {
                    eprintln!(
                        "KV has no expiry; remove key from expire table and add to nonexpire."
                    );
                    self.expire_table.remove(key);
                    self.nonexpire_table.insert(key.clone(), value.clone());
                }
            }
            return;
        }

        eprintln!("New key: {:?}", key);
        match px {
            Some(dur) => {
                self.expire_table
                    .insert(key.clone(), (value.clone(), SystemTime::now() + dur));
            }
            None => {
                self.nonexpire_table.insert(key.clone(), value.clone());
            }
        }
    }

    pub(crate) fn keys(&self) -> Vec<Vec<u8>> {
        let mut keys = self
            .nonexpire_table
            .keys()
            .map(|k| k.clone())
            .collect::<Vec<Vec<u8>>>();
        let mut expire_keys = self
            .expire_table
            .keys()
            .map(|k| k.clone())
            .collect::<Vec<Vec<u8>>>();
        keys.append(&mut expire_keys);
        keys
    }

    pub(crate) fn lookup_type(&mut self, key: &Vec<u8>) -> Option<RedisValueType> {
        self.get(key).map(|_| RedisValueType::String).or_else(|| {
            if self.streams.contains_key(key) {
                Some(RedisValueType::Stream)
            } else {
                None
            }
        })
    }

    pub(crate) fn xadd(
        &mut self,
        key: &Vec<u8>,
        entry_id: Option<ReqStreamEntryID>,
        data: HashMap<Vec<u8>, Vec<u8>>,
    ) -> anyhow::Result<StreamEntryID> {
        match self.streams.get_mut(key) {
            Some(stream) => stream.insert(entry_id, data),
            None => {
                let mut stream = RedisStream::new();
                let res = stream.insert(entry_id, data)?;
                self.streams.insert(key.clone(), stream);
                Ok(res)
            }
        }
    }

    pub(crate) fn xrange(
        &self,
        key: &Vec<u8>,
        start: StreamEntryID,
        end: StreamEntryID,
    ) -> Vec<(Vec<u8>, Vec<Vec<u8>>)> {
        self.streams
            .get(key)
            .map_or_else(|| vec![], |stream| stream.xrange(start, end))
    }

    pub(crate) fn xread(
        &self,
        args: &Vec<XReadStreamArg>,
    ) -> Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<Vec<u8>>)>)> {
        args.into_iter()
            .map(|arg| {
                return (
                    arg.key.clone(),
                    self.streams
                        .get(&arg.key)
                        .map_or_else(|| vec![], |stream| stream.xread(&arg.start)),
                );
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    fn get_sample_db() -> RedisDb {
        let mut db = RedisDb::new();

        let _ = db.xadd(
            &b"apple".to_vec(),
            Some(ReqStreamEntryID {
                millis: 0,
                seq_num: Some(1),
            }),
            HashMap::from([(b"temperature".to_vec(), b"23".to_vec())]),
        );

        let _ = db.xadd(
            &b"apple".to_vec(),
            Some(ReqStreamEntryID {
                millis: 0,
                seq_num: Some(2),
            }),
            HashMap::from([(b"temperature".to_vec(), b"24".to_vec())]),
        );

        let _ = db.xadd(
            &b"orange".to_vec(),
            Some(ReqStreamEntryID {
                millis: 0,
                seq_num: Some(4),
            }),
            HashMap::from([(b"temperature".to_vec(), b"20".to_vec())]),
        );

        db
    }

    #[test]
    fn test_xread_singlestream() {
        // Arrange
        let db = get_sample_db();
        let args = &vec![XReadStreamArg {
            key: b"apple".to_vec(),
            start: StreamEntryID {
                millis: 0,
                seq_num: 1,
            },
        }];
        let expected = vec![(
            b"apple".to_vec(),
            vec![(
                b"0-2".to_vec(),
                vec![b"temperature".to_vec(), b"24".to_vec()],
            )],
        )];

        // Act
        let actual = db.xread(args);

        // Assert
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_xread_multistream() {
        // Arrange
        let db = get_sample_db();
        let args = &vec![
            XReadStreamArg {
                key: b"apple".to_vec(),
                start: StreamEntryID {
                    millis: 0,
                    seq_num: 1,
                },
            },
            XReadStreamArg {
                key: b"orange".to_vec(),
                start: StreamEntryID {
                    millis: 0,
                    seq_num: 3,
                },
            },
        ];
        let expected = vec![
            (
                b"apple".to_vec(),
                vec![(
                    b"0-2".to_vec(),
                    vec![b"temperature".to_vec(), b"24".to_vec()],
                )],
            ),
            (
                b"orange".to_vec(),
                vec![(
                    b"0-4".to_vec(),
                    vec![b"temperature".to_vec(), b"20".to_vec()],
                )],
            ),
        ];

        // Act
        let actual = db.xread(args);

        // Assert
        assert_eq!(actual, expected);
    }
}
