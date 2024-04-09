use std::{
    collections::HashMap,
    fmt,
    time::{Duration, SystemTime},
};

pub(crate) enum RedisValueType {
    String,
}

impl fmt::Display for RedisValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            RedisValueType::String => "string",
        };
        write!(f, "{}", s)
    }
}

pub(crate) struct RedisDb {
    pub(crate) nonexpire_table: HashMap<Vec<u8>, Vec<u8>>,
    pub(crate) expire_table: HashMap<Vec<u8>, (Vec<u8>, SystemTime)>,
}

impl RedisDb {
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

    pub(crate) fn set(&mut self, key: &Vec<u8>, value: &Vec<u8>, px: &Option<Duration>) {
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
                        .insert(key.clone(), (value.clone(), SystemTime::now() + *dur));
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
                    *stored_val = (value.clone(), SystemTime::now() + *dur);
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
                    .insert(key.clone(), (value.clone(), SystemTime::now() + *dur));
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
        self.get(key).map(|_| RedisValueType::String)
    }

    pub fn new() -> Self {
        Self {
            nonexpire_table: HashMap::new(),
            expire_table: HashMap::new(),
        }
    }
}
