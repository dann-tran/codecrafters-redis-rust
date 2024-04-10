use std::{cmp::Ordering, collections::HashMap};

use crate::trie::Trie;

#[derive(Debug, Clone, Eq)]
pub(crate) struct StreamEntryID {
    pub(crate) millis: Vec<u8>,
    pub(crate) seq_num: Vec<u8>,
}

impl PartialEq for StreamEntryID {
    fn eq(&self, other: &Self) -> bool {
        self.millis == other.millis && self.seq_num == other.seq_num
    }
}

impl PartialOrd for StreamEntryID {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamEntryID {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.millis.len().cmp(&other.millis.len()) {
            std::cmp::Ordering::Equal => match self.millis.cmp(&other.millis) {
                std::cmp::Ordering::Equal => match self.seq_num.len().cmp(&other.seq_num.len()) {
                    std::cmp::Ordering::Equal => self.seq_num.cmp(&other.seq_num),
                    ord => ord,
                },
                ord => ord,
            },
            ord => ord,
        }
    }
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
pub(crate) struct RedisStream {
    root: Trie<Trie<HashMap<Vec<u8>, Vec<u8>>>>,
    last_entry: Option<StreamEntryID>,
}

impl RedisStream {
    pub(crate) fn new() -> Self {
        Self {
            root: Trie::new(),
            last_entry: None,
        }
    }

    pub(crate) fn insert(
        &mut self,
        entry_id: &StreamEntryID,
        data: HashMap<Vec<u8>, Vec<u8>>,
    ) -> anyhow::Result<()> {
        if *entry_id
            == (StreamEntryID {
                millis: vec![b'0'],
                seq_num: vec![b'0'],
            })
        {
            return Err(anyhow::anyhow!(
                "ERR The ID specified in XADD must be greater than 0-0"
            ));
        }
        if let Some(last_entry) = &self.last_entry {
            if entry_id <= last_entry {
                return Err(anyhow::anyhow!(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                ));
            }
        }
        self.last_entry = Some(entry_id.clone());

        if !self.root.contains_key(&entry_id.millis) {
            self.root.insert(&entry_id.millis, Trie::new());
        }

        let node = self.root.get_mut(&entry_id.millis).expect("Not None");

        node.insert(&entry_id.seq_num, data);

        Ok(())
    }
}
