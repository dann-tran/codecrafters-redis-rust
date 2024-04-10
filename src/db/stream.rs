use std::collections::HashMap;

use crate::trie::Trie;

pub(crate) struct RedisStream {
    root: Trie<Trie<HashMap<Vec<u8>, Vec<u8>>>>,
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

impl RedisStream {
    pub(crate) fn new() -> Self {
        Self { root: Trie::new() }
    }

    pub(crate) fn insert(&mut self, entry_id: &StreamEntryID, data: HashMap<Vec<u8>, Vec<u8>>) {
        if !self.root.contains_key(&entry_id.millis) {
            self.root.insert(&entry_id.millis, Trie::new());
        }

        let node = self.root.get_mut(&entry_id.millis).expect("Not None");
        node.insert(&entry_id.seq_num, data);
    }
}
