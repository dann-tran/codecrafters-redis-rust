struct TrieNode<T> {
    children: [Option<Box<Self>>; 10], // 0..9
    value: Option<T>,
}

impl<T> TrieNode<T> {
    pub(crate) fn new() -> Self {
        Self {
            children: std::array::from_fn(|_| None),
            value: None,
        }
    }
}

pub(crate) struct Trie<T> {
    root: TrieNode<T>,
}

fn byte2idx(b: u8) -> usize {
    (b - b'0').into()
}

impl<T> Trie<T> {
    pub(crate) fn new() -> Self {
        Self {
            root: TrieNode::new(),
        }
    }

    pub(crate) fn insert(&mut self, key: &[u8], value: T) {
        // key is an array of 0..9
        let mut node = &mut self.root;

        let mut chars = key.iter();
        while let Some(&c) = chars.next() {
            if node.children[byte2idx(c)].is_none() {
                node.children[byte2idx(c)] = Some(Box::new(TrieNode::new()));
            }
            node = node.children[byte2idx(c)].as_mut().expect("Not None");
        }

        node.value = Some(value);
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<&T> {
        let mut node = &self.root;

        for &c in key {
            node = match node.children[byte2idx(c)].is_some() {
                true => node.children[byte2idx(c)].as_ref().expect("Not None"),
                false => return None,
            }
        }

        node.value.as_ref()
    }

    pub(crate) fn get_mut(&mut self, key: &[u8]) -> Option<&mut T> {
        let mut node = &mut self.root;

        for &c in key {
            node = match node.children[byte2idx(c)].is_some() {
                true => node.children[byte2idx(c)].as_mut().expect("Not None"),
                false => return None,
            }
        }

        node.value.as_mut()
    }

    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        let mut node = &self.root;

        for &c in key {
            node = match node.children[byte2idx(c)].is_some() {
                true => node.children[byte2idx(c)].as_ref().expect("Not None"),
                false => return false,
            }
        }

        node.value.is_some()
    }
}
