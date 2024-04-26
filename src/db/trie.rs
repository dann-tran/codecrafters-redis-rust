const CHAR_BITSIZE: usize = 4; // Must divides 8
const CHARSET_SIZE: usize = 1 << CHAR_BITSIZE;

struct TrieNode<T> {
    children: [Option<Box<Self>>; CHARSET_SIZE],
    value: Option<T>,
}

impl<T> TrieNode<T> {
    pub(crate) fn new() -> Self {
        Self {
            children: std::array::from_fn(|_| None),
            value: None,
        }
    }

    pub(crate) fn get_all(&self) -> Vec<(Vec<u8>, &T)> {
        // DFS
        let mut data = Vec::new();
        if let Some(v) = &self.value {
            data.push((vec![], v));
        }

        let mut stack = Vec::new();
        for idx in (0..CHARSET_SIZE).rev() {
            if let Some(n) = &self.children[idx] {
                stack.push((vec![idx as u8], n));
            }
        }

        while let Some((chars, n)) = stack.pop() {
            if let Some(v) = &n.value {
                data.push((chars.clone(), v));
            }
            for _idx in (0..CHARSET_SIZE).rev() {
                if let Some(n) = &n.children[_idx] {
                    let mut _chars = chars.clone();
                    _chars.push(_idx as u8);
                    stack.push((_chars, n));
                }
            }
        }

        data
    }
}

pub(crate) struct Trie<T> {
    root: TrieNode<T>,
}

fn u64_to_chars(val: u64) -> Vec<u8> {
    val.to_be_bytes()
        .into_iter()
        .flat_map(|byte| {
            [byte >> CHAR_BITSIZE, byte % (1 << CHAR_BITSIZE)]
                .to_vec()
                .into_iter()
        })
        .collect()
}

impl<T> Trie<T> {
    pub(crate) fn new() -> Self {
        Self {
            root: TrieNode::new(),
        }
    }

    pub(crate) fn insert(&mut self, key: u64, value: T) {
        // key is an array of 0..=15
        let mut node = &mut self.root;

        let mut chars = u64_to_chars(key).into_iter();
        while let Some(c) = chars.next() {
            let idx = c as usize;
            if node.children[idx].is_none() {
                node.children[idx] = Some(Box::new(TrieNode::new()));
            }
            node = node.children[idx].as_mut().expect("Not None");
        }

        node.value = Some(value);
    }

    pub(crate) fn get_mut(&mut self, key: u64) -> Option<&mut T> {
        let mut node = &mut self.root;

        let mut chars = u64_to_chars(key).into_iter();
        while let Some(c) = chars.next() {
            let idx = c as usize;
            node = match node.children[idx].is_some() {
                true => node.children[idx].as_mut().expect("Not None"),
                false => return None,
            }
        }

        node.value.as_mut()
    }

    pub(crate) fn contains_key(&self, key: u64) -> bool {
        let mut node = &self.root;

        let mut chars = u64_to_chars(key).into_iter();
        while let Some(c) = chars.next() {
            let idx = c as usize;
            node = match node.children[idx].is_some() {
                true => node.children[idx].as_ref().expect("Not None"),
                false => return false,
            }
        }

        node.value.is_some()
    }

    pub(crate) fn get_range_incl(&self, start: u64, end: u64) -> Vec<(u64, &T)> {
        // Assumptions: start <= end
        let mut node = &self.root;
        let start_iter = u64_to_chars(start).into_iter();
        let end_iter = u64_to_chars(end).into_iter();
        let mut cpair_iter = start_iter.zip(end_iter);
        let mut common_chars = 0u64;
        let mut cpair = None;

        // find first u4 char that differs in start and end
        while let Some((start_char, end_char)) = cpair_iter.next() {
            if start_char != end_char {
                cpair = Some((start_char, end_char));
                break;
            }
            common_chars = (common_chars << CHAR_BITSIZE) + start_char as u64;
            let idx = start_char as usize;
            match &node.children[idx] {
                Some(n) => {
                    node = n.as_ref();
                }
                None => {
                    return vec![];
                }
            }
        }

        match cpair {
            Some((start_char, end_char)) => {
                let common_node = node;
                let mut data = Vec::new();

                let (start_chars, end_chars) = cpair_iter.fold(
                    (
                        Vec::with_capacity(CHARSET_SIZE),
                        Vec::with_capacity(CHARSET_SIZE),
                    ),
                    |(mut start_chars, mut end_chars), (start_c, end_c)| {
                        start_chars.push(start_c);
                        end_chars.push(end_c);
                        (start_chars, end_chars)
                    },
                );

                // collect values along the paths to start node
                let mut depth_delta = 0usize;
                let mut stack = Vec::new();
                let mut start_iter = start_chars.into_iter();
                let mut node = common_node;
                let mut char = start_char;
                loop {
                    common_chars = (common_chars << CHAR_BITSIZE) + char as u64;
                    depth_delta += 1;

                    node = match node.children[char as usize..].iter().find(|&n| n.is_some()) {
                        Some(n) => n.as_ref().expect("Not None"),
                        None => {
                            break;
                        }
                    };

                    for idx in ((char + 1) as usize..CHARSET_SIZE).rev() {
                        if let Some(n) = &node.children[idx] {
                            stack.push((common_chars, n));
                        }
                    }

                    if let Some(v) = &node.value {
                        data.push((common_chars, v));
                    }
                    match start_iter.next() {
                        Some(c) => {
                            char = c;
                        }
                        None => {
                            break;
                        }
                    }
                }

                while let Some((chars, n)) = stack.pop() {
                    let mut items = n
                        .get_all()
                        .into_iter()
                        .map(|(_chars, v)| {
                            (
                                (chars << (4 * _chars.len()))
                                    + _chars
                                        .into_iter()
                                        .fold(0u64, |acc, c| (acc << CHAR_BITSIZE) + c as u64),
                                v,
                            )
                        })
                        .collect();
                    data.append(&mut items);
                }

                // collect values after the divergence point, between the paths to start and end nodes
                common_chars = common_chars >> (4 * depth_delta);
                for c in (start_char + 1)..end_char {
                    if let Some(n) = &common_node.children[c as usize] {
                        common_chars = (common_chars << CHAR_BITSIZE) + c as u64;

                        let mut items = n
                            .get_all()
                            .into_iter()
                            .map(|(_chars, v)| {
                                (
                                    (common_chars << (4 * _chars.len()))
                                        + _chars
                                            .into_iter()
                                            .fold(0u64, |acc, c| (acc << CHAR_BITSIZE) + c as u64),
                                    v,
                                )
                            })
                            .collect();
                        data.append(&mut items);

                        common_chars = common_chars >> CHAR_BITSIZE;
                    }
                }

                // collect values on the path to end node
                let mut node = common_node;
                let mut end_iter = end_chars.iter();
                let mut char = end_char;
                loop {
                    common_chars = (common_chars << CHAR_BITSIZE) + char as u64;
                    node = if let Some(n) = &node.children[char as usize] {
                        n.as_ref()
                    } else {
                        break;
                    };

                    for c in 0..char {
                        if let Some(n) = &node.children[c as usize] {
                            common_chars = (common_chars << CHAR_BITSIZE) + c as u64;

                            let mut items = n
                                .get_all()
                                .into_iter()
                                .map(|(mut _chars, v)| {
                                    (
                                        (common_chars << (4 * _chars.len()))
                                            + _chars.into_iter().fold(0u64, |acc, c| {
                                                (acc << CHAR_BITSIZE) + c as u64
                                            }),
                                        v,
                                    )
                                })
                                .collect();
                            data.append(&mut items);

                            common_chars = common_chars >> CHAR_BITSIZE;
                        }
                    }

                    if let Some(v) = &node.value {
                        data.push((common_chars.clone(), v));
                    }

                    match end_iter.next() {
                        Some(&c) => {
                            char = c;
                        }
                        None => {
                            break;
                        }
                    }
                }

                data
            }
            None => node
                .get_all()
                .into_iter()
                .map(|(chars, v)| {
                    (
                        (common_chars << (4 * chars.len()))
                            + chars
                                .into_iter()
                                .fold(0u64, |acc, c| (acc << CHAR_BITSIZE) + c as u64),
                        v,
                    )
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trie_getall() {
        let mut trie = Trie::new();
        trie.insert(2, "two".to_string());
        trie.insert(16, "sixteen".to_string());

        let actual = trie.root.get_all();

        assert_eq!(actual.len(), 2);
        let (key0, val0) = &actual[0];
        assert_eq!(*key0, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);
        assert_eq!(*val0, "two");
        let (key1, val1) = &actual[1];
        assert_eq!(*key1, vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]);
        assert_eq!(*val1, "sixteen");
    }

    #[test]
    fn test_trie_getrangeinclusive() {
        let mut trie = Trie::new();
        trie.insert(2, "test".to_string());
        trie.insert(4, "test".to_string());
        trie.insert(8, "test".to_string());

        let actual = trie.get_range_incl(2, 5);

        assert_eq!(actual.len(), 2, "{:?}", actual);
        let (key0, val0) = &actual[0];
        assert_eq!(*key0, 2);
        assert_eq!(*val0, "test");
        let (key1, val1) = &actual[1];
        assert_eq!(*key1, 4);
        assert_eq!(*val1, "test");
    }
}
