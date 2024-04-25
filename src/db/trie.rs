struct TrieNode<T> {
    children: [Option<Box<Self>>; 16], // u4 vocab
    value: Option<T>,
}

impl<T> TrieNode<T> {
    pub(crate) fn new() -> Self {
        Self {
            children: std::array::from_fn(|_| None),
            value: None,
        }
    }

    // pub(crate) fn get_all(&self) -> Vec<(u64, &T)> {
    //     // DFS
    //     let mut data = Vec::new();
    //     if let Some(v) = &self.value {
    //         data.push((vec![], v));
    //     }

    //     let mut stack = Vec::new();
    //     for idx in (0..10).rev() {
    //         if let Some(n) = &self.children[idx] {
    //             stack.push((vec![b'0' + idx as u8], n));
    //         }
    //     }

    //     while let Some((chars, n)) = stack.pop() {
    //         if let Some(v) = &n.value {
    //             data.push((chars.clone(), v));
    //         }
    //         for idx in (0..10).rev() {
    //             if let Some(n) = &n.children[idx] {
    //                 let mut _chars = chars.clone();
    //                 _chars.push(b'0' + idx as u8);
    //                 stack.push((_chars, n));
    //             }
    //         }
    //     }

    //     data
    // }
}

pub(crate) struct Trie<T> {
    root: TrieNode<T>,
}

fn byte2idx(b: u8) -> usize {
    if b < b'0' || b > b'9' {
        panic!("Expect numeric byte, found: {b}");
    }
    (b - b'0').into()
}

fn u64_to_u4s(val: u64) -> Vec<u8> {
    val.to_be_bytes()
        .into_iter()
        .flat_map(|byte| [byte >> 4, byte % (1 << 4)].to_vec().into_iter())
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

        let mut chars_u4 = u64_to_u4s(key).into_iter();
        while let Some(c_u4) = chars_u4.next() {
            let idx = c_u4 as usize;
            if node.children[idx].is_none() {
                node.children[idx] = Some(Box::new(TrieNode::new()));
            }
            node = node.children[idx].as_mut().expect("Not None");
        }

        node.value = Some(value);
    }

    pub(crate) fn get_mut(&mut self, key: u64) -> Option<&mut T> {
        let mut node = &mut self.root;

        let mut chars_u4 = u64_to_u4s(key).into_iter();
        while let Some(c_u4) = chars_u4.next() {
            let idx = c_u4 as usize;
            node = match node.children[idx].is_some() {
                true => node.children[idx].as_mut().expect("Not None"),
                false => return None,
            }
        }

        node.value.as_mut()
    }

    pub(crate) fn contains_key(&self, key: u64) -> bool {
        let mut node = &self.root;

        let mut chars_u4 = u64_to_u4s(key).into_iter();
        while let Some(c_u4) = chars_u4.next() {
            let idx = c_u4 as usize;
            node = match node.children[idx].is_some() {
                true => node.children[idx].as_ref().expect("Not None"),
                false => return false,
            }
        }

        node.value.is_some()
    }

    // pub(crate) fn get_all(&self) -> Vec<(u64, &T)> {
    //     self.root.get_all()
    // }

    // pub(crate) fn get_range_incl(
    //     &self,
    //     start: Option<&[u8]>,
    //     end: Option<&[u8]>,
    // ) -> Vec<(Vec<u8>, &T)> {
    //     eprintln!("Get range inclusive: {start:?} {end:?}");
    //     // Assumptions:
    //     // 1. start, end, and all millisecondsTime of entry IDs have the same number of digits
    //     // 2. start <= end
    //     let mut node = &self.root;
    //     let start_iter = start.map_or_else(
    //         || std::iter::empty::<&[u8]>() as std::slice::Iter<'_, u8>,
    //         |start| start.iter(),
    //     );
    //     let mut cpair_iter = start.iter().zip(end.iter());
    //     let mut common_chars = Vec::new();
    //     let mut cpair = None;

    //     while let Some((&start_c, &end_c)) = cpair_iter.next() {
    //         if start_c != end_c {
    //             cpair = Some((start_c, end_c));
    //             break;
    //         }
    //         common_chars.push(start_c);
    //         let idx = byte2idx(start_c);
    //         match &node.children[idx] {
    //             Some(n) => {
    //                 node = n.as_ref();
    //             }
    //             None => {
    //                 return vec![];
    //             }
    //         }
    //     }

    //     match cpair {
    //         Some((mut start_c, mut end_c)) => {
    //             let common_node = node;
    //             let mut data = Vec::new();
    //             let (start_chars, end_chars) = cpair_iter.fold(
    //                 (Vec::new(), Vec::new()),
    //                 |(mut start_chars, mut end_chars), (&start_c, &end_c)| {
    //                     start_chars.push(start_c);
    //                     end_chars.push(end_c);
    //                     (start_chars, end_chars)
    //                 },
    //             );

    //             let mut depth_delta = 0usize;
    //             let mut stack = Vec::new();
    //             let mut start_iter = start_chars.iter();
    //             loop {
    //                 common_chars.push(start_c);
    //                 depth_delta += 1;

    //                 let mut idx = byte2idx(start_c);
    //                 while idx < 10 && node.children[idx].is_none() {
    //                     idx += 1;
    //                 }

    //                 if idx < 10 {
    //                     match &node.children[idx] {
    //                         Some(n) => {
    //                             for idx in ((idx + 1)..10).rev() {
    //                                 if let Some(n) = &node.children[idx] {
    //                                     stack.push((common_chars.clone(), n));
    //                                 }
    //                             }

    //                             node = n.as_ref();
    //                             match start_iter.next() {
    //                                 Some(&c) => {
    //                                     start_c = c;
    //                                 }
    //                                 None => {
    //                                     if let Some(v) = &node.value {
    //                                         data.push((common_chars.clone(), v));
    //                                     }
    //                                 }
    //                             }
    //                         }
    //                         None => {
    //                             break;
    //                         }
    //                     }
    //                 }
    //             }

    //             while let Some((chars, n)) = stack.pop() {
    //                 let mut items = n
    //                     .get_all()
    //                     .into_iter()
    //                     .map(|(mut _chars, v)| {
    //                         let mut chars = chars.clone();
    //                         chars.append(&mut _chars);
    //                         (chars, v)
    //                     })
    //                     .collect();
    //                 data.append(&mut items);
    //             }

    //             for _ in 0..depth_delta {
    //                 common_chars.pop();
    //             }
    //             for c in (start_c + 1)..end_c {
    //                 if let Some(n) = &common_node.children[byte2idx(c)] {
    //                     common_chars.push(c);

    //                     let mut items = n
    //                         .get_all()
    //                         .into_iter()
    //                         .map(|(mut _chars, v)| {
    //                             let mut chars = common_chars.clone();
    //                             chars.append(&mut _chars);
    //                             (chars, v)
    //                         })
    //                         .collect();
    //                     data.append(&mut items);

    //                     common_chars.pop();
    //                 }
    //             }

    //             let mut node = common_node;
    //             let mut end_iter = end_chars.iter();
    //             loop {
    //                 common_chars.push(end_c);

    //                 for c in b'0'..end_c {
    //                     if let Some(n) = &node.children[byte2idx(c)] {
    //                         common_chars.push(c);

    //                         let mut items = n
    //                             .get_all()
    //                             .into_iter()
    //                             .map(|(mut _chars, v)| {
    //                                 let mut chars = common_chars.clone();
    //                                 chars.append(&mut _chars);
    //                                 (chars, v)
    //                             })
    //                             .collect();
    //                         data.append(&mut items);

    //                         common_chars.pop();
    //                     }
    //                 }

    //                 let idx = byte2idx(end_c);
    //                 match &node.children[idx] {
    //                     Some(n) => {
    //                         node = n.as_ref();
    //                         match end_iter.next() {
    //                             Some(&c) => {
    //                                 end_c = c;
    //                             }
    //                             None => {
    //                                 if let Some(v) = &node.value {
    //                                     data.push((common_chars.clone(), v));
    //                                 }
    //                             }
    //                         }
    //                     }
    //                     None => {
    //                         break;
    //                     }
    //                 }
    //             }

    //             data
    //         }
    //         None => node
    //             .get_all()
    //             .into_iter()
    //             .map(|(mut chars, v)| {
    //                 let mut _chars = common_chars.clone();
    //                 _chars.append(&mut chars);
    //                 (_chars, v)
    //             })
    //             .collect(),
    //     }
    // }
}
