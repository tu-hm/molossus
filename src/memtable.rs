use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

pub struct MemTable {
    pub map: SkipMap<Bytes, Bytes>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        let key_bytes = Bytes::copy_from_slice(&key);
        let value_bytes = Bytes::copy_from_slice(&value);
        self.map.insert(key_bytes, value_bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_keeps_keys_sorted() {
        let memtable = MemTable::new();

        memtable.put(b"zebra", b"value_z");
        memtable.put(b"apple", b"value_a");
        memtable.put(b"monkey", b"value_m");

        let mut keys = Vec::new();
        for entry in memtable.map.iter() {
            keys.push(entry.key().clone());
        }

        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0].as_ref(), b"apple");
        assert_eq!(keys[1].as_ref(), b"monkey");
        assert_eq!(keys[2].as_ref(), b"zebra");
    }
}
