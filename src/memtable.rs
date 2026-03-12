use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct MemTable {
    pub map: SkipMap<Bytes, Bytes>,
    approximate_size: Arc<AtomicUsize>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        self.approximate_size
            .fetch_add(key.len() + value.len(), Ordering::Relaxed);
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).map(|e| e.value().clone())
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size.load(Ordering::Relaxed)
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

    #[test]
    fn test_memtable_get_and_overwrite() {
        let memtable = MemTable::new();
        memtable.put(b"key1", b"value1");
        assert_eq!(memtable.get(b"key1").unwrap().as_ref(), b"value1");
        memtable.put(b"key1", b"value2");
        assert_eq!(memtable.get(b"key1").unwrap().as_ref(), b"value2");

        let mut count = 0;
        for entry in memtable.map.iter() {
            if entry.key().as_ref() == b"key1" {
                count += 1;
            }
        }
        assert_eq!(count, 1);
    }
}
