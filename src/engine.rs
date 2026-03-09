use bytes::Bytes;
use crate::memtable::MemTable;
use crate::wal::Wal;

pub struct LsmEngine {
    memtable: MemTable,
    wal: Wal
}

impl LsmEngine {
    pub fn open(wal_path: impl AsRef<std::path::Path>) -> Self {
        let wal = Wal::open(&wal_path);

        let memtable = MemTable::new();

        let recovered_entries = wal.recover();
        for (key, value) in recovered_entries {
            memtable.put(&key, &value);
        }

        Self { memtable, wal }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.wal.append(&key, &value);
        self.memtable.put(&key, &value);
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.memtable.map.get(key).map(|entry| entry.value().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_engine_writes_and_recovers_state() {
        let dir = tempdir().expect("Failed to create temp dir");
        let wal_path = dir.path().join("primary.wal");

        // Init the data
        let mut engine = LsmEngine::open(&wal_path);
        engine.put(b"batman", b"bruce");
        engine.put(b"superman", b"clark");

        assert_eq!(engine.get(b"batman").unwrap().as_ref(), b"bruce");
        assert_eq!(engine.get(b"superman").unwrap().as_ref(), b"clark");

        drop(engine);

        // test recover
        let recovered_engine = LsmEngine::open(&wal_path);

        assert_eq!(recovered_engine.get(b"batman").unwrap().as_ref(), b"bruce");
        assert_eq!(recovered_engine.get(b"superman").unwrap().as_ref(), b"clark");

        assert_eq!(recovered_engine.get(b"one ok rock"), None);
    }
}