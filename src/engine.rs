use crate::memtable::MemTable;
use crate::wal::Wal;
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};

pub struct LsmStorageOptions {
    pub target_sst_size: usize,
}

impl Default for LsmStorageOptions {
    fn default() -> Self {
        Self {
            target_sst_size: 256 * 1024 * 1024,
        }
    }
}

pub struct LsmStorageState {
    pub memtable: Arc<MemTable>,
    pub imm_memtables: VecDeque<Arc<MemTable>>,
}

impl LsmStorageState {
    pub fn create() -> Self {
        Self {
            memtable: Arc::new(MemTable::new()),
            imm_memtables: VecDeque::new(),
        }
    }
}

pub struct LsmEngine {
    state: Arc<RwLock<Arc<LsmStorageState>>>,
    wal: Mutex<Wal>,
    options: Arc<LsmStorageOptions>,
}

impl LsmEngine {
    pub fn open(wal_path: impl AsRef<std::path::Path>, options: LsmStorageOptions) -> Self {
        let wal = Wal::open(&wal_path);

        let state = LsmStorageState::create();
        let recovered_entries = wal.recover();
        for (key, value) in recovered_entries {
            state.memtable.put(&key, &value);
        }

        Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            wal: Mutex::new(wal),
            options: Arc::new(options),
        }
    }

    fn check_freeze(&self) {
        let mut state_lock = self.state.write().unwrap();
        // Check again after acquiring write lock
        if state_lock.memtable.approximate_size() >= self.options.target_sst_size {
            let mut new_state = LsmStorageState {
                memtable: Arc::new(MemTable::new()),
                imm_memtables: state_lock.imm_memtables.clone(),
            };
            new_state
                .imm_memtables
                .push_front(state_lock.memtable.clone());
            *state_lock = Arc::new(new_state);
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.wal.lock().unwrap().append(key, value);

        // Put and check capacity
        let size = {
            let state = self.state.read().unwrap().clone();
            state.memtable.put(key, value);
            state.memtable.approximate_size()
        };

        if size >= self.options.target_sst_size {
            self.check_freeze();
        }
    }

    pub fn delete(&self, key: &[u8]) {
        self.put(key, b""); // Empty slice is a delete tombstone
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let state = self.state.read().unwrap().clone();

        // 1. check mutable memtable
        if let Some(value) = state.memtable.get(key) {
            if value.is_empty() {
                return None; // Delete tombstone
            }
            return Some(value);
        }

        // 2. check immutable memtables from newest to oldest
        for memtable in state.imm_memtables.iter() {
            if let Some(value) = memtable.get(key) {
                if value.is_empty() {
                    return None; // Delete tombstone
                }
                return Some(value);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_engine_writes_recovers_and_deletes() {
        let dir = tempdir().expect("Failed to create temp dir");
        let wal_path = dir.path().join("primary.wal");

        // Init the data
        let engine = LsmEngine::open(&wal_path, LsmStorageOptions::default());
        engine.put(b"batman", b"bruce");
        engine.put(b"superman", b"clark");

        assert_eq!(engine.get(b"batman").unwrap().as_ref(), b"bruce");
        assert_eq!(engine.get(b"superman").unwrap().as_ref(), b"clark");

        engine.delete(b"superman");
        assert_eq!(engine.get(b"superman"), None);

        drop(engine);

        // test recover
        let recovered_engine = LsmEngine::open(&wal_path, LsmStorageOptions::default());

        assert_eq!(recovered_engine.get(b"batman").unwrap().as_ref(), b"bruce");
        assert_eq!(recovered_engine.get(b"superman"), None); // should still be deleted
        assert_eq!(recovered_engine.get(b"one ok rock"), None);
    }

    #[test]
    fn test_engine_freezes_memtable() {
        let dir = tempdir().expect("Failed to create temp dir");
        let wal_path = dir.path().join("primary.wal");

        let options = LsmStorageOptions {
            target_sst_size: 10,
        };

        let engine = LsmEngine::open(&wal_path, options);
        engine.put(b"k1", b"v1"); // Size 4
        engine.put(b"k2", b"v2"); // Size 8
        engine.put(b"k3", b"v3"); // Size 12 -> triggers freeze

        let state = engine.state.read().unwrap().clone();
        assert_eq!(state.imm_memtables.len(), 1);
        assert_eq!(state.memtable.approximate_size(), 0);

        assert_eq!(engine.get(b"k1").unwrap().as_ref(), b"v1");
        assert_eq!(engine.get(b"k3").unwrap().as_ref(), b"v3");
    }

    #[test]
    fn test_engine_gets_latest_value() {
        let dir = tempdir().expect("Failed to create temp dir");
        let wal_path = dir.path().join("primary.wal");

        let options = LsmStorageOptions {
            target_sst_size: 10,
        };

        let engine = LsmEngine::open(&wal_path, options);
        engine.put(b"k1", b"v1");
        engine.put(b"k2", b"v2");
        engine.put(b"k3", b"v3"); // Triggers freeze, k1/k2/k3 are in imm_memtable

        engine.put(b"k1", b"v1_new"); // Overwrites k1 in mutable memtable
        engine.delete(b"k2");         // Tombstones k2 in mutable memtable

        // Check mutable memtable latest values
        assert_eq!(engine.get(b"k1").unwrap().as_ref(), b"v1_new");
        assert_eq!(engine.get(b"k2"), None);
        // Check immutable memtable fallback
        assert_eq!(engine.get(b"k3").unwrap().as_ref(), b"v3");
    }
}
