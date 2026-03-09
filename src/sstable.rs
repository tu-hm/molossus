use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

use memmap2::Mmap;

/// Represents an immutable, sorted on-disk file of key-value pairs.
pub struct SsTable {
    _file: File, // We keep the file handle open so the OS doesn't close it
    mmap: Mmap,  // The memory-mapped file for zero-copy reads
    // Our in-memory index for O(log N) binary search: (Key, Byte Offset)
    index: Vec<(Vec<u8>, u32)>,
}

impl SsTable {
    pub fn build(path: impl AsRef<Path>, iter: impl Iterator<Item = (Vec<u8>, Vec<u8>)>) -> Self {
        let mut file = OpenOptions::new().create(true).append(true).open(&path).expect("failed to open file");

        let mut current_offset = 0u32;
        let mut index = Vec::new();

        for (key, value) in iter {
            index.push((key.clone(), current_offset));

            let k_len = key.len() as u32;
            file.write_all(&k_len.to_le_bytes()).unwrap();
            file.write_all(&key).unwrap();

            let v_len = value.len() as u32;
            file.write_all(&v_len.to_le_bytes()).unwrap();
            file.write_all(&value).unwrap();

            current_offset += 4 + k_len + 4 + v_len;
        }

        let index_start_offset = current_offset;

        for (key, offset) in &index {
            let k_len = key.len() as u32;
            file.write_all(&k_len.to_le_bytes()).unwrap();
            file.write_all(key).unwrap();
            file.write_all(&offset.to_le_bytes()).unwrap();
        }

        file.write_all(&index_start_offset.to_le_bytes()).unwrap();
        file.sync_data().unwrap();

        Self::open(path)
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        let file = File::open(path).expect("Failed to open SSTable");

        let mmap = unsafe { Mmap::map(&file).expect("Failed to mmap SSTable") };

        let len = mmap.len();
        let mut footer_bytes = [0u8; 4];
        footer_bytes.copy_from_slice(&mmap[len - 4..len]);
        let index_offset = u32::from_le_bytes(footer_bytes) as usize;

        let mut index = Vec::new();
        let mut curr = index_offset;

        while curr < len - 4 {
            // Read Key Length
            let mut k_len_bytes = [0u8; 4];
            k_len_bytes.copy_from_slice(&mmap[curr..curr + 4]);
            let k_len = u32::from_le_bytes(k_len_bytes) as usize;
            curr += 4;

            // Read Key
            let key = mmap[curr..curr + k_len].to_vec();
            curr += k_len;

            // Read Data Offset
            let mut ptr_bytes = [0u8; 4];
            ptr_bytes.copy_from_slice(&mmap[curr..curr + 4]);
            let data_offset = u32::from_le_bytes(ptr_bytes);
            curr += 4;

            index.push((key, data_offset));
        }

        Self { _file: file, mmap, index }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let search_result = self.index.binary_search_by(|(k, _offset)| k.as_slice().cmp(key));

        if let Ok(idx) = search_result {
            let offset = self.index[idx].1 as usize;

            let mut k_len_bytes = [0u8; 4];
            k_len_bytes.copy_from_slice(&self.mmap[offset..offset + 4]);
            let k_len = u32::from_le_bytes(k_len_bytes) as usize;

            let v_len_offset = offset + 4 + k_len;
            let mut v_len_bytes = [0u8; 4];
            v_len_bytes.copy_from_slice(&self.mmap[v_len_offset..v_len_offset + 4]);
            let v_len = u32::from_le_bytes(v_len_bytes) as usize;

            let val_offset = v_len_offset + 4;
            let value = self.mmap[val_offset..val_offset + v_len].to_vec();

            return Some(value);
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_sstable_binary_search_finds_key() {
        let dir = tempdir().expect("Failed to create temp dir");
        let sst_path = dir.path().join("00001.sst");

        let data = vec![
            (b"apple".to_vec(), b"red".to_vec()),
            (b"banana".to_vec(), b"yellow".to_vec()),
            (b"cherry".to_vec(), b"red".to_vec()),
            (b"date".to_vec(), b"brown".to_vec()),
            (b"elderberry".to_vec(), b"purple".to_vec()),
        ];

        let sstable = SsTable::build(&sst_path, data.into_iter());

        assert_eq!(sstable.get(b"banana").unwrap(), b"yellow");
        assert_eq!(sstable.get(b"elderberry").unwrap(), b"purple");

        assert!(sstable.get(b"carrot").is_none());
        assert!(sstable.get(b"zebra").is_none());
    }
}