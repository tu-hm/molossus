use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};

pub struct Wal {
    path: PathBuf,
    file: BufWriter<File>,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> Self {
        let path_buf = path.as_ref().to_path_buf();

        let file = OpenOptions::new().create(true).append(true).open(path).expect("cannot open file");

        Self {
            path: path_buf,
            file: BufWriter::new(file),
        }
    }

    pub fn append(&mut self, key: &[u8], value: &[u8]) {
        let key_len = key.len() as u32;
        self.file.write_all(&key_len.to_le_bytes()).unwrap();
        self.file.write_all(key).unwrap();

        let val_len = value.len() as u32;
        self.file.write_all(&val_len.to_le_bytes()).unwrap();
        self.file.write_all(value).unwrap();

        self.file.flush().unwrap();
        self.file.get_ref().sync_data().unwrap();

    }

    pub fn recover(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let file = File::open(&self.path).expect("cannot open file");
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut len_buf = [0u8; 4];
            if reader.read_exact(&mut len_buf).is_err() {
                break;
            }

            let key_len = u32::from_le_bytes(len_buf);
            let mut key = vec![0u8; key_len as usize];
            reader.read_exact(&mut key).unwrap();

            reader.read_exact(&mut len_buf).unwrap();

            let val_len = u32::from_le_bytes(len_buf) as usize;
            let mut value = vec![0u8; val_len as usize];
            reader.read_exact(&mut value).unwrap();

            entries.push((key, value));
        }

        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_recovers_state_after_crash() {
        let dir = tempdir().expect("cannot create temp dir");
        let wal_path = dir.path().join("wal");

        let mut wal = Wal::open(&wal_path);
        wal.append(b"key1", b"value1");
        wal.append(b"key2", b"value2");

        drop(wal);

        let recover_wal = Wal::open(&wal_path);
        let recover_data = recover_wal.recover();

        assert_eq!(recover_data.len(), 2);
        assert_eq!(recover_data[0], (b"key1".to_vec(), b"value1".to_vec()));
        assert_eq!(recover_data[1], (b"key2".to_vec(), b"value2".to_vec()));
    }
}

