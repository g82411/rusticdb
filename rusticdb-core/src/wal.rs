use std::fs::{File, OpenOptions};
use std::path::Path;
use std::io::{Read, Seek, SeekFrom, Write};
use crc32fast::Hasher;

const WAL_MAGIC: u32 = 0xd00dfeed;
const WAL_PAGE_SIZE: usize = 4096;

struct WAL {
    file: File
}

impl WAL {
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let wal = WAL { file };
        Ok(wal)
    }
    pub fn append(&mut self, page_id: usize, data: &[u8]) -> std::io::Result<()> {
        // header 24 byte
        // 4 byte for magic number
        // 4 byte for page id
        // 4 byte for chunk_id
        // 4 byte for total chunks
        // 4 byte for data len 
        // 4 byte for checksum
        let chunk_size = WAL_PAGE_SIZE - 28;
        let total_chunks = (data.len() + chunk_size - 1) / chunk_size;
        for i in 0..total_chunks {
            let offset = i * chunk_size;
            let end = std::cmp::min(offset + chunk_size, data.len());
            let chunk = &data[offset..end];
            let mut buffer = vec![0u8; WAL_PAGE_SIZE];
            buffer[0..4].copy_from_slice(&WAL_MAGIC.to_le_bytes());
            buffer[4..12].copy_from_slice(&(page_id as u64).to_le_bytes());
            buffer[12..16].copy_from_slice(&(i as u32).to_le_bytes());
            buffer[16..20].copy_from_slice(&(total_chunks as u32).to_le_bytes());
            buffer[20..24].copy_from_slice(&(chunk.len() as u32).to_le_bytes());
            buffer[24..24 + chunk.len()].copy_from_slice(chunk);
            
            let mut hasher = Hasher::new();
            hasher.update(chunk);
            let checksum = hasher.finalize();
            buffer[24 + chunk.len()..28 + chunk.len()].copy_from_slice(&checksum.to_le_bytes());
            self.file.write_all(&buffer)?;
        }
        self.file.flush()?;
        Ok(())
    }

    pub fn replay<F: FnMut(usize, Vec<u8>)>(&mut self, mut callback: F) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut page = [0u8; WAL_PAGE_SIZE];
        let mut current_page_id = None;
        let mut expected_chunks = 0;
        let mut collected_chunks: Vec<Vec<u8>> = vec![];

        loop {
            let n = self.file.read(&mut page)?;
            if n == 0 {
                break;
            }

            if n < 28 {
                break;
            }

            let magic = u32::from_le_bytes(page[0..4].try_into().unwrap());
            if magic != WAL_MAGIC {
                break;
            }

            let page_id = u64::from_le_bytes(page[4..12].try_into().unwrap()) as usize;
            let chunk_id = u32::from_le_bytes(page[12..16].try_into().unwrap()) as usize;
            let total_chunks = u32::from_le_bytes(page[16..20].try_into().unwrap()) as usize;
            let data_len = u32::from_le_bytes(page[20..24].try_into().unwrap()) as usize;

            if 24 + data_len + 4 > WAL_PAGE_SIZE {
                break;
            }

            let data = page[24..24 + data_len].to_vec();
            let expected_crc = u32::from_le_bytes(page[24 + data_len..28 + data_len].try_into().unwrap());

            let mut hasher = Hasher::new();
            hasher.update(&data);
            let actual_crc = hasher.finalize();

            if actual_crc != expected_crc {
                break;
            }

            if current_page_id != Some(page_id) {
                current_page_id = Some(page_id);
                expected_chunks = total_chunks;
                collected_chunks = vec![Vec::new(); total_chunks];
            }

            if chunk_id < expected_chunks {
                collected_chunks[chunk_id] = data;
            }

            if collected_chunks.iter().all(|c| !c.is_empty()) {
                let full = collected_chunks.concat();
                callback(page_id, full);
                current_page_id = None;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::fs;

    fn open_temp_wal() -> (WAL, NamedTempFile) {
        let temp = NamedTempFile::new().unwrap();
        let wal = WAL::open(temp.path()).unwrap();
        (wal, temp)
    }

    #[test]
    fn test_single_chunk_append_and_replay() {
        let (mut wal, temp) = open_temp_wal();
        let input = b"Hello WAL!".to_vec();
        wal.append(1, &input).unwrap();

        let mut replayed = Vec::new();
        WAL::open(temp.path()).unwrap().replay(|id, data| {
            replayed.push((id, data));
        }).unwrap();

        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0], (1, input));
    }

    #[test]
    fn test_multi_chunk_append_and_replay() {
        let (mut wal, temp) = open_temp_wal();
        let input = vec![0xAB; WAL_PAGE_SIZE * 2 + 100]; // 多 chunk
        wal.append(42, &input).unwrap();

        let mut replayed = Vec::new();
        WAL::open(temp.path()).unwrap().replay(|id, data| {
            replayed.push((id, data));
        }).unwrap();

        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].0, 42);
        assert_eq!(replayed[0].1, input);
    }

    #[test]
    fn test_crc_corruption_stops_replay() {
        let (mut wal, temp) = open_temp_wal();
        let input = vec![0xCD; 100];
        wal.append(7, &input).unwrap();

        // 人為破壞 CRC
        let mut file = fs::OpenOptions::new().read(true).write(true).open(temp.path()).unwrap();
        let mut buf = [0u8; WAL_PAGE_SIZE];
        file.read_exact(&mut buf).unwrap();
        buf[28] ^= 0xFF; // 搞亂一個位元
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&buf).unwrap();
        file.flush().unwrap();

        let mut replayed = Vec::new();
        WAL::open(temp.path()).unwrap().replay(|id, data| {
            replayed.push((id, data));
        }).unwrap();

        assert_eq!(replayed.len(), 0); // replay 被中止
    }
}