use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use crc32fast::Hasher;

pub const WAL_MAGIC: u32 = 0xC0DECAFE;
pub const WAL_PAGE_SIZE: usize = 4096;

#[derive(PartialEq, Eq, Clone, Copy)]
enum FrameType {
    Data = 0,
    Checkpoint = 1,
}

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Wal { file })
    }

    pub fn append(&mut self, page_id: usize, data: &[u8]) -> std::io::Result<()> {
        Self::append_internal(&mut self.file, FrameType::Data, Some((page_id, data)))
    }

    pub(crate) fn append_checkpoint(&mut self, last_offset: u64) -> std::io::Result<()> {
        let meta = last_offset.to_le_bytes();
        Self::append_internal(&mut self.file, FrameType::Checkpoint, Some((0, &meta)))
    }

    fn append_internal(file: &mut File, ftype: FrameType, payload: Option<(usize, &[u8])>) -> std::io::Result<()> {
        let (page_id, data) = payload.unwrap_or((0, &[]));
        let chunk_size = WAL_PAGE_SIZE - 29;
        let total_chunks = (data.len() + chunk_size - 1) / chunk_size;

        for i in 0..total_chunks.max(1) {
            let offset = i * chunk_size;
            let end = std::cmp::min(offset + chunk_size, data.len());
            let chunk = &data[offset..end];

            let mut buffer = vec![0u8; WAL_PAGE_SIZE];
            buffer[0] = ftype as u8;
            buffer[1..5].copy_from_slice(&WAL_MAGIC.to_le_bytes());
            buffer[5..13].copy_from_slice(&(page_id as u64).to_le_bytes());
            buffer[13..17].copy_from_slice(&(i as u32).to_le_bytes());
            buffer[17..21].copy_from_slice(&(total_chunks as u32).to_le_bytes());
            buffer[21..25].copy_from_slice(&(chunk.len() as u32).to_le_bytes());
            buffer[25..25 + chunk.len()].copy_from_slice(chunk);

            let mut hasher = Hasher::new();
            hasher.update(chunk);
            let crc = hasher.finalize();
            buffer[25 + chunk.len()..29 + chunk.len()].copy_from_slice(&crc.to_le_bytes());

            file.write_all(&buffer)?;
        }

        file.flush()?;
        Ok(())
    }

    pub fn replay_from_offset<F: FnMut(usize, Vec<u8>)>(
        &mut self,
        offset: u64,
        mut callback: F,
    ) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut page = [0u8; WAL_PAGE_SIZE];
        let mut current_page_id = None;
        let mut expected_chunks = 0;
        let mut collected_chunks: Vec<Vec<u8>> = vec![];

        loop {
            let n = self.file.read(&mut page)?;
            if n == 0 {
                break;
            }
            if n < 29 {
                break;
            }

            let ftype = match page[0] {
                0 => FrameType::Data,
                1 => FrameType::Checkpoint,
                _ => break,
            };

            let magic = u32::from_le_bytes(page[1..5].try_into().unwrap());
            if magic != WAL_MAGIC {
                break;
            }

            let page_id = u64::from_le_bytes(page[5..13].try_into().unwrap()) as usize;
            let chunk_id = u32::from_le_bytes(page[13..17].try_into().unwrap()) as usize;
            let total_chunks = u32::from_le_bytes(page[17..21].try_into().unwrap()) as usize;
            let data_len = u32::from_le_bytes(page[21..25].try_into().unwrap()) as usize;

            if 25 + data_len + 4 > WAL_PAGE_SIZE {
                break;
            }

            let data = page[25..25 + data_len].to_vec();
            let expected_crc = u32::from_le_bytes(page[25 + data_len..29 + data_len].try_into().unwrap());
            let mut hasher = Hasher::new();
            hasher.update(&data);
            let actual_crc = hasher.finalize();
            if actual_crc != expected_crc {
                break;
            }

            match ftype {
                FrameType::Data => {
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
                FrameType::Checkpoint => {
                    // checkpoint frame 可略過或另存處理
                    // 目前只略過
                }
            }
        }

        Ok(())
    }

    pub fn current_offset(&mut self) -> std::io::Result<u64> {
        self.file.seek(SeekFrom::End(0))?;
        Ok(self.file.stream_position()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_append_and_replay_with_checkpoint() {
        let temp = NamedTempFile::new().unwrap();
        let mut wal = Wal::open(temp.path()).unwrap();

        let data1 = b"hello world".to_vec();
        let data2 = vec![42u8; WAL_PAGE_SIZE * 2];

        wal.append(1, &data1).unwrap();
        let mid_offset = wal.current_offset().unwrap();
        wal.append_checkpoint(mid_offset).unwrap();
        wal.append(2, &data2).unwrap();

        let mut wal = Wal::open(temp.path()).unwrap();
        let mut seen = vec![];

        wal.replay_from_offset(0, |pid, data| {
            seen.push((pid, data));
        }).unwrap();

        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].0, 1);
        assert_eq!(seen[0].1, data1);
        assert_eq!(seen[1].0, 2);
        assert_eq!(seen[1].1, data2);
    }
}
