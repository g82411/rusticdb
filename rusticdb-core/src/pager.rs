use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

pub const PAGE_SIZE: usize = 4096;

pub struct Pager {
    file: Mutex<File>,
}

impl Pager {
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(Pager {
            file: Mutex::new(file),
        })
    }

    pub fn read_page(&self, page_id: usize) -> std::io::Result<[u8; PAGE_SIZE]> {
        let mut file = self.file.lock().unwrap();
        let offset = (page_id * PAGE_SIZE) as u64;
        file.seek(SeekFrom::Start(offset))?;

        let mut buffer = [0u8; PAGE_SIZE];
        let _ = file.read(&mut buffer)?;  // 容忍讀不到這麼多
        Ok(buffer)
    }

    pub fn write_page(&self, page_id: usize, data: &[u8; PAGE_SIZE]) -> std::io::Result<()> {
        let mut file = self.file.lock().unwrap();
        let offset = (page_id * PAGE_SIZE) as u64;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        file.flush()?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use assert_matches::assert_matches;

    #[test]
    fn test_write_page_success() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path();

        let pager = Pager::open(path).unwrap();
        let data = [42u8; PAGE_SIZE];

        let result = pager.write_page(0, &data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_read_page_content_correct() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path();

        {
            let pager = Pager::open(path).unwrap();
            let mut buf = [0u8; PAGE_SIZE];
            buf[..4].copy_from_slice(&[1, 2, 3, 4]);
            pager.write_page(0, &buf).unwrap();
        }

        {
            let pager = Pager::open(path).unwrap();
            let page = pager.read_page(0).unwrap();
            assert_eq!(&page[..4], &[1, 2, 3, 4]);
        }
    }

    #[test]
    fn test_read_uninitialized_page_zero_filled() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path();

        let pager = Pager::open(path).unwrap();
        let page = pager.read_page(9999).unwrap();

        assert!(page.iter().all(|&b| b == 0));
    }
}