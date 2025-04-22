use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use crate::pager::{Pager, PAGE_SIZE};
type PageData = Rc<RefCell<[u8; PAGE_SIZE]>>;

pub struct PageCache {
    pager: Pager,
    cache: HashMap<usize, PageData>,
    dirty_flag: HashMap<usize, bool>,
    lru: VecDeque<usize>,
    capacity: usize,
}

impl PageCache {
    pub fn new(pager: Pager, capacity: usize) -> Self {
        Self {
            pager,
            cache: HashMap::new(),
            dirty_flag: HashMap::new(),
            lru: VecDeque::new(),
            capacity,
        }
    }

    fn evict_if_necessary(&mut self) {
        while self.cache.len() > self.capacity {
            if let Some(victim) = self.lru.pop_front() {
                if self.dirty_flag.get(&victim).copied().unwrap_or(false) {
                    self.lru.push_back(victim);
                    continue;
                }
                self.cache.remove(&victim);
                self.dirty_flag.remove(&victim);
            } else {
                break;
            }
        }
    }

    pub fn get_page(&mut self, page_id: usize) -> std::io::Result<PageData> {
        if let Some(page) = self.cache.get(&page_id) {
            if let Some(pos) = self.lru.iter().position(|&id| id == page_id) {
                self.lru.remove(pos);
            }
            self.lru.push_back(page_id);
            return Ok(page.clone());
        }

        let page_data = self.pager.read_page(page_id)?;
        let page_rc = Rc::new(RefCell::new(page_data));
        self.cache.insert(page_id, page_rc.clone());
        self.lru.push_back(page_id);
        self.evict_if_necessary();
        return Ok(page_rc);
    }

    pub fn mark_dirty(&mut self, page_id: usize) {
        self.dirty_flag.insert(page_id, true);
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        for page_id in self.lru.iter() {
            if let Some(page) = self.cache.get(page_id) {
                if let Some(_) = self.dirty_flag.get(page_id) {
                    self.pager.write_page(*page_id, &page.borrow())?;
                    self.dirty_flag.remove(page_id);
                }
            }
        }
        self.dirty_flag.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn setup_cache() -> (PageCache, NamedTempFile) {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let pager = Pager::open(temp.path()).unwrap();
        let cache = PageCache::new(pager, 3); // 減少容量便於測 eviction
        (cache, temp)
    }

    #[test]
    fn test_get_page_returns_zeroed() {
        let (mut cache, _) = setup_cache();
        let page = cache.get_page(1).unwrap();
        assert!(page.borrow().iter().all(|&b| b == 0));
    }

    #[test]
    fn test_page_can_be_modified() {
        let (mut cache, _) = setup_cache();
        let page = cache.get_page(1).unwrap();
        page.borrow_mut()[..4].copy_from_slice(&[9, 8, 7, 6]);
        assert_eq!(&page.borrow()[..4], &[9, 8, 7, 6]);
    }

    #[test]
    fn test_mark_dirty_does_not_panic() {
        let (mut cache, _) = setup_cache();
        cache.mark_dirty(1);
    }

    #[test]
    fn test_flush_writes_to_disk() {
        let (mut cache, path) = setup_cache();
        let page = cache.get_page(1).unwrap();
        page.borrow_mut()[..4].copy_from_slice(&[9, 8, 7, 6]);
        cache.mark_dirty(1);
        cache.flush().unwrap();

        let pager = Pager::open(path).unwrap();
        let page_data = pager.read_page(1).unwrap();
        assert_eq!(&page_data[..4], &[9, 8, 7, 6]);
    }

    #[test]
    fn test_eviction_removes_oldest_clean_page() {
        let (mut cache, _) = setup_cache();
        cache.get_page(1).unwrap();
        cache.get_page(2).unwrap();
        cache.get_page(3).unwrap();
        cache.get_page(4).unwrap(); // 觸發淘汰
        assert!(cache.cache.get(&1).is_none());
    }

    #[test]
    fn test_eviction_skips_dirty_page() {
        let (mut cache, _) = setup_cache();
        cache.get_page(1).unwrap();
        cache.mark_dirty(1);
        cache.get_page(2).unwrap();
        cache.get_page(3).unwrap();
        cache.get_page(4).unwrap(); // 會跳過 page 1，不淘汰 dirty
        assert!(cache.cache.get(&1).is_some());
    }

    #[test]
    fn test_cache_hit_miss_statistics() {
        struct StatsPageCache {
            cache: PageCache,
            hits: usize,
            misses: usize,
        }

        impl StatsPageCache {
            fn new(pager: Pager, capacity: usize) -> Self {
                Self {
                    cache: PageCache::new(pager, capacity),
                    hits: 0,
                    misses: 0,
                }
            }

            fn get_page(&mut self, page_id: usize) -> std::io::Result<PageData> {
                if self.cache.cache.contains_key(&page_id) {
                    self.hits += 1;
                } else {
                    self.misses += 1;
                }
                self.cache.get_page(page_id)
            }
        }

        let temp = NamedTempFile::new().unwrap();
        let pager = Pager::open(temp.path()).unwrap();
        let mut stats_cache = StatsPageCache::new(pager, 3);

        stats_cache.get_page(1).unwrap(); // miss
        stats_cache.get_page(2).unwrap(); // miss
        stats_cache.get_page(1).unwrap(); // hit
        stats_cache.get_page(3).unwrap(); // miss
        stats_cache.get_page(2).unwrap(); // hit

        assert_eq!(stats_cache.hits, 2);
        assert_eq!(stats_cache.misses, 3);
    }
}

