use std::fs::File;
use std::io::Write;
use std::rc::Rc;
use std::sync::RwLock;

use super::InternalPage;

pub type PageId = u32;
pub type SharedInternalPage = Rc<RwLock<InternalPage>>;

const FILENAME: &str = "bsql.db";

pub struct PageManager {
    pages: Vec<SharedInternalPage>,
}

impl PageManager {
    pub fn new() -> Self {
        PageManager { pages: Vec::new() }
    }

    /// Creates a new pages and returns its page_id and the page itself.
    pub fn create_page(&mut self) -> (PageId, SharedInternalPage) {
        let page = Rc::new(RwLock::new(InternalPage::new()));
        let page_id = self.pages.len();
        self.pages.push(page.clone());

        return (page_id as u32, page);
    }

    /// Returns the page if it exists.
    pub fn fetch_page(&self, page_id: PageId) -> Option<SharedInternalPage> {
        self.pages.get(page_id as usize).cloned()
    }

    /// Write the pages to a disk on file.
    pub fn commit(&self) {
        println!("PageManager::commit()");
        let mut file = File::create(FILENAME).unwrap();

        for page in &self.pages {
            let page = page.read().unwrap();

            file.write_all(&page.metadata).unwrap();
            file.write_all(&page.data).unwrap();
        }
    }
}
