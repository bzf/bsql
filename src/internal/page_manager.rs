use std::fs::File;
use std::io::Write;
use std::rc::Rc;
use std::sync::RwLock;

use super::InternalPage;

pub type PageId = u32;
pub type SharedInternalPage = Rc<RwLock<InternalPage>>;

pub struct PageManager {
    filename: String,

    pages: Vec<SharedInternalPage>,
}

impl PageManager {
    pub fn new(filename: &str) -> Self {
        let mut pages = Vec::new();
        let path = std::path::Path::new(filename);

        if std::path::Path::exists(path) {
            let content = std::fs::read(filename).unwrap_or(vec![]);

            for chunk in content.chunks(8192) {
                let (metadata, data) = chunk.split_at(4096);

                let mut page = InternalPage::new();
                page.metadata.copy_from_slice(&metadata);
                page.data.copy_from_slice(&data);

                pages.push(Rc::new(RwLock::new(page)));
            }
        }

        PageManager {
            filename: filename.to_string(),
            pages,
        }
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
        if self.filename == ":memory:" {
            return;
        }

        println!("PageManager::commit()");
        let mut file = File::create(&self.filename).unwrap();

        for page in &self.pages {
            let page = page.read().unwrap();

            file.write_all(&page.metadata).unwrap();
            file.write_all(&page.data).unwrap();
        }
    }
}
