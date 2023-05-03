pub type PageData = [u8; 4096];

pub struct InternalPage {
    data_page: PageData,
}

impl InternalPage {
    pub fn new() -> Self {
        Self {
            data_page: [0; 4096],
        }
    }

    pub fn data(&self) -> &PageData {
        &self.data_page
    }

    pub fn data_mut(&mut self) -> &mut PageData {
        &mut self.data_page
    }
}
