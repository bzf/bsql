pub type PageData = [u8; 4096];

pub struct InternalPage {
    pub data: PageData,
}

impl InternalPage {
    pub fn new() -> Self {
        Self { data: [0; 4096] }
    }
}
