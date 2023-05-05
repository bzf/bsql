pub type PageData = [u8; 4096];

pub struct InternalPage {
    pub metadata: PageData,
    pub data: PageData,
}

impl InternalPage {
    pub fn new() -> Self {
        Self {
            metadata: [0; 4096],
            data: [0; 4096],
        }
    }
}
