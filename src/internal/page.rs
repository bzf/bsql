pub type PageData = [u8; 4096];

pub struct InternalPage {
    pub metadadata: PageData,
    pub data: PageData,
}

impl InternalPage {
    pub fn new() -> Self {
        Self {
            metadadata: [0; 4096],
            data: [0; 4096],
        }
    }
}
