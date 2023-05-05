use std::cell::RefCell;

#[derive(Debug)]
pub struct BitmapIndex<const N: usize>
where
    [(); N / 8 + 1]:,
{
    bitmap: RefCell<[u8; N / 8 + 1]>,
}

impl<const N: usize> BitmapIndex<N>
where
    [(); N / 8 + 1]:,
{
    pub fn from_raw(bytes: RefCell<[u8; N / 8 + 1]>) -> Option<Self> {
        if bytes.borrow().len() == { N / 8 + 1 } {
            Some(Self { bitmap: bytes })
        } else {
            None
        }
    }

    /// Find the first available flag, set it and return its index.
    pub fn consume(&mut self) -> Option<u8> {
        let index = self.find_available_index()?;
        self.set(index);

        return Some(index);
    }

    pub fn set(&mut self, index: u8) {
        if index <= N as u8 {
            self.bitmap.borrow_mut()[(index / 8) as usize] |= 1 << (index % 8);
        }
    }

    pub fn unset(&mut self, index: u8) {
        if index <= N as u8 {
            self.bitmap.borrow_mut()[(index / 8) as usize] &= 0 << (index % 8);
        }
    }

    pub fn is_set(&self, index: u8) -> bool {
        if index <= N as u8 {
            self.bitmap.borrow()[(index / 8) as usize] & (1 << (index % 8)) != 0
        } else {
            false
        }
    }

    pub fn is_full(&self) -> bool {
        self.available() == 0
    }

    /// Returns the number of unset bits.
    pub fn available(&self) -> u8 {
        N as u8 - self.count()
    }

    /// Returns the number of set bits.
    pub fn count(&self) -> u8 {
        self.bitmap
            .borrow()
            .iter()
            .map(|byte| byte.count_ones() as u8)
            .sum()
    }

    /// Returns a `Vec<u8>` with all bits that are set.
    pub fn indices(&self) -> Vec<u8> {
        (0..=255).filter(|index| self.is_set(*index)).collect()
    }

    fn find_available_index(&self) -> Option<u8> {
        for (index, byte) in self.bitmap.borrow().iter().enumerate() {
            if *byte == u8::MAX {
                continue;
            } else {
                let first_free_index = (!byte).trailing_zeros() as u8;
                return Some(index as u8 * 8 + first_free_index);
            }
        }

        return None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_that_it_works() {
        let array = RefCell::new([0; 32]);
        let mut bitmap_index: BitmapIndex<255> =
            BitmapIndex::from_raw(array).expect("Failed to build BitmapIndex");

        assert_eq!(false, bitmap_index.is_set(0));

        assert_eq!(255, bitmap_index.available());
        assert_eq!(0, bitmap_index.count());
        assert!(!bitmap_index.is_full());

        bitmap_index.set(0);
        assert_eq!(254, bitmap_index.available());
        assert_eq!(1, bitmap_index.count());
        assert!(!bitmap_index.is_full());

        assert!(bitmap_index.is_set(0));

        bitmap_index.set(1);
        assert_eq!(253, bitmap_index.available());
        assert_eq!(2, bitmap_index.count());
    }

    #[test]
    fn test_that_unset_works() {
        let array = RefCell::new([0; 32]);
        let mut bitmap_index: BitmapIndex<255> =
            BitmapIndex::from_raw(array).expect("Failed to build BitmapIndex");

        assert_eq!(false, bitmap_index.is_set(0));

        bitmap_index.set(0);
        assert_eq!(true, bitmap_index.is_set(0));

        bitmap_index.unset(0);
        assert_eq!(false, bitmap_index.is_set(0));
    }

    #[test]
    fn test_returns_all_set_bit_indices() {
        let array = RefCell::new([0; 32]);
        let mut bitmap_index: BitmapIndex<255> =
            BitmapIndex::from_raw(array).expect("Failed to build BitmapIndex");

        assert!(bitmap_index.indices().is_empty());

        bitmap_index.set(0);
        assert_eq!(vec![0], bitmap_index.indices());

        bitmap_index.set(13);
        assert_eq!(vec![0, 13], bitmap_index.indices());

        bitmap_index.unset(0);
        assert_eq!(vec![13], bitmap_index.indices());
    }

    #[test]
    fn test_does_nothing_when_setting_the_same_bit_twice() {
        let array = RefCell::new([0; 32]);
        let mut bitmap_index: BitmapIndex<255> =
            BitmapIndex::from_raw(array).expect("Failed to build BitmapIndex");
        assert_eq!(false, bitmap_index.is_set(0));

        bitmap_index.set(0);
        assert!(bitmap_index.is_set(0));

        bitmap_index.set(0);
        assert!(bitmap_index.is_set(0));
    }

    #[test]
    fn test_consume_finds_available_slots_in_the_middle_of_the_index() {
        let array = RefCell::new([0; 32]);
        let mut bitmap_index: BitmapIndex<255> =
            BitmapIndex::from_raw(array).expect("Failed to build BitmapIndex");
        (0..=128).for_each(|index| {
            bitmap_index.set(index.into());
        });
        (130..255).for_each(|index| {
            bitmap_index.set(index.into());
        });

        assert_eq!(1, bitmap_index.available());
        assert_eq!(Some(129), bitmap_index.consume());
    }

    #[test]
    fn test_consume_return_available_value() {
        let array = RefCell::new([0; 32]);
        let mut bitmap_index: BitmapIndex<255> =
            BitmapIndex::from_raw(array).expect("Failed to build BitmapIndex");

        for _ in 0..=u8::MAX {
            assert!(bitmap_index.consume().is_some());
        }

        assert!(bitmap_index.consume().is_none());
    }

    #[test]
    fn test_from_raw_works() {
        let raw_data = RefCell::new([0; 32]);
        raw_data.borrow_mut()[2] = 0xFF;
        let bitmap_index: BitmapIndex<255> =
            BitmapIndex::from_raw(raw_data).expect("Failed to load from bytes");

        assert_eq!(8, bitmap_index.count());
    }
}
