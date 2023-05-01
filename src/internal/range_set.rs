use std::{collections::HashSet, ops::Range};

#[derive(Debug)]
pub struct RangeSet {
    available: HashSet<u8>,
    used: HashSet<u8>,
}

impl RangeSet {
    pub fn new(range: Range<u8>) -> Self {
        let mut available = HashSet::new();

        for i in range {
            available.insert(i);
        }

        Self {
            available,
            used: HashSet::new(),
        }
    }

    pub fn contains(&self, value: u8) -> bool {
        self.used.contains(&value)
    }

    pub fn consume(&mut self) -> Option<u8> {
        let available_key = self.available.iter().next()?.clone();
        self.insert(available_key);

        return Some(available_key);
    }

    pub fn insert(&mut self, value: u8) -> bool {
        if self.available.contains(&value) {
            self.available.remove(&value);
            self.used.insert(value);
            return true;
        } else {
            return false;
        }
    }

    pub fn keys(&self) -> Vec<u8> {
        self.used.iter().cloned().collect()
    }

    pub fn remove(&mut self, value: u8) {
        self.used.remove(&value);
        self.available.insert(value);
    }

    pub fn available(&self) -> usize {
        self.available.len()
    }

    pub fn count(&self) -> usize {
        self.used.len()
    }

    pub fn is_full(&self) -> bool {
        self.available.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_that_it_works() {
        let mut range_set = RangeSet::new(0..2);

        assert_eq!(false, range_set.contains(0));

        assert_eq!(2, range_set.available());
        assert_eq!(0, range_set.count());
        assert!(!range_set.is_full());

        assert!(range_set.insert(0));
        assert_eq!(1, range_set.available());
        assert_eq!(1, range_set.count());
        assert!(!range_set.is_full());

        assert!(range_set.contains(0));

        assert!(range_set.insert(1));
        assert_eq!(0, range_set.available());
        assert_eq!(2, range_set.count());
        assert!(range_set.is_full());

        // Fails to insert a duplicate value
        assert_eq!(false, range_set.insert(1));

        // Fails to insert a value out-of-bounds
        assert_eq!(false, range_set.insert(3));
    }

    #[test]
    fn test_consume_return_available_value() {
        let mut range_set = RangeSet::new(0..2);

        assert!(range_set.consume().is_some());
        assert!(range_set.consume().is_some());
        assert!(range_set.consume().is_none());
    }
}
