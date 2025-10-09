//! Simple wrapper around VecDeque, that provides automatic removal from the front
//! when the item size exceeds a threshold. See [`SizeLimitVecDeque`] .
use std::collections::VecDeque;

/// Trait for objects that can estimate their size in RAM
pub trait MeasurableSize {
    /// The size in bytes of the object, in memory
    fn size_bytes(&self) -> usize;
}

/// A vector limited in size.
///
/// The total size of all stored items is always less than the limit.
pub struct SizeLimitVecDeque<T> {
    inner: VecDeque<T>,
    size_limit: usize,
    accum_size: usize,
}

impl<T: MeasurableSize> SizeLimitVecDeque<T> {
    /// Create a new instance of SizeLimitVecDeque, with a max size of
    /// `limit_bytes`.
    pub fn new(limit_bytes: usize) -> SizeLimitVecDeque<T> {
        Self {
            inner: VecDeque::new(),
            size_limit: limit_bytes,
            accum_size: 0,
        }
    }

    /// Push the given value. Old values are removed until the item can fit.
    ///
    /// If the item still doesn't fit, the collection will be empty.
    pub fn push(&mut self, value: T) {
        let size = value.size_bytes();
        while self.accum_size + size > self.size_limit {
            let Some(prev) = self.inner.pop_front() else {
                return;
            };
            self.accum_size -= prev.size_bytes();
        }
        self.accum_size += size;
        self.inner.push_back(value);
    }

    /// Push the given value. Old values are removed until the item can fit.
    ///
    /// If the item still doesn't fit, the collection will be empty. If the insert point
    /// is popped off, the value is not inserted.
    pub fn insert(&mut self, mut insert_point: usize, value: T) {
        let size = value.size_bytes();
        while self.accum_size + size > self.size_limit {
            if insert_point == 0 {
                return;
            }
            let Some(prev) = self.inner.pop_front() else {
                return;
            };
            insert_point -= 1;
            self.accum_size -= prev.size_bytes();
        }
        self.accum_size += size;
        self.inner.insert(insert_point, value);
    }

    /// The inner vecdeque wrapped by Self
    pub fn inner(&self) -> &VecDeque<T> {
        &self.inner
    }
    /// Remove element with index 'index'.
    /// Time complexity is O(N) where N i the number
    /// of elements in the collection.
    pub fn remove(&mut self, index: usize) -> Option<T> {
        let item = self.inner.remove(index)?;
        self.accum_size -= item.size_bytes();
        Some(item)
    }
    /// Perform a binary search on the items in the collection.
    ///
    /// This method only provides meaningful results if the collection is sorted
    /// on the key selected.
    ///
    /// See [`std::collections::VecDeque::binary_search_by_key`] for docs.
    pub fn binary_search_by_key<'a, B, F>(&'a self, b: &B, f: F) -> Result<usize, usize>
    where
        F: FnMut(&'a T) -> B,
        B: Ord,
    {
        self.inner.binary_search_by_key(b, f)
    }
}

#[cfg(test)]
mod tests {
    use crate::communication::size_limit_vec_deque::{MeasurableSize, SizeLimitVecDeque};

    #[derive(PartialEq, Debug)]
    struct TestItem(usize);

    impl MeasurableSize for TestItem {
        fn size_bytes(&self) -> usize {
            self.0
        }
    }
    #[test]
    fn simple_limit_test() {
        let mut v = SizeLimitVecDeque::new(10);
        v.push(TestItem(9));
        assert_eq!(v.inner(), &[TestItem(9)]);
        v.push(TestItem(2));
        assert_eq!(v.inner(), &[TestItem(2)]);
        v.push(TestItem(4));
        assert_eq!(v.inner(), &[TestItem(2), TestItem(4)]);
        v.push(TestItem(40));
        assert_eq!(v.inner(), &[]);

        v.push(TestItem(1));
        v.push(TestItem(9));
        assert_eq!(v.inner(), &[TestItem(1), TestItem(9)]);
        v.remove(0);
        assert_eq!(v.inner(), &[TestItem(9)]);
    }

    #[test]
    fn simple_limit_insert_test() {
        let mut v = SizeLimitVecDeque::new(10);
        v.insert(0, TestItem(5));
        v.insert(0, TestItem(4));
        v.insert(0, TestItem(1));
        assert_eq!(v.inner(), &[TestItem(1), TestItem(4), TestItem(5)]);

        v.insert(2, TestItem(3));
        assert_eq!(v.inner(), &[TestItem(3), TestItem(5)]);
    }

    #[test]
    fn simple_limit_insert1_test() {
        let mut v = SizeLimitVecDeque::new(10);
        v.push(TestItem(5));
        v.push(TestItem(4));
        v.push(TestItem(1));
        assert_eq!(v.inner(), &[TestItem(5), TestItem(4), TestItem(1)]);

        v.insert(3, TestItem(10));
        assert_eq!(v.inner(), &[TestItem(10)]);
    }
}
