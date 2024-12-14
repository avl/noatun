use std::cell::Cell;
use std::ops::{Range, RangeBounds};
use std::slice::SliceIndex;
use bytemuck::{from_bytes, from_bytes_mut, Pod};

pub struct DummyMemoryMappedBuffer {
    data: *mut u8,
    data_len: usize,
    pointer: Cell<usize>,
}
impl Default for DummyMemoryMappedBuffer {
    fn default() -> Self {
        let boxed : Box<[u8]> = vec![0; 10_000].into();

        let len = boxed.len();
        let raw_ptr = Box::into_raw(boxed);
        Self {
            data: raw_ptr as *mut u8,
            data_len: len,
            pointer: 0.into(),
        }
    }
}

// This has been shamelessly lifted from the rust std
#[inline]
fn size_rounded_up_to_custom_align(curr: usize, align: usize) -> Option<usize> {
    // SAFETY:
    // Rounded up value is:
    //   size_rounded_up = (size + align - 1) & !(align - 1);
    //
    // The arithmetic we do here can never overflow:
    //
    // 1. align is guaranteed to be > 0, so align - 1 is always
    //    valid.
    //
    // 2. size is at most `isize::MAX`, so adding `align - 1` (which is at
    //    most `isize::MAX`) can never overflow a `usize`.
    //
    // 3. masking by the alignment can remove at most `align - 1`,
    //    which is what we just added, thus the value we return is never
    //    less than the original `size`.
    //
    // (Size 0 Align MAX is already aligned, so stays the same, but things like
    // Size 1 Align MAX or Size isize::MAX Align 2 round up to `isize::MAX + 1`.)
    unsafe {
        let align_m1 = align - 1;
        let size_rounded_up = (curr.checked_add(align_m1)?) & !align_m1;
        Some(size_rounded_up)
    }
}

impl DummyMemoryMappedBuffer {
    pub fn pointer(&self) -> usize {
        self.pointer.get()
    }

    pub fn start_ptr(&self) -> *const u8 {
        self.data
    }

    pub unsafe fn allocate_pod<'a, T:Pod>(&self) -> &'a mut T {
        let bytes = self.allocate_raw(
            std::mem::size_of::<T>(),
            std::mem::align_of::<T>()
        );
        unsafe { &mut *(bytes as *mut T) }
    }
    pub fn allocate_raw(&self, N: usize, ALIGN: usize) -> *mut u8 {
        if ALIGN > 256 {
            panic!("Noatun arbitrarily does not support types with alignment > 256");
        }

        let aligned_pos = size_rounded_up_to_custom_align(self.pointer.get(), ALIGN).unwrap();
        self.pointer.set(aligned_pos.checked_add(N).unwrap());
        if self.pointer.get() > self.data_len {
            panic!("Out of memory");
        }
        unsafe {self.data.wrapping_add(self.pointer.get()-N) }
    }
    pub fn allocate<const N: usize, const ALIGN: usize>(&self) -> &mut [u8; N] {
        self.allocate_dyn(N, ALIGN).try_into().unwrap()
    }
    pub fn allocate_dyn(&self, N: usize, ALIGN: usize) -> &mut [u8] {
        if ALIGN > 256 {
            panic!("Noatun arbitrarily does not support types with alignment > 256");
        }

        let aligned_pos = size_rounded_up_to_custom_align(self.pointer.get(), ALIGN).unwrap();
        self.pointer.set(aligned_pos.checked_add(N).unwrap());
        if self.pointer.get() > self.data_len {
            panic!("Out of memory");
        }
        unsafe { std::slice::from_raw_parts_mut(self.data.wrapping_add(self.pointer.get()-N), N) }
    }
    pub unsafe fn access(&self, range: Range<usize>) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data.wrapping_add(range.start), range.end - range.start) }
    }
    pub unsafe fn access_mut(&self, range: Range<usize>) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data.wrapping_add(range.start), range.end - range.start) }
    }

    pub fn write(&self, index: usize, data: &[u8]) {
        debug_assert!(index+data.len() <= self.data_len);
        let target = unsafe { self.access_mut(index..index+data.len()) };
        target.copy_from_slice(data);
    }
}
