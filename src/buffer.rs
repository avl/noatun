use std::cell::Cell;
use std::slice::SliceIndex;

pub struct DummyMemoryMappedBuffer {
    data: Box<[Cell<u8>]>,
    pointer: Cell<usize>,
}
impl Default for DummyMemoryMappedBuffer {
    fn default() -> Self {
        Self {
            data: vec![Cell::new(0);10_000].into(),
            pointer: 0.into()
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
        self.data.as_ptr() as *const u8
    }

    pub fn allocate<const N: usize, const ALIGN: usize>(&self) -> &[Cell<u8>; N] {
        self.allocate_dyn(N,ALIGN).try_into().unwrap()
    }
    pub fn allocate_dyn(&self, N: usize, ALIGN: usize) -> &[Cell<u8>] {
        if ALIGN > 256 {
            panic!("Noatun arbitrarily does not support types with alignment > 256");
        }

        let aligned_pos = size_rounded_up_to_custom_align(self.pointer.get(), ALIGN).unwrap();
        self.pointer.set(aligned_pos.checked_add(N).unwrap());
        if self.pointer.get() > self.data.len() {
            panic!("Out of memory");
        }
        &self.data[self.pointer.get()-N..self.pointer.get()]
    }
    pub fn access(&self, range: impl SliceIndex<[Cell<u8>], Output = [Cell<u8>]>) -> &[Cell<u8>] {
        &self.data[range]
    }

    pub fn write(&self, index: usize, data: &[u8]) {
        let target = &self.data[index..index+data.len()];
        for (dst,src) in target.iter().zip(data.iter().copied()) {
            dst.set(src);
        }
    }

    pub fn index_of<T>(&self, t: &T) -> usize {
        (t as *const _ as *const u8 as usize)  - (self.data.as_ptr() as *const u8 as usize)
    }
}