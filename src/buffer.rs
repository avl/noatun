use crate::{FatPtr, FixedSizeObject, GenPtr, Object, Pointer, ThinPtr};
use bytemuck::{Pod, from_bytes, from_bytes_mut, bytes_of};
use std::alloc::Layout;
use std::any::{Any, TypeId};
use std::cell::Cell;
use std::mem::transmute_copy;
use std::ops::Range;
use std::slice;
use std::slice::SliceIndex;
use crate::undo_store::{HowToProceed, UndoLog, UndoLogEntry};

pub struct DatabaseContext {
    data: *mut u8,
    data_len: usize,
    pointer: Cell<usize>,
    root_index: Option<GenPtr>,

    time: u64,
    undo_log: UndoLog
}
impl Default for DatabaseContext {
    fn default() -> Self {
        let layout = Layout::from_size_align(10000, 256).unwrap();

        let data = unsafe { std::alloc::alloc_zeroed(layout) };
        Self {
            data,
            data_len: 10000,
            pointer: 0.into(),
            root_index: None,
            time: 0,
            undo_log: UndoLog::new(),
        }
    }
}
impl Drop for DatabaseContext {
    fn drop(&mut self) {
        /*let r = unsafe { slice::from_raw_parts_mut(self.data, self.data_len) };
        let p = r as *mut _;*/
        //let _: Box<[u8]> = unsafe { Box::from_raw(self.data_orig) };
        let layout = Layout::from_size_align(10000, 256).unwrap();
        unsafe { std::alloc::dealloc(self.data, layout) }
    }
}

// This has been shamelessly lifted from the rust std
#[inline]
fn index_rounded_up_to_custom_align(curr: usize, align: usize) -> Option<usize> {
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


impl DatabaseContext {

    pub fn rewind(&mut self, new_time: u64) {

        let result = self.undo_log.rewind(|entry|{
            println!("Parsed entry: {:?}", entry);
            match entry {
                UndoLogEntry::SetPointer(new_pointer) => {
                    let cur = self.pointer.get();
                    debug_assert!(new_pointer <= cur);
                    unsafe { Self::mut_slice(self.data, new_pointer..cur).fill(0) };
                    self.pointer.set(new_pointer);
                    HowToProceed::Proceed
                }
                UndoLogEntry::ZeroOut { start, len } => {
                    unsafe { Self::mut_slice(self.data, start..start+len).fill(0) };
                    HowToProceed::Proceed
                }
                UndoLogEntry::Restore { start, data } => {
                    unsafe { Self::mut_slice(self.data, start..start+data.len()).copy_from_slice(data)};
                    HowToProceed::Proceed
                }
                UndoLogEntry::Rewind(time) => {
                    if time == new_time {
                        self.time = new_time;
                        HowToProceed::PutBackAndStop
                    } else if time > new_time {
                        self.time = new_time;
                        HowToProceed::Proceed
                    } else {
                        panic!("Couldn't rewind time to {}, ended up back at {}", time, new_time);
                    }
                }
            }
        });
        if !result {
            panic!("Rewind failed");
        }

    }

    pub fn pointer(&self) -> usize {
        self.pointer.get()
    }

    pub fn start_ptr(&self) -> *const u8 {
        self.data
    }

    pub fn set_time(&mut self, time: u64) {
        if time != self.time {
            self.undo_log.record(UndoLogEntry::Rewind(time));
        }
        self.time = time;
    }

    pub fn set_root_ptr(&mut self, genptr: GenPtr)  {
        self.root_index = Some(genptr);
    }

    pub fn get_root_ptr<Ptr:Pointer+Any+'static>(&self) -> Ptr {
        match self.root_index {
            Some(GenPtr::Thin(ptr)) => {
                if TypeId::of::<Ptr>()  == TypeId::of::<ThinPtr>() {
                    return unsafe  { transmute_copy(&ptr) };
                }
                panic!("Wrong type of root pointer in database. Has schema changed significantly since last access?");
            }
            Some(GenPtr::Fat(ptr)) => {
                if TypeId::of::<Ptr>()  == TypeId::of::<FatPtr>() {
                    return unsafe  { transmute_copy(&ptr) };
                }
                panic!("Wrong type of root pointer in database. Has schema changed significantly since last access?");
            }
            None => {
                panic!("Unknown root pointer");
            }
        }
    }


    pub fn copy(&mut self, source: FatPtr, dest_index: usize) {

        unsafe {
            dbg!(&source, &dest_index);

            self.undo_log.record(UndoLogEntry::Restore {
                start: dest_index,
                data: self.access(FatPtr::from(dest_index, source.len)),
            });

            let dest = self.access_mut(FatPtr {
                start: dest_index,
                len: source.len,
            });

            let src = self.access(source);

            dest.copy_from_slice(src);
        }
    }

    pub unsafe fn allocate_pod<'a, T: Pod>(&mut self) -> &'a mut T {
        let bytes = self.allocate_raw(std::mem::size_of::<T>(), std::mem::align_of::<T>());
        unsafe { &mut *(bytes as *mut T) }
    }
    pub fn allocate_raw(&mut self, size: usize, align: usize) -> *mut u8 {
        if align > 256 {
            panic!("Noatun arbitrarily does not support types with alignment > 256");
        }

        let alignment_adjustment = index_rounded_up_to_custom_align(self.pointer.get(), align).unwrap();
        self.undo_log.record(UndoLogEntry::SetPointer(
            self.pointer.get()
        ));
        self.pointer.set(alignment_adjustment.checked_add(size).unwrap());
        if self.pointer.get() > self.data_len {
            panic!("Out of memory");
        }
        unsafe { self.data.wrapping_add(self.pointer.get() - size) }
    }
    pub fn allocate_array<const N: usize, const ALIGN: usize>(&mut self) -> &mut [u8; N] {
        self.allocate_slice(N, ALIGN).try_into().unwrap()
    }
    pub fn allocate_slice(&mut self, size: usize, align: usize) -> &mut [u8] {
        let start = self.allocate_raw(size, align);
        unsafe { std::slice::from_raw_parts_mut(start, size) }
    }
    pub unsafe fn access<'a>(&self, range: FatPtr) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.data.wrapping_add(range.start), range.len) }
    }
    pub unsafe fn access_mut<'a>(&mut self, range: FatPtr) -> &'a mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data.wrapping_add(range.start), range.len) }
    }
    pub unsafe fn mut_slice<'a>(data: *mut u8, range: Range<usize>) -> &'a mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(data.wrapping_add(range.start), range.end-range.start) }
    }
    pub unsafe fn access_pod<'a, T: Pod>(&self, index: usize) -> &'a T {
        unsafe {
            from_bytes(std::slice::from_raw_parts(
                self.data.wrapping_add(index),
                size_of::<T>(),
            ))
        }
    }
    pub unsafe fn access_pod_mut<'a, T: Pod>(&self, index: ThinPtr) -> &'a mut T {
        unsafe {
            from_bytes_mut(std::slice::from_raw_parts_mut(
                self.data.wrapping_add(index.0),
                size_of::<T>(),
            ))
        }
    }

    pub fn write(&mut self, index: usize, data: &[u8]) {
        debug_assert!(index + data.len() <= self.data_len);
        let fat = FatPtr {
            start: index,
            len: data.len(),
        };
        let target = unsafe { self.access_mut(fat) };
        target.copy_from_slice(data);
    }
    pub fn write_pod<T:Pod>(&mut self, src: T, dest: &mut T) {
        let dest_index = self.index_of_sized(dest);

        self.undo_log.record(UndoLogEntry::Restore {
            start: dest_index.0,
            data: bytes_of(dest),
        });
        *dest = src;
    }
    pub fn index_of_sized<T:Sized>(&self, t: &T) -> ThinPtr
    {
        ThinPtr::create(t, self.data as *const u8)
    }
    pub fn index_of<T:Object>(&self, t: &T) -> T::Ptr
    {
        T::Ptr::create(t, self.data)
    }
    pub fn index_of_ptr<T>(&self, t: *const T) -> ThinPtr {
        ThinPtr((t as *const u8 as usize) - (self.data as usize))
    }
}
