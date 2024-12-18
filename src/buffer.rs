use crate::undo_store::{HowToProceed, UndoLog, UndoLogEntry};
use crate::{
    Application, FatPtr, FixedSizeObject, GenPtr, Message, MessageId, Object, Pointer, SequenceNr,
    ThinPtr,
};
use bumpalo::Bump;
use bytemuck::{Pod, bytes_of, from_bytes, from_bytes_mut};
use indexmap::{IndexMap, IndexSet};
use std::alloc::Layout;
use std::any::{Any, TypeId};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::transmute_copy;
use std::ops::Range;
use std::slice;
use std::slice::SliceIndex;

#[derive(Default, Debug)]
struct RegistrarTracker {
    uses: Vec<u32>,
    /// Messages are added to this list when their
    /// last registrar_point is overwritten
    /// Such messages are candidates to be removed, but only if
    /// no other message (that isn't also to be removed), depend on it.
    unused_messages: Vec<SequenceNr>,

    /// Mapping from message-id to other messages that have read output
    /// from said message
    message_dependencies: IndexMap<SequenceNr, Vec<SequenceNr>>,
}

mod message_dependency_tracker {
    use std::fs::{File, OpenOptions};
    use std::iter;
    use memmap2::{Mmap, MmapMut};
    use anyhow::Result;
    use bytemuck::{Pod, Zeroable};
    use crate::SequenceNr;

    // Mapping from observee to observers.
    struct MessageDependencyTracker {
        /// the sequencenr that has been observed
        keys: MmapMut,
        key_capacity: usize,
        /// the sequence nr that have observed each key. I.e, the messages that
        /// are dependent upon the key.
        vals: MmapMut,
        value_capacity: usize,
        key_file: File,
        value_file: File,
    }

    #[derive(Debug,Clone,Copy,Pod,Zeroable)]
    #[repr(C)]
    struct LinkedListEntry {
        next_lsb: u32,
        next_msb: u32,
        seq: SequenceNr,
    }

    impl LinkedListEntry {
        fn set_next(&mut self, next: u64) {
            self.next_lsb = next as u32;
            self.next_msb = (next >>32) as u32;
        }
        fn get_next(&self) -> u64 {
            self.next_lsb as u64 | ((self.next_msb as u64) << 32)
        }
    }

    impl MessageDependencyTracker {

        #[inline]
        fn get_count(mmap: &MmapMut) -> usize {
            *bytemuck::from_bytes::<u64>(&mmap[0..size_of::<u64>()]) as usize
        }
        #[inline]
        fn access<T:Pod>(mmap: &mut MmapMut) -> (&mut u64, &mut [T]) {
            let slice_bytes:&mut [u8] = mmap;
            //println!("Mmap size: {}, item size: {}", slice_bytes.len(), size_of::<T>());
            let (slice_a, slice_b) = slice_bytes.split_at_mut(std::mem::size_of::<u64>());
            let count: &mut u64 = bytemuck::from_bytes_mut(slice_a);
            let slice: &mut [T] = bytemuck::cast_slice_mut(slice_b);
            //println!("Couint: {}, cap: {}", count, slice.len());
            (count, slice)
        }


        // Record mapping from observee to observer
        pub fn record_dependency(&mut self, observee: SequenceNr, observer: SequenceNr ) {
            assert!(observer.is_valid());
            assert!(observee.is_valid());

            //println!("Observee index {}, cap: {}", observee.index(), self.key_capacity);
            if observee.index() >= self.key_capacity {
                //println!("REALLOC!");
                self.reallocate_keys((observee.index()+1)*2);
            }

            //dbg!(Self::get_count(&self.vals) + 1, self.value_capacity);
            if Self::get_count(&self.vals) + 1 >= self.value_capacity {
                //println!("REALLOC!");
                self.reallocate_values((self.value_capacity+1)*2);
            }

            let (val_len, vals) : (_ , &mut [LinkedListEntry]) = Self::access(&mut self.vals);
            let (key_len, keys) : (_, &mut [u64])= Self::access(&mut self.keys);
            debug_assert_eq!(keys.len(), self.key_capacity as usize);
            debug_assert_eq!(vals.len(), self.value_capacity as usize);

            let prev = keys[observee.index()];

            let new_entry: &mut LinkedListEntry = &mut vals[*val_len as usize];
            new_entry.seq = observer;
            new_entry.set_next(prev);
            keys[observee.index()] = *val_len + 1;

            *val_len += 1;
            /*println!("Final state:\n{:?} keys: {:?}\n{:?} values: {:?}",
                key_len, keys,
                val_len, vals,
            )*/
        }
        pub fn read_dependency(&mut self, observee: SequenceNr) -> impl Iterator<Item=SequenceNr> {
            let (key_len, keys) : (_, &mut [u64])= Self::access(&mut self.keys);
            let (val_len, vals) : (_ , &mut [LinkedListEntry])= Self::access(&mut self.vals);
            debug_assert_eq!(keys.len(), self.key_capacity as usize);
            debug_assert_eq!(vals.len(), self.value_capacity as usize);

            let mut cur: u64 = keys[observee.index()];

            iter::from_fn(move||{
                if cur == 0 {
                    return None;
                }
                let entry = &vals[cur as usize - 1];
                cur = entry.get_next();
                return Some(entry.seq);

            })
        }

        fn calc_needed_bytes_keys(count: usize) -> usize {
            (std::mem::size_of::<u64>()) * count +std::mem::size_of::<u64>()
        }
        fn calc_needed_bytes_vals(count: usize) -> usize {
            (std::mem::size_of::<u64>() + size_of::<SequenceNr>()) * count +std::mem::size_of::<u64>()
        }

        fn reallocate_keys(&mut self, new_count: usize) -> Result<()> {
            //println!("Reallocating keys to {}", new_count);
            Self::reallocate(&mut self.keys, &mut self.key_file, Self::calc_needed_bytes_keys(new_count))?;
            self.key_capacity = new_count;
            Ok(())
        }
        fn reallocate_values(&mut self, new_count: usize) -> Result<()>  {
            //println!("Reallocating values to {}", new_count);
            Self::reallocate(&mut self.vals, &mut self.value_file, Self::calc_needed_bytes_vals(new_count))?;
            self.value_capacity = new_count;
            Ok(())
        }

        fn reallocate(mmap: &mut MmapMut, file: &File, new_bytes: usize) -> Result<()> {
            file.set_len(new_bytes as u64)?;
            let new_mmap = unsafe { MmapMut::map_mut(file)? };
            *mmap = new_mmap;
            Ok(())
        }

        pub fn new(path: impl AsRef<std::path::Path>) -> Result<Self> {
            std::fs::create_dir_all(path.as_ref());
            let key_path = path.as_ref().join("dep_keys.bin");
            let value_path = path.as_ref().join("dep_values.bin");
            let key_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&key_path)?;
            let value_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&value_path)?;

            const DEFAULT_KEY_CAPACITY: usize = 3;
            const DEFAULT_VALUE_CAPACITY: usize = 3;

            let key_size_bytes = Self::calc_needed_bytes_keys(DEFAULT_KEY_CAPACITY);
            let value_size_bytes = Self::calc_needed_bytes_vals(DEFAULT_VALUE_CAPACITY);


            //println!("Setting lens: {} {}", key_size_bytes, value_size_bytes);
            key_file.set_len(key_size_bytes as u64)?;
            value_file.set_len(value_size_bytes as u64)?;

            let key_mmap = unsafe { MmapMut::map_mut(&key_file)? };
            let value_mmap = unsafe { MmapMut::map_mut(&value_file)? };

            Ok(MessageDependencyTracker {
                key_file,
                value_file,
                key_capacity:DEFAULT_KEY_CAPACITY,
                keys: key_mmap,
                value_capacity: DEFAULT_VALUE_CAPACITY,
                vals: value_mmap
            })
        }
    }
    #[cfg(test)]
    mod tests {
        use std::time::Instant;
        use crate::buffer::message_dependency_tracker::MessageDependencyTracker;
        use crate::SequenceNr;

        #[test]
        fn smoke_deptrack() {
            std::fs::remove_dir_all("test_smoke_deptrack");
            let mut tracker = MessageDependencyTracker::new("test_smoke_deptrack").unwrap();
            tracker.record_dependency(SequenceNr::from_index(1), SequenceNr::from_index(2));

            let result: Vec<_> = tracker.read_dependency(SequenceNr::from_index(1)).collect();
            assert_eq!(result, vec![SequenceNr::from_index(2)]);
        }

        #[test]
        fn smoke_deptrack_many() {
            std::fs::remove_dir_all("test_smoke_deptrack");
            let mut tracker = MessageDependencyTracker::new("test_smoke_deptrack").unwrap();
            let t = Instant::now();
            for i in 0..1000000 {
                tracker.record_dependency(SequenceNr::from_index(i/2), SequenceNr::from_index(i));
            }
            println!("Time: {:?}", t.elapsed());

            let result: Vec<_> = tracker.read_dependency(SequenceNr::from_index(0)).map(|x|x.index()).collect();
            assert_eq!(result, vec![1,0]);
            let result: Vec<_> = tracker.read_dependency(SequenceNr::from_index(4)).map(|x|x.index()).collect();
            assert_eq!(result, vec![9,8]);
        }
    }

}

impl RegistrarTracker {
    fn finalize_message(&mut self, message_id: SequenceNr) {
        debug_assert_ne!(message_id.0, 0);
        if self.uses.len() <= message_id.index() as usize || self.uses[message_id.index()] == 0 {
            self.unused_messages.push(message_id);
        }
    }

    fn finalize_transaction<M: Message+Debug>(
        &mut self,
        messages: &IndexMap<MessageId, Option<M>>,
    ) -> IndexSet<SequenceNr> {
        //println!("Messages: {:#?}", messages);
        self.unused_messages.sort(); //Sort in seq-nr order
        let mut deleted = IndexSet::new();
        let mut parent_lists = Bump::new();

        'outer: for msg in self.unused_messages.iter().rev() {
            if let Some(observers) = self.message_dependencies.get(msg) {
                for observer in observers {
                    if !deleted.contains(observer) {
                        // 'msg' can't be deleted, because it's observed by
                        // 'observer' - i.e a later message that has not been deleted.
                        println!(
                            "Can't delete {:?} because it's observed by {:?}",
                            msg, observer
                        );
                        continue 'outer;
                    }
                }
            }
            deleted.insert(*msg);
        }
        // 'deleted' ends up in reverse seqnr-order
        // iterate in seq-nr order.
        let mut parent_remap: IndexMap<SequenceNr, Vec<SequenceNr>> = IndexMap::new();
        for deleted in deleted.iter().rev() {
            let mut parent_list = vec![];
            let Some(msg) = &messages[deleted.index()] else {
                panic!("Attempt to delete already-deleted message.");
            };
            for parent in msg.parents() {
                let parent_index = SequenceNr::from_index(messages.get_index_of(&parent)
                    .expect("Parent unknown. This is not supported like this - it needs to be cleansed before msg added to store.").try_into().unwrap());
                if let Some(remapping) = parent_remap.get(&parent_index) {
                    parent_list.extend(remapping);
                } else {
                    parent_list.push(parent_index);
                }
            }

            parent_list.sort();
            parent_list.dedup();
            parent_remap.insert(*deleted, parent_list);
        }
        deleted
    }
    fn report_observed(&mut self, observer: SequenceNr, observee: SequenceNr) {
        self.message_dependencies
            .entry(observee)
            .or_default()
            .push(observer);
    }
    fn increase_use(&mut self, registrar: SequenceNr) {
        if self.uses.len() <= registrar.index() {
            self.uses.resize(registrar.index()+1, 0);
        }
        self.uses[registrar.index()] += 1;
    }
    fn decrease_use(&mut self, registrar: SequenceNr) {
        let cur = &mut self.uses[registrar.index()];
        if *cur == 0 {
            panic!("Corrupt use count for sequence nr {:?}", registrar);
        }
        *cur -= 1;
        if *cur == 0 {
            self.unused_messages.push(registrar);
        }
    }
}



pub struct DatabaseContext {
    data: *mut u8,
    data_len: usize,
    pointer: Cell<usize>,
    root_index: Option<GenPtr>,
    undo_log: UndoLog,
    // Make sure neither Send nor Sync
    phantom: PhantomData<*mut ()>,

    // The current message being written (or None if not open for writing)
    registrar_tracker: RefCell<RegistrarTracker>,

    /// The next message expected to be applied.
    /// Starts at 0. When a message is being applied, this field
    /// will have the seqnr of the message being applied, not the next one.
    next_seqnr: SequenceNr,
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
            undo_log: UndoLog::new(),
            phantom: Default::default(),
            registrar_tracker: Default::default(),
            next_seqnr: SequenceNr::INVALID,
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
    pub fn next_seqnr(&self) -> SequenceNr {
        self.next_seqnr
    }

    /// Note: Must not be public, since while output of [`crate::Database::get_root`] is still
    /// live, time travel _must not_ occur, since it would lead to unsoundness (potentially
    /// changing objects while they were used).
    /// Rewinding during construction of the root object is not allowed.
    /// This rewinds time to just _before_ the given sequence number was added.
    pub(crate) fn rewind(&mut self, new_time: SequenceNr) {
        if self.next_seqnr().is_invalid() {
            panic!(
                "Attempt to rewind time before any time snapshot was recorded.\
                    It is not allowed to rewind time before/while constructing the root object."
            )
        }

        let result = self.undo_log.rewind(|entry| {
            match entry {
                UndoLogEntry::SetPointer(new_pointer) => {
                    let cur = self.pointer.get();
                    debug_assert!(new_pointer <= cur);
                    unsafe { Self::mut_slice(self.data, new_pointer..cur).fill(0) };
                    self.pointer.set(new_pointer);
                    HowToProceed::PopAndContinue
                }
                UndoLogEntry::ZeroOut { start, len } => {
                    unsafe { Self::mut_slice(self.data, start..start + len).fill(0) };
                    HowToProceed::PopAndContinue
                }
                UndoLogEntry::Restore { start, data } => {
                    unsafe {
                        Self::mut_slice(self.data, start..start + data.len()).copy_from_slice(data)
                    };
                    HowToProceed::PopAndContinue
                }
                UndoLogEntry::Rewind(time) => {
                    if time == new_time {
                        self.next_seqnr = new_time;
                        HowToProceed::PopAndStop
                    } else if time > new_time {
                        HowToProceed::PopAndContinue
                    } else {
                        panic!(
                            "Couldn't rewind time to {}, ended up back at {}",
                            new_time, time
                        );
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

    pub fn set_next_seqnr(&mut self, seqnr: SequenceNr) {
        if seqnr.is_invalid() {
            self.next_seqnr = seqnr;
            return;
        }
        if seqnr <= self.next_seqnr {
            panic!("Attempt to set sequence number to a smaller or equal value");
        }


        self.undo_log.record(UndoLogEntry::Rewind(seqnr));
        self.next_seqnr = seqnr;
    }

    pub fn set_root_ptr(&mut self, genptr: GenPtr) {
        self.root_index = Some(genptr);
    }

    pub fn get_root_ptr<Ptr: Pointer + Any + 'static>(&self) -> Ptr {
        match self.root_index {
            Some(GenPtr::Thin(ptr)) => {
                if TypeId::of::<Ptr>() == TypeId::of::<ThinPtr>() {
                    return unsafe { transmute_copy(&ptr) };
                }
                panic!(
                    "Wrong type of root pointer in database. Has schema changed significantly since last access?"
                );
            }
            Some(GenPtr::Fat(ptr)) => {
                if TypeId::of::<Ptr>() == TypeId::of::<FatPtr>() {
                    return unsafe { transmute_copy(&ptr) };
                }
                panic!(
                    "Wrong type of root pointer in database. Has schema changed significantly since last access?"
                );
            }
            None => {
                panic!("Unknown root pointer");
            }
        }
    }

    pub fn copy(&mut self, source: FatPtr, dest_index: usize) {
        unsafe {
            //dbg!(&source, &dest_index);

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

        let alignment_adjustment =
            index_rounded_up_to_custom_align(self.pointer.get(), align).unwrap();
        self.undo_log
            .record(UndoLogEntry::SetPointer(self.pointer.get()));
        self.pointer
            .set(alignment_adjustment.checked_add(size).unwrap());
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
        unsafe {
            std::slice::from_raw_parts_mut(data.wrapping_add(range.start), range.end - range.start)
        }
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
    pub fn write_pod<T: Pod>(&mut self, src: T, dest: &mut T) {
        let dest_index = self.index_of_sized(dest);

        self.undo_log.record(UndoLogEntry::Restore {
            start: dest_index.0,
            data: bytes_of(dest),
        });
        *dest = src;
    }
    pub fn index_of_sized<T: Sized>(&self, t: &T) -> ThinPtr {
        ThinPtr::create(t, self.data as *const u8)
    }
    pub fn index_of<T: Object>(&self, t: &T) -> T::Ptr {
        T::Ptr::create(t, self.data)
    }
    pub fn index_of_ptr<T>(&self, t: *const T) -> ThinPtr {
        ThinPtr((t as *const u8 as usize) - (self.data as usize))
    }

    /// Call after writing a message.
    pub fn finalize_message(&mut self) {
        self.registrar_tracker.borrow_mut().finalize_message(
            self.next_seqnr,
        );
    }
    /// Call after a complete update, i.e, applying multiple messages
    /// Returns all messages that can now be removed.
    pub fn finalize_transaction<M: Message+Debug>(
        &mut self,
        message_store: &IndexMap<MessageId, Option<M>>,
    ) -> Vec<SequenceNr> {
        self.registrar_tracker
            .borrow_mut()
            .finalize_transaction(message_store)
            .into_iter()
            .collect()
    }

    pub fn update_registrar(&mut self, registrar_point: &mut SequenceNr) {
        let mut track = self.registrar_tracker.borrow_mut();
        if registrar_point.0 != 0 {
            track.decrease_use(*registrar_point);
        }
        let current_registrar = self
            .next_seqnr;
        track.increase_use(current_registrar);
        drop(track);
        self.write_pod(current_registrar, registrar_point)
    }

    /// Signify that the current message has observed data previously written
    /// by 'registrar'.
    pub fn observe_registrar(&self, observee: SequenceNr) {
        if self.next_seqnr.is_invalid() {
            return;
        }
        if observee.is_invalid() {
            return;
        }
        let observer = self
            .next_seqnr;
        if observer != observee {
            println!("Tracking that {} observed {}", observer, observee);
            self.registrar_tracker
                .borrow_mut()
                .report_observed(observer, observee);
        }
    }
}
