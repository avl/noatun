use crate::undo_store::{HowToProceed, UndoLog, UndoLogEntry};
use crate::{Application, FatPtr, FixedSizeObject, GenPtr, Message, MessageId, Object, Pointer, SequenceNr, Target, ThinPtr};
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
use std::path::Path;
use std::slice;
use std::slice::SliceIndex;
use crate::buffer::message_dependency_tracker::{MessageDependencyTracker, MmapMessageDependencyTracker};
use anyhow::Result;
use crate::buffer::disk_main_store::DiskMainStore;

#[derive(Debug)]
struct RegistrarTracker<T:MessageDependencyTracker> {
    uses: Vec<u32>,
    /// Messages are added to this list when their
    /// last registrar_point is overwritten
    /// Such messages are candidates to be removed, but only if
    /// no other message (that isn't also to be removed), depend on it.
    unused_messages: Vec<SequenceNr>,

    /// Mapping from message-id to other messages that have read output
    /// from said message
    message_dependencies: T,
}

impl<T:MessageDependencyTracker> RegistrarTracker<T> {
    fn new (path: &Target) -> Result<RegistrarTracker<T>> {
        Ok(RegistrarTracker {
            uses: vec![],
            unused_messages: vec![],
            message_dependencies: T::new(path)?,
        })
    }
}


mod message_dependency_tracker;

impl<T:MessageDependencyTracker> RegistrarTracker<T> {
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

            for observer in self.message_dependencies.read_dependency(*msg) {
                if !deleted.contains(&observer) {
                    // 'msg' can't be deleted, because it's observed 7by
                    // 'observer' - i.e a later message that has not been deleted.
                    println!(
                        "Can't delete {:?} because it's observed by {:?}",
                        msg, observer
                    );
                    continue 'outer;
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
        /*self.message_dependencies
            .entry(observee)
            .or_default()
            .push(observer);*/
        self.message_dependencies.record_dependency(observee, observer);
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

pub trait MainStore {
    fn new(name: &Target) -> Result<Self> where Self: Sized;
    fn data(&self) -> *const u8;
    fn mut_data(&mut self) -> *mut u8;
    fn len(&self) -> usize;
}
pub struct InMemoryMainStore {
    data: *mut u8,
    data_len: usize,

}
impl Drop for InMemoryMainStore {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(10000, 256).unwrap();
        unsafe { std::alloc::dealloc(self.data, layout) }
    }
}
impl MainStore for InMemoryMainStore {
    fn new(name: &Target) -> Result<Self> {
        let layout = Layout::from_size_align(10000, 256).unwrap();
        let data = unsafe { std::alloc::alloc_zeroed(layout) };
        Ok(InMemoryMainStore {
            data,
            data_len: 10000,
        })
    }

    fn data(&self) -> *const u8 {
        self.data
    }

    fn mut_data(&mut self) -> *mut u8 {
        self.data
    }

    fn len(&self) -> usize {
        self.data_len
    }
}

mod disk_main_store {
    use std::fs::OpenOptions;
    use memmap2::MmapMut;
    use crate::buffer::MainStore;
    use crate::Target;

    pub struct DiskMainStore {
        data: MmapMut,
    }
    impl MainStore for DiskMainStore {
        fn new(name: &Target) -> anyhow::Result<Self>
        where
            Self: Sized
        {
            std::fs::create_dir_all(&name.path);
            let main_path = name.path.join("main_store.bin");
            let main_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(name.overwrite)
                .open(&main_path)?;

            main_file.set_len(10000)?;
            let data_mmap = unsafe { MmapMut::map_mut(&main_file)? };

            Ok(DiskMainStore {
                data: data_mmap,
            })
        }

        fn data(&self) -> *const u8 {
            unsafe { self.data.as_ptr() }
        }

        fn mut_data(&mut self) -> *mut u8 {
            unsafe { self.data.as_mut_ptr() }
        }

        fn len(&self) -> usize {
            self.data.len()
        }
    }
}

compile_error!("add on-disk store for the actual messages!");
pub struct DatabaseContext<M = DiskMainStore, D  = MmapMessageDependencyTracker>
    where M: MainStore, D: MessageDependencyTracker
{
    main_store: M,
    pointer: Cell<usize>,
    root_index: Option<GenPtr>,
    undo_log: UndoLog,
    // Make sure neither Send nor Sync
    phantom: PhantomData<*mut ()>,

    // The current message being written (or None if not open for writing)
    registrar_tracker: RefCell<RegistrarTracker<D>>,

    /// The next message expected to be applied.
    /// Starts at 0. When a message is being applied, this field
    /// will have the seqnr of the message being applied, not the next one.
    next_seqnr: SequenceNr,
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

impl<M:MainStore, D: MessageDependencyTracker> DatabaseContext<M,D> {
    pub fn next_seqnr(&self) -> SequenceNr {
        self.next_seqnr
    }
    pub fn new(name: &Target) -> Result<Self> {
        Ok(Self {
            main_store: M::new(name)?,
            pointer: Cell::new(0),
            root_index: None,
            undo_log: UndoLog::new(),
            phantom: Default::default(),
            registrar_tracker: RefCell::new(RegistrarTracker::new(name)?),
            next_seqnr: SequenceNr::INVALID,
        })
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
                    unsafe { Self::mut_slice(self.main_store.mut_data(), new_pointer..cur).fill(0) };
                    self.pointer.set(new_pointer);
                    HowToProceed::PopAndContinue
                }
                UndoLogEntry::ZeroOut { start, len } => {
                    unsafe { Self::mut_slice(self.main_store.mut_data(), start..start + len).fill(0) };
                    HowToProceed::PopAndContinue
                }
                UndoLogEntry::Restore { start, data } => {
                    unsafe {
                        Self::mut_slice(self.main_store.mut_data(), start..start + data.len()).copy_from_slice(data)
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
        self.main_store.data()
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
        if self.pointer.get() > self.main_store.len() {
            panic!("Out of memory");
        }
        unsafe { self.main_store.mut_data().wrapping_add(self.pointer.get() - size) }
    }
    pub fn allocate_array<const N: usize, const ALIGN: usize>(&mut self) -> &mut [u8; N] {
        self.allocate_slice(N, ALIGN).try_into().unwrap()
    }
    pub fn allocate_slice(&mut self, size: usize, align: usize) -> &mut [u8] {
        let start = self.allocate_raw(size, align);
        unsafe { std::slice::from_raw_parts_mut(start, size) }
    }
    pub unsafe fn access<'a>(&self, range: FatPtr) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.main_store.data().wrapping_add(range.start), range.len) }
    }
    pub unsafe fn access_mut<'a>(&mut self, range: FatPtr) -> &'a mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.main_store.mut_data().wrapping_add(range.start), range.len) }
    }
    pub unsafe fn mut_slice<'a>(data: *mut u8, range: Range<usize>) -> &'a mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(data.wrapping_add(range.start), range.end - range.start)
        }
    }
    pub unsafe fn access_pod<'a, T: Pod>(&self, index: usize) -> &'a T {
        unsafe {
            from_bytes(std::slice::from_raw_parts(
                self.main_store.data().wrapping_add(index),
                size_of::<T>(),
            ))
        }
    }
    pub unsafe fn access_pod_mut<'a, T: Pod>(&mut self, index: ThinPtr) -> &'a mut T {
        unsafe {
            from_bytes_mut(std::slice::from_raw_parts_mut(
                self.main_store.mut_data().wrapping_add(index.0),
                size_of::<T>(),
            ))
        }
    }

    pub fn write(&mut self, index: usize, data: &[u8]) {
        debug_assert!(index + data.len() <= self.main_store.len());
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
        ThinPtr::create(t, self.main_store.data() as *const u8)
    }
    pub fn index_of<T: Object>(&self, t: &T) -> T::Ptr {
        T::Ptr::create(t, self.main_store.data())
    }
    pub fn index_of_ptr<T>(&self, t: *const T) -> ThinPtr {
        ThinPtr((t as *const u8 as usize) - (self.main_store.data() as usize))
    }

    /// Call after writing a message.
    pub fn finalize_message(&mut self) {
        self.registrar_tracker.borrow_mut().finalize_message(
            self.next_seqnr,
        );
    }
    /// Call after a complete update, i.e, applying multiple messages
    /// Returns all messages that can now be removed.
    pub fn finalize_transaction<MSG: Message+Debug>(
        &mut self,
        message_store: &IndexMap<MessageId, Option<MSG>>,
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
