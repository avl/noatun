use crate::boot_checksum::get_boot_checksum;
use crate::disk_abstraction::{Disk, StandardDisk};
use crate::disk_access::FileAccessor;
use crate::message_store::OnDiskMessageStore;
use crate::platform_specific::get_boot_time;
use crate::undo_store::{HowToProceed, UndoLog, UndoLogEntry};
use crate::{
    Application, FatPtr, FixedSizeObject, GenPtr, Message, MessageId, Object, Pointer, SequenceNr,
    Target, ThinPtr,
};
use anyhow::{Context, Result, bail};
use bumpalo::Bump;
use bytemuck::{Pod, Zeroable, bytes_of, from_bytes, from_bytes_mut};
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
use std::{iter, slice};
use std::slice::SliceIndex;
use crate::data_types::{DatabaseVec, RawDatabaseVec};
use crate::projection_store::message_dependency_tracker::linked_list_entry::DepTrackLinkedListEntry;
use crate::projection_store::registrar_info::{RegistrarInfo, RegistrarTracker, UnusedInfo};

mod registrar_info {
    use std::fmt::Debug;
    use bumpalo::Bump;
    use indexmap::IndexMap;
    use crate::disk_abstraction::Disk;

    use crate::{DatabaseContext, Message, SequenceNr, Target};
    use crate::message_store::OnDiskMessageStore;

    #[derive(Debug, Clone, Default)]
    pub(crate) struct RegistrarInfo {
        uses: u32,
    }
    impl RegistrarInfo {
        pub fn get_opaque(&self) -> bool {
            self.uses&0x8000_0000 == 0
        }
        pub fn set_non_opaque(&mut self) {
            self.uses|=0x8000_0000;
        }
        pub fn get_use(&self) -> u32 {
            self.uses&0x7FFF_FFFF
        }
        pub fn increase_use(&mut self) {
            if self.get_use() >= 0x7FFF_FFFF {
                return;
            }
            self.uses += 1;
        }
        pub fn decrease_use(&mut self) {
            let cur_uses = self.get_use();
            if cur_uses == 0 {
                panic!("Internal error, use count wrong");
            }
            if cur_uses >= 0x7FFF_FFFF {
                return;
            }
            self.uses -= 1;
        }
    }

    // TODO: We might want to optimize by only looking at 'seq' in the Ord impl
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
    pub struct UnusedInfo {
        /// The message that is no longer used (to be deleted, possibly)
        pub seq: SequenceNr,
        /// True if the above message only wrote opaque data
        pub opaque: bool,
    }

    #[derive(Debug)]
    pub(crate) struct RegistrarTracker {
        //TODO: Move this to mmap-file too!
        uses: Vec<RegistrarInfo>,
        /// Messages are added to this list when their
        /// last registrar_point is overwritten
        /// Such messages are candidates to be removed, but only if
        /// no other message (that isn't also to be removed), depend on it.
        unused_messages: Vec<UnusedInfo>,

        // Mapping from message-id to other messages that have read output
        // from said message
    }
    impl RegistrarTracker {
        pub(crate) fn new<S: Disk>(disk: &mut S, path: &Target, max_size: usize) -> anyhow::Result<RegistrarTracker> {
            Ok(RegistrarTracker {
                uses: vec![],
                unused_messages: vec![],
            })
        }

        pub(crate) fn clear(&mut self) {
            self.uses.clear();
            self.unused_messages.clear();
            //self.message_dependencies.clear();
        }

        pub(crate) fn finalize_message(&mut self, message_id: SequenceNr) {
            debug_assert_ne!(message_id.0, 0);
            if self.uses.len() <= message_id.index() {
                // This is a bit of a special case. This is a message
                // that did not actually modify any state at all during its projection.
                self.unused_messages.push(UnusedInfo {
                    seq: message_id,
                    opaque: true,
                });
                return;
            }
            let track = &self.uses[message_id.index()];

            if track.get_use() == 0 {
                self.unused_messages.push(UnusedInfo {
                    seq: message_id,
                    opaque: track.get_opaque(),
                });
            }
        }

        /*
    About deletions:

    We can delete messages that no longer have any effect on the state, but only in these
    circumstances:

     1: Messages that only update OpaqueData
     2: Messages that have never been transmitted, and upon which no existing message depends
     3: Messages that are older than MAX_PARTITION_TIME, and have no trace in the state at
        MAX_PARTITION_TIME, and upon which no message depends

    */

        pub(crate) fn calculate_stale_messages<M: Message + Debug>(
            &mut self,
            context: &mut DatabaseContext,
            messages: &mut OnDiskMessageStore<M>,
            is_before_cutoff: bool
        ) -> anyhow::Result<Vec<SequenceNr>> {
            self.unused_messages.sort(); //Sort in seq-nr order
            let mut deleted = Vec::new();
            let mut parent_lists = Bump::new();

            println!("Calculating staleness, cutoff: {:?}", is_before_cutoff);
            'outer: for msg in self.unused_messages.iter().rev() {

                if msg.opaque {
                    // This can be deleted
                } else if !messages.may_have_been_transmitted(msg.seq)? || is_before_cutoff {

                    for observer in context.read_dependency(context, msg.seq) {
                        if !deleted.contains(&observer) {
                            // 'msg' can't be deleted, because it's observed by
                            // 'observer' - i.e a later message that has not been deleted.
                            println!(
                                "Can't delete {:?} because it's observed by {:?}",
                                msg, observer
                            );
                            continue 'outer;
                        }
                    }
                } else {
                    println!(
                        "Can't delete {:?}, because we can't know if someone will use its output",
                        msg
                    );
                    continue 'outer;
                }


                println!("Deleting {:?}", msg);

                deleted.push(msg.seq);
            }
            self.unused_messages.clear();

            // 'deleted' ends up in reverse seqnr-order
            // iterate in seq-nr order.
            let mut parent_remap: IndexMap<SequenceNr, Vec<SequenceNr>> = IndexMap::new();
            for deleted in deleted.iter().rev() {
                let mut parent_list = vec![];

                let Some(msg) = messages.read_message_by_index(deleted.index())? else {
                    panic!("Attempt to delete already-deleted message.");
                };
                for parent in msg.parents() {
                    let parent_index = SequenceNr::from_index(messages.get_index_of(parent)?
                        .expect("Parent unknown. This is not supported like this - it needs to be cleansed before msg added to store."));
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
            Ok(deleted)
        }
        pub(crate) fn increase_use(&mut self, registrar: SequenceNr) {
            if self.uses.len() <= registrar.index() {
                self.uses.resize(registrar.index() + 1, RegistrarInfo::default());
            }
            self.uses[registrar.index()].increase_use();
        }
        pub(crate) fn set_non_opaque(&mut self, registrar: SequenceNr) {
            if self.uses.len() <= registrar.index() {
                self.uses.resize(registrar.index() + 1, RegistrarInfo::default());
            }
            self.uses[registrar.index()].set_non_opaque();
        }
        pub(crate) fn decrease_use(&mut self, registrar: SequenceNr) {
            let cur = &mut self.uses[registrar.index()];
            if cur.get_use() == 0 {
                panic!("Corrupt use count for sequence nr {:?}", registrar);
            }
            cur.decrease_use();
            if cur.get_use() == 0 {
                // This is the normal way messages end up in 'unused_messags'
                self.unused_messages.push(UnusedInfo {
                    seq: registrar,
                    opaque: cur.get_opaque(),
                });
            }
        }
    }

}



mod message_dependency_tracker;


const DEFAULT_SIZE: usize = 10000;

const MAIN_DB_STATUS_CLEAN: u8 = 1;
const MAIN_DB_STATUS_DIRTY: u8 = 0;

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(transparent)]
pub struct MainDbStatus(u8);

/// The header of the main database
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct MainDbHeader {
    /// The sequence number of the next message that will be applied.
    /// For an empty database, this starts at 0. 0 is considered an 'invalid' sequence number,
    /// representing the state before the root message has been initially created.
    next_seqnr: SequenceNr,
    /// Dummy padding, otherwise bytemuck derive fails (presumably because size of
    /// struct isn't sum of size of fields).
    status: MainDbStatus,
    usize_size: u8,
    padding1: u8,
    padding2: u8,

    /// SHA2-checksum of output of `who -b`.
    /// This is used to detect if there's been a reboot (because of power outage, for example)
    /// since the last access. This only affects recovery after the db has been left in a
    /// dirty state.
    last_boot: [u8; 16],
}

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]pub struct MainDbAuxHeader {
    deptrack_keys: ThinPtr,
    deptrack_vals: ThinPtr,
}

pub struct DatabaseContext {
    main_db_mmap: FileAccessor,
    //pointer: Cell<usize>,
    root_index: Option<GenPtr>,
    undo_log: UndoLog,
    // Make sure neither Send nor Sync
    phantom: PhantomData<*mut ()>,

    // The current message being written (or None if not open for writing)
    registrar_tracker: RefCell<RegistrarTracker>,
    // The next message expected to be applied.
    // Starts at 0. When a message is being applied, this field
    // will have the seqnr of the message being applied, not the next one.
    //next_seqnr: SequenceNr,

    /// the sequencenr that has been observed
    /// This is a sequence of u64 message indices, pointing at LinkedListEntry's in 'vals'
    deptrack_keys: ThinPtr,
    /// the sequence nr that have observed each key. I.e, the messages that
    /// are dependent upon the key.
    /// This is a sequence of 'LinkedListEntry'.
    deptrack_vals: ThinPtr,
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

    fn clear_deptrack(&mut self, context: &mut DatabaseContext) {
        let (key_len, keys): (_, &mut [u64]) = Self::access(context, self.deptrack_keys);
        *key_len = 0;
        let (val_len, vals): (_, &mut [crate::projection_store::message_dependency_tracker::linked_list_entry::DepTrackLinkedListEntry]) = Self::access(context, self.deptrack_vals);
        *val_len = 0;
    }

    fn record_dependency(&self, observee: SequenceNr, observer: SequenceNr) {
        assert!(observee.is_valid());
        assert!(observer.is_valid());

        let aux_header = self.get_aux_header();

        let keys: &mut RawDatabaseVec<ThinPtr> = unsafe { self.access_pod_mut(aux_header.deptrack_keys) };
        let vals: &mut RawDatabaseVec<DepTrackLinkedListEntry> = unsafe { self.access_pod_mut(aux_header.deptrack_vals) };

        keys.grow(self, observee.index());
        if observee.index() >= keys.len() {
            keys.grow(self, observee.index()+1);
        }

        //let (val_len, vals): (_, &mut [crate::projection_store::message_dependency_tracker::linked_list_entry::DepTrackLinkedListEntry]) = Self::access(&mut self.deptrack_vals);
        //let (key_len, keys): (_, &mut [u64]) = Self::access(&mut self.deptrack_keys);
        //debug_assert_eq!(keys.len(), self.key_capacity);
        //debug_assert_eq!(vals.len(), self.value_capacity);

        let prev = keys.get_internal(self, observee.index());

        let mut new_entry = DepTrackLinkedListEntry::default();
        new_entry.set_seq(observer);
        new_entry.set_next(*prev);

        let new_entry = vals.push_internal(self, new_entry);

        let key_place = keys.get_mut_internal(self, observee.index());

        self.write_pod(new_entry, key_place);

        compile_error!("Continue porting the record-dependency system to persist in the MainDb, so it rolls back naturally")

    }
    fn read_dependency(&mut self, context: &mut DatabaseContext, observee: SequenceNr) -> impl Iterator<Item = SequenceNr> {
        let (key_len, keys): (_, &mut [u64]) = Self::access(context, self.deptrack_keys);
        let (val_len, vals): (_, &mut [crate::projection_store::message_dependency_tracker::linked_list_entry::DepTrackLinkedListEntry]) = Self::access(context, self.deptrack_vals);

        let mut cur: u64 = if observee.index() < keys.len() {
            keys[observee.index()]
        } else {
            0
        };

        iter::from_fn(move || {
            if cur == 0 {
                return None;
            }
            let entry = &vals[cur as usize - 1];
            cur = entry.get_next();
            Some(entry.seq())
        })
    }
    #[inline]
    fn get_count(mmap: &FileAccessor) -> usize {
        *bytemuck::from_bytes::<u64>(&mmap.map()[0..size_of::<u64>()]) as usize
    }
    #[inline]
    fn access<'a, T: Pod>(context: &mut DatabaseContext, index: ThinPtr) -> (&'a mut u64, &'a mut [T]) {

        let vec = unsafe { RawDatabaseVec::<u8>::access_mut(context, index) };

        let slice_bytes: &mut [u8] = vec.get_full_slice_mut(context);

        let (slice_a, slice_b) = slice_bytes.split_at_mut(std::mem::size_of::<u64>());
        let count: &mut u64 = bytemuck::from_bytes_mut(slice_a);
        let slice: &mut [T] = bytemuck::cast_slice_mut(slice_b);
        (count, slice)
    }

    // Record mapping from observee to observer

    fn calc_needed_bytes_keys(count: usize) -> usize {
        (std::mem::size_of::<u64>()) * count + std::mem::size_of::<u64>()
    }
    fn calc_needed_bytes_vals(count: usize) -> usize {
        (std::mem::size_of::<u64>() + size_of::<SequenceNr>()) * count + std::mem::size_of::<u64>()
    }

    fn reallocate_keys(&mut self, context: &mut DatabaseContext, new_count: usize) -> Result<()> {
        let keys = unsafe { RawDatabaseVec::<u8>::access_mut(context, self.deptrack_keys) };

        keys.grow(Self::calc_needed_bytes_keys(new_count));
        Ok(())
    }
    fn reallocate_values(&mut self, new_count: usize) -> Result<()> {
        self.deptrack_vals.grow(Self::calc_needed_bytes_vals(new_count));

        self.value_capacity = new_count;
        Ok(())
    }

    #[inline(always)]
    pub fn next_seqnr(&self) -> SequenceNr {
        let header: &MainDbHeader =
            unsafe { &*(self.main_db_mmap.map_const_ptr() as *const MainDbHeader) };
        header.next_seqnr
    }

    pub fn buggy_clear_registrars(&mut self) {
        self.registrar_tracker.borrow_mut().clear();
    }

    // We call this 'pointer' here, but 'used_space' in mmap.
    // This is because the write-pointer for new data in the DatabaseContext is at the
    // end of the memory mapped file - which is equal to 'used_space'.
    #[inline(always)]
    fn pointer(&self) -> usize {
        self.main_db_mmap.used_space()
    }
    #[inline(always)]
    fn set_pointer(&self, new_value: usize) {
        self.main_db_mmap.set_used_space(new_value);
        /*let header: &mut MainDbHeader =
            unsafe { &mut *(self.main_db_mmap.map_mut_ptr() as *mut MainDbHeader) };
        header.set_pointer = new_value as u64;*/
    }
    #[inline(always)]
    fn raw_set_next_seqnr(&self, new_value: SequenceNr) {
        let header: &mut MainDbHeader =
            unsafe { &mut *(self.main_db_mmap.map_mut_ptr() as *mut MainDbHeader) };
        header.next_seqnr = new_value;
    }
    #[inline(always)]
    fn set_pointer_of(main_db_mmap: &FileAccessor, new_value: usize) {
        /*let header: &mut MainDbHeader =
            unsafe { &mut *(main_db_mmap.map_mut_ptr() as *mut MainDbHeader) };
        header.pointer = new_value as u64;*/
        main_db_mmap.set_used_space(new_value);
    }
    #[inline(always)]
    fn pointer_of(main_db_mmap: &FileAccessor) -> usize {
        main_db_mmap.used_space()
    }
    #[inline(always)]
    fn raw_set_next_seqnr_of(main_db_mmap: &FileAccessor, new_value: SequenceNr) {
        let header: &mut MainDbHeader =
            unsafe { &mut *(main_db_mmap.map_mut_ptr() as *mut MainDbHeader) };
        header.next_seqnr = new_value;
    }

    /// Returns true if database was previously clean
    pub fn mark_dirty(&mut self) -> Result<bool> {
        let header: &mut MainDbHeader =
            unsafe { &mut *(self.main_db_mmap.map_mut_ptr() as *mut MainDbHeader) };

        let was_clean = header.status.0 == MAIN_DB_STATUS_CLEAN;
        header.status = MainDbStatus(MAIN_DB_STATUS_DIRTY);

        self.main_db_mmap
            .flush_range(0, std::mem::size_of::<MainDbHeader>())?;

        Ok(was_clean)
    }
    pub fn mark_clean(&mut self) -> Result<()> {
        let header: &mut MainDbHeader =
            unsafe { &mut *(self.main_db_mmap.map_mut_ptr() as *mut MainDbHeader) };

        header.status = MainDbStatus(MAIN_DB_STATUS_CLEAN);

        Ok(())
    }

    pub fn is_dirty(&self) -> bool {
        let header: &MainDbHeader =
            unsafe { &*(self.main_db_mmap.map_mut_ptr() as *const MainDbHeader) };

        header.status.0 != MAIN_DB_STATUS_CLEAN
    }
    pub fn clear(&mut self) -> Result<()> {
        self.main_db_mmap.truncate(0);
        self.main_db_mmap.grow(size_of::<MainDbHeader>() + size_of::<MainDbAuxHeader>())?;
        Self::write_initial_header(&mut self.main_db_mmap);
        self.undo_log.clear();
        self.registrar_tracker.borrow_mut().clear();

        let aux_header: &mut MainDbAuxHeader =
            bytemuck::from_bytes_mut(&mut self.main_db_mmap.map_mut()[size_of::<MainDbHeader>()..size_of::<MainDbHeader>()+size_of::<MainDbAuxHeader>()]);

        aux_header.deptrack_keys =
            self.index_of_sized(RawDatabaseVec::<ThinPtr>::new(self));
        aux_header.deptrack_vals =
            self.index_of_sized(RawDatabaseVec::<DepTrackLinkedListEntry>::new(self));

        Ok(())
    }

    pub(crate) fn get_aux_header(&self) -> MainDbAuxHeader {
        let aux_header: &MainDbAuxHeader =
            bytemuck::from_bytes(&mut self.main_db_mmap.map()[size_of::<MainDbHeader>()..size_of::<MainDbHeader>()+size_of::<MainDbAuxHeader>()]);
        *aux_header
    }

    fn write_initial_header(mmap: &mut FileAccessor) {
        assert_eq!(mmap.get_used_space(), size_of::<MainDbHeader>() + size_of::<MainDbAuxHeader>());
        let header: &mut MainDbHeader =
            bytemuck::from_bytes_mut(&mut mmap.map_mut()[0..size_of::<MainDbHeader>()]);
        header.next_seqnr = SequenceNr::INVALID;

        header.status = MainDbStatus(MAIN_DB_STATUS_DIRTY);
        header.usize_size = size_of::<usize>()
            .try_into()
            .expect("The size of an 'usize' must be less than 256 bytes");

        header.last_boot = get_boot_checksum();
        let aux_header: &mut MainDbAuxHeader =
            bytemuck::from_bytes_mut(&mut mmap.map_mut()[size_of::<MainDbHeader>()..size_of::<MainDbHeader>()+size_of::<MainDbAuxHeader>()]);
        aux_header.deptrack_keys = ThinPtr(0);
        aux_header.deptrack_vals = ThinPtr(0);

    }

    pub(crate) fn new<S: Disk>(s: &mut S, name: &Target, max_size: usize) -> Result<Self> {
        let mut main_db_file = s
            .open_file(name, "maindb", 0, max_size)
            .context("opening main store file")?;

        let mut is_new = false;
        if main_db_file.used_space() < size_of::<MainDbHeader>() {
            main_db_file
                .grow(size_of::<MainDbHeader>())
                .context("Writing initial header to main db file")?;
            main_db_file.map_mut().fill(0);
            is_new = true;
        }

        if is_new {
            Self::write_initial_header(&mut main_db_file);
        }

        let header: &MainDbHeader =
            unsafe { &*(main_db_file.map_const_ptr() as *const MainDbHeader) };
        if <u8 as Into<usize>>::into(header.usize_size) != size_of::<usize>() {
            bail!(
                "The file on disk was created on a machine with usize = {} bytes, but this machine has usize = {} bytes",
                header.usize_size,
                size_of::<usize>()
            );
        }

        Ok(Self {
            main_db_mmap: main_db_file,
            root_index: None,
            undo_log: UndoLog::new(s, name, max_size)?,
            phantom: Default::default(),
            registrar_tracker: RefCell::new(RegistrarTracker::new(s, name, max_size)?),
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
        println!("Rewinding to {:?}", new_time);

        let result = self.undo_log.rewind(|entry| match entry {
            UndoLogEntry::SetPointer(new_pointer) => {
                let cur = Self::pointer_of(&self.main_db_mmap);
                debug_assert!(new_pointer <= cur);
                unsafe {
                    Self::mut_slice(self.main_db_mmap.map_mut_ptr(), new_pointer..cur).fill(0)
                };
                Self::set_pointer_of(&self.main_db_mmap, new_pointer);
                HowToProceed::PopAndContinue
            }
            UndoLogEntry::ZeroOut { start, len } => {
                unsafe {
                    Self::mut_slice(self.main_db_mmap.map_mut_ptr(), start..start + len).fill(0)
                };
                HowToProceed::PopAndContinue
            }
            UndoLogEntry::Restore { start, data } => {
                unsafe {
                    Self::mut_slice(self.main_db_mmap.map_mut_ptr(), start..start + data.len())
                        .copy_from_slice(data)
                };
                HowToProceed::PopAndContinue
            }
            UndoLogEntry::Rewind(time) => {
                if time == new_time {
                    Self::raw_set_next_seqnr_of(&self.main_db_mmap, new_time);
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
        });
        if !result {
            panic!("Rewind failed");
        }
    }

    pub fn start_ptr(&self) -> *const u8 {
        self.main_db_mmap.map_const_ptr()
    }

    pub fn set_next_seqnr(&mut self, new_seqnr: SequenceNr) {
        if new_seqnr.is_invalid() {
            self.raw_set_next_seqnr(new_seqnr);
            return;
        }
        if new_seqnr <= self.next_seqnr() {
            panic!(
                "Attempt to set sequence number to a smaller or equal value. Was: {:?}, attempted new value: {:?}",
                self.next_seqnr(),
                new_seqnr
            );
        }

        self.undo_log.record(UndoLogEntry::Rewind(new_seqnr));
        self.raw_set_next_seqnr(new_seqnr);
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

    pub fn copy(&self, source: FatPtr, dest_index: usize) {
        unsafe {
            //dbg!(&source, &dest_index);

            self.undo_log.record(UndoLogEntry::Restore {
                start: dest_index,
                data: self.access_slice(FatPtr::from(dest_index, source.len)),
            });

            let dest = self.access_slice_mut::<u8>(FatPtr {
                start: dest_index,
                len: source.len,
            });

            let src = self.access_slice(source);

            dest.copy_from_slice(src);
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn allocate_pod<T: Pod>(&self) -> &mut T {
        let bytes = self.allocate_raw(std::mem::size_of::<T>(), std::mem::align_of::<T>());
        unsafe { &mut *(bytes as *mut T) }
    }

    pub fn allocate_raw(&self, size: usize, align: usize) -> *mut u8 {
        if align > 256 {
            panic!("Noatun arbitrarily does not support types with alignment > 256");
        }

        let alignment_adjustment = index_rounded_up_to_custom_align(self.pointer(), align).unwrap();
        self.undo_log
            .record(UndoLogEntry::SetPointer(self.pointer()));

        let new_pointer = alignment_adjustment.checked_add(size).unwrap();
        self.main_db_mmap
            .grow(new_pointer)
            .expect("Failed to allocate memory");
        unsafe {
            self.main_db_mmap
                .map_mut_ptr()
                .wrapping_add(new_pointer - size)
        }
    }
    pub fn allocate_array<const N: usize, const ALIGN: usize>(&mut self) -> &mut [u8; N] {
        self.allocate_slice(N, ALIGN).try_into().unwrap()
    }
    pub fn allocate_slice(&mut self, size: usize, align: usize) -> &mut [u8] {
        let start = self.allocate_raw(size, align);
        unsafe { std::slice::from_raw_parts_mut(start, size) }
    }
    /// # Safety
    /// The returned range must not overlap any mutable reference.
    /// Alignment must be right.
    pub unsafe fn access_slice_at<'a, T: Pod>(&self, offset: usize, size: usize) -> &'a [T] {
        assert!(offset + size * size_of::<T>() <= self.main_db_mmap.used_space());
        unsafe {
            std::slice::from_raw_parts(
                self.main_db_mmap.map_const_ptr().wrapping_add(offset) as *const T,
                size,
            )
        }
    }
    /// # Safety
    /// The returned range must not overlap any reference.
    /// Alignment must be right.
    pub unsafe fn access_slice_at_mut<'a, T: Pod>(&self, offset: usize, size: usize) -> &'a mut [T] {
        assert!(offset + size * size_of::<T>() <= self.main_db_mmap.used_space());
        unsafe {
            std::slice::from_raw_parts_mut(
                self.main_db_mmap.map_mut_ptr().wrapping_add(offset) as *mut T,
                size,
            )
        }
    }

    /// # Safety
    /// The returned range must not overlap any mutable reference
    /// Alignment must be right.
    pub unsafe fn access_slice<'a, T: Pod>(&self, range: FatPtr) -> &'a [T] {
        assert!(range.start + range.len <= self.main_db_mmap.used_space());
        unsafe {
            std::slice::from_raw_parts(
                self.main_db_mmap.map_const_ptr().wrapping_add(range.start) as *const T,
                range.len / size_of::<T>(),
            )
        }
    }
    /// # Safety
    /// The returned range must not overlap any other reference
    /// Alignment must be right.
    pub unsafe fn access_slice_mut<'a, T: Pod>(&self, range: FatPtr) -> &'a mut [T] {
        assert!(range.start + range.len <= self.main_db_mmap.used_space());
        unsafe {
            std::slice::from_raw_parts_mut(
                self.main_db_mmap.map_mut_ptr().wrapping_add(range.start) as *mut T,
                range.len / size_of::<T>(),
            )
        }
    }

    /// # Safety
    /// The given range must point to valid memory, and must not overlap any other reference
    pub unsafe fn mut_slice<'a>(data: *mut u8, range: Range<usize>) -> &'a mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(data.wrapping_add(range.start), range.end - range.start)
        }
    }

    /// # Safety:
    /// Caller must ensure no mutable reference exists to the requested object
    pub unsafe fn access_pod<'a, T: Pod>(&self, index: ThinPtr) -> &'a T {
        if index
            .0
            .checked_add(size_of::<T>())
            .expect("invalid address for pointer")
            > self.main_db_mmap.used_space()
        {
            panic!("invalid pointer value");
        }
        unsafe {
            from_bytes(std::slice::from_raw_parts(
                self.main_db_mmap.map_const_ptr().wrapping_add(index.0),
                size_of::<T>(),
            ))
        }
    }

    /// # Safety:
    /// Caller must ensure no references exists to the requested object
    pub unsafe fn access_pod_mut<'a, T: Pod>(&self, index: ThinPtr) -> &'a mut T {
        if index
            .0
            .checked_add(size_of::<T>())
            .expect("invalid address for pointer")
            > self.main_db_mmap.used_space()
        {
            panic!("invalid pointer value");
        }
        unsafe {
            from_bytes_mut(std::slice::from_raw_parts_mut(
                self.main_db_mmap.map_mut_ptr().wrapping_add(index.0),
                size_of::<T>(),
            ))
        }
    }

    pub fn write(&mut self, index: usize, data: &[u8]) {
        debug_assert!(index + data.len() <= self.main_db_mmap.used_space());
        let fat = FatPtr {
            start: index,
            len: data.len(),
        };
        let target = unsafe { self.access_slice_mut(fat) };
        target.copy_from_slice(data);
    }
    pub fn write_pod<T: Pod>(&self, src: T, dest: &mut T) {
        let dest_index = self.index_of_sized(dest);

        self.undo_log.record(UndoLogEntry::Restore {
            start: dest_index.0,
            data: bytes_of(dest),
        });
        *dest = src;
    }
    pub fn index_of_sized<T: Sized>(&self, t: &T) -> ThinPtr {
        ThinPtr::create(t, self.main_db_mmap.map_const_ptr())
    }
    pub fn index_of<T: Object + ?Sized>(&self, t: &T) -> T::Ptr {
        T::Ptr::create(t, self.main_db_mmap.map_const_ptr())
    }
    pub fn index_of_ptr<T>(&self, t: *const T) -> ThinPtr {
        ThinPtr((t as *const u8 as usize) - (self.main_db_mmap.map_const_ptr() as usize))
    }

    /// Call after writing a message.
    pub fn finalize_message(&mut self, seqnr: SequenceNr) {
        self.registrar_tracker.borrow_mut().finalize_message(seqnr);
    }
    /// Call after a complete update, i.e, applying multiple messages
    /// Returns all messages that can now be removed.
    pub(crate) fn calculate_stale_messages<MSG: Message + Debug>(
        &mut self,
        message_store: &mut OnDiskMessageStore<MSG>,
        is_before_cutoff: bool
    ) -> Result<Vec<SequenceNr>> {
        Ok(self
            .registrar_tracker
            .borrow_mut()
            .calculate_stale_messages(message_store, is_before_cutoff)?
            .into_iter()
            .collect())
    }

    pub fn update_registrar(&self, registrar_point: &mut SequenceNr, opaque: bool) {
        let mut track = self.registrar_tracker.borrow_mut();
        if registrar_point.0 != 0 {
            track.decrease_use(*registrar_point);
        }
        let current_registrar = self.next_seqnr();
        if !opaque {
            track.set_non_opaque(current_registrar);
        }
        track.increase_use(current_registrar);
        drop(track);
        self.write_pod(current_registrar, registrar_point)
    }

    // Signify that the current message has observed data previously written
    // by 'registrar'.
    pub fn observe_registrar(&self, observee: SequenceNr) {
        if self.next_seqnr().is_invalid() {
            return;
        }
        if observee.is_invalid() {
            return;
        }
        let observer = self.next_seqnr();
        if observer != observee {
            self.record_dependency(observee, observer);
        }
    }
}
