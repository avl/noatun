use crate::boot_checksum::get_boot_checksum;
use crate::data_types::RawDatabaseVec;
use crate::disk_abstraction::Disk;
use crate::disk_access::FileAccessor;
use crate::message_store::OnDiskMessageStore;
use crate::undo_store::{HowToProceed, UndoLog, UndoLogEntry};
use crate::{
    bytes_of_mut, bytes_of_mut_uninit, from_bytes, from_bytes_mut, FatPtr, GenPtr, Message,
    NoatunStorable, Object, Pointer, RawFatPtr, SerializableGenPtr, Target, ThinPtr,
};
use anyhow::{bail, Context, Result};
use std::any::{Any, TypeId};
use std::fmt::Debug;
use std::mem::{offset_of, take, transmute_copy, MaybeUninit};
use std::ops::Range;
use std::{iter, slice};

use crate::projection_store::registrar_info::{RegistrarInfo, UnusedInfo};
use crate::sequence_nr::SequenceNr;
use std::pin::Pin;
use tracing::{debug, error, info, trace};

mod registrar_info {
    use crate::sequence_nr::SequenceNr;
    use crate::{DatabaseContextData, NoatunStorable};
    use std::cmp::Ordering;
    use std::fmt::{Debug, Formatter};
    use std::pin::Pin;
    use tracing::debug;

    #[derive(Clone, Copy, Default)]
    #[repr(C)]
    pub(crate) struct RegistrarInfo {
        // mask 0x8000_0000 = tainted
        // mask 0x4000_0000 = wrote non-opaque. I.e, didn't just write opaque values.
        // mask 0x2000_0000 = this sequencenr itself wrote at least one tombstone
        uses: u32,
    }

    unsafe impl NoatunStorable for RegistrarInfo {}

    impl Debug for RegistrarInfo {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.get_use())
        }
    }
    impl RegistrarInfo {
        pub fn tainted(&self) -> bool {
            self.uses & 0x8000_0000 != 0
        }
        pub fn wrote_non_opaques(&self) -> bool {
            self.uses & 0x4000_0000 != 0
        }
        pub fn wrote_tombstones(&self) -> bool {
            /*println!("Retrieve registrar tombstone-user: {:x} @  {:?} = {}",
                     self.uses, &self.uses as *const u32,
                     self.uses & 0x2000_0000 != 0
            );*/
            self.uses & 0x2000_0000 != 0
        }
        pub fn get_use(&self) -> u32 {
            self.uses & 0x1FFF_FFFF
        }
        pub fn increase_use(&mut self, context: &mut DatabaseContextData) {
            if self.get_use() >= 0x1FFF_FFFF {
                return;
            }
            //TODO(future): We could have a special "increment 1" noatun primitive.
            context.write_storable(self.uses + 1, unsafe { Pin::new_unchecked(&mut self.uses) });
        }
        pub fn set_wrote_tombstone(&mut self, context: &mut DatabaseContextData) {
            let mut raw_uses = self.uses;
            if raw_uses & 0x2000_0000 != 0 {
                return;
            }
            raw_uses |= 0x2000_0000;
            /*println!("Mark registrar as tombstone-user: {:x} @  {:?}",
                raw_uses, &self.uses as *const u32
            );*/
            context.write_storable(raw_uses, unsafe { Pin::new_unchecked(&mut self.uses) });
        }
        pub fn decrease_use(
            &mut self,
            context: &mut DatabaseContextData,
            tainted: bool,
            wrote_non_opaques: bool,
        ) {
            let mut raw_uses = self.uses;
            let cur_uses = self.get_use();
            if cur_uses == 0 {
                panic!("Internal error, use count wrong");
            }
            if cur_uses >= 0x1FFF_FFFF {
                // Since we saturate at 0x1FFF_FFFF when adding, we cannot safely decrement.
                // The effect is that a message that touched equal or more than 0x3FFF_FFFF places
                // in memory will never be deleted.
                return;
            }
            raw_uses -= 1;
            if tainted {
                debug!(
                    "mark tainted (raw use={}, ptr = {:x?})",
                    self.uses, &self.uses as *const u32
                );
                raw_uses |= 0x8000_0000;
            }
            if wrote_non_opaques {
                debug!(
                    "mark wrote non-opaque (raw use={}, ptr = {:x?})",
                    self.uses, &self.uses as *const u32
                );
                raw_uses |= 0x4000_0000;
            }
            // TODO(future): We could have a special "decrement 1" noatun primitive.

            context.write_storable(raw_uses, unsafe { Pin::new_unchecked(&mut self.uses) });
        }
    }

    /// Information about a message that has been deemed unused. I.e, it is a candidate
    /// to be pruned. We have detected that none of the data it wrote to the document
    /// remains.
    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    #[repr(C)]
    pub struct UnusedInfo {
        /// The message that finally overwrote the last part of 'seq', meaning
        /// it no longer affects the state. Note that other messages may
        /// in turn depend on this 'last_overwriter', so it's not 100% sure
        /// that 'seq' can be removed.
        ///
        /// Since we know the order in which events occurred, we *know* that
        /// last_overwriter here must be the highest numbered sequence number that
        /// overwrote this registrar.
        ///
        /// This is used in the advance_cutoff function, to know which not-yet-deleted
        /// messages that now have their last-overwriter overtaken by the cutoff-frontier,
        /// so that we can prune messages for the long-term pre-cutoff-life.
        pub last_overwriter: SequenceNr,
        /// The message that is no longer used (to be deleted, possibly).
        /// This is sometimes known as a 'registrar'.
        /// Yes, registrar is a funny word here.
        pub seq: SequenceNr,

        /// There are two required properties for a message to qualify for early deletion:
        ///
        /// #1
        /// None of the overwriters were tainted (i.e., all of them did the overwrite without
        /// having read any of the current state of the db, making them immune to changes
        /// caused by earlier (by time) messages not yet present at the current node.
        ///
        /// Note, this is not sufficient to guarantee that this message could be pruned.
        /// Some as-of-yet not observed read could come and
        /// _read_ the value at an _earlier_ point than now, and smuggle the information
        /// over to some other field. See `wrote_only_opaques`.
        ///
        /// #2
        /// If this message only wrote opaque data. I.e, it only wrote data that
        /// cannot be read during message apply. Example: `OpaqueNoatunCell`.
        ///
        /// This is false for messages that have written non-opaque cells.
        pub can_be_deleted_early: bool,
        pub wrote_tombstone: bool,
        /// Padding, just to make sure this struct has no compiler provided padding,
        /// but rather has explicit padding.
        pub padding: u16,
    }

    impl PartialOrd for UnusedInfo {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.seq.cmp(&other.seq))
        }
    }
    impl Ord for UnusedInfo {
        fn cmp(&self, other: &Self) -> Ordering {
            self.seq.cmp(&other.seq)
        }
    }

    unsafe impl NoatunStorable for UnusedInfo {}
}

const DEFAULT_SIZE: usize = 10000;

// The state was clean in memory, and then a complete sync occurred.
const MAIN_DB_STATUS_FULLY_CLEAN: u8 = 1;

// The database was in a consistent state in memory.
// As long as the machine hasn't been rebooted, the state is clean.
const MAIN_DB_STATUS_HOT_CLEAN: u8 = 1;
// The database state is not clean. Recovery is needed.
const MAIN_DB_STATUS_DIRTY: u8 = 0;

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct MainDbStatus(u8);

unsafe impl NoatunStorable for MainDbStatus {}

/// The header of the main database
#[derive(Debug, Clone, Copy)]
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
    root_ptr: SerializableGenPtr,
}

impl MainDbHeader {
    pub fn is_clean(&self) -> bool {
        self.status.0 == MAIN_DB_STATUS_FULLY_CLEAN
            || (get_boot_checksum() == self.last_boot && self.status.0 == MAIN_DB_STATUS_HOT_CLEAN)
    }
}

unsafe impl NoatunStorable for MainDbHeader {}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct DepTrackEntry {
    dep: ThinPtr,
    reverse_dep: ThinPtr,
}

unsafe impl NoatunStorable for DepTrackEntry {}

#[derive(Default, Debug)]
#[repr(C)]
pub(crate) struct MainDbAuxHeader {
    deptrack_keys: RawDatabaseVec<DepTrackEntry>,
    uses: RawDatabaseVec<RegistrarInfo>,
    unused_messages: RawDatabaseVec<UnusedInfo>,
}
unsafe impl NoatunStorable for MainDbAuxHeader {}

// Note, this type is in a private module and isn't nameable from other crates.
// It has to be public since it's named in methods in the Pointer trait, and the
// Pointer trait must be public because it's implemented by the object macro.
#[doc(hidden)]
pub struct DatabaseContextData {
    main_db_mmap: FileAccessor,
    //root_index: Option<GenPtr>,
    undo_log: UndoLog,

    // The current message being written (or None if not open for writing)
    unused_messages: Vec<UnusedInfo>,
    // Set to true when run from within message apply
    pub(crate) is_mutable: bool,
    pub(crate) is_message_apply: bool,
    // The next message expected to be applied.
    // Starts at 0. When a message is being applied, this field
    // will have the seqnr of the message being applied, not the next one.
    //next_seqnr: SequenceNr,
    filesystem_sync_disabled: bool,
    /// Flag that keeps track of whether the current message has observed
    /// any part of the database. If it has, it's considered tainted, which means
    /// any data it overwrites cannot be considered definitely overwritten until after
    /// the cutoff frontier passes, because some other node _could_ insert data that causes
    /// the tainted message to not overwrite the data in question.
    tainted: bool,
    /// Messages that cleared an item (for example, in a map), must not be pruned.
    ///
    /// The reason is that such messages don't actually have any use count remaining
    /// in the database, but still cannot be removed. The reason they cannot be removed
    /// is that doing so would cause the deleted item to reappear.
    ///
    /// Note, that the act of deleting an item will allow pruning the message that originally
    /// wrote said item. After that has occurred, it is okay to prune the tombstone writer
    /// (because it's not actually writing anything anymore).
    wrote_tombstone: bool,
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

    let align_m1 = align - 1;
    let size_rounded_up = (curr.checked_add(align_m1)?) & !align_m1;
    Some(size_rounded_up)
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub(crate) struct DepTrackLinkedListEntry {
    pub next: ThinPtr,
    pub seq: SequenceNr,
    pub padding: u32,
}
unsafe impl NoatunStorable for DepTrackLinkedListEntry {}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub(crate) struct ReverseDepTrackLinkedListEntry {
    /// MSB = true if this entry (with sequence number = `seq`) can
    /// be deleted early.
    /// Bits 0..62 = ThinPtr (index into backing disk storage) of `seq`
    pub next_and_flag: u64,
    pub seq: SequenceNr,
    pub last_overwriter: SequenceNr,
}

unsafe impl NoatunStorable for ReverseDepTrackLinkedListEntry {}

impl DatabaseContextData {
    pub fn sync_all(&mut self) -> Result<()> {
        self.main_db_mmap.sync_all()?;
        Ok(())
    }
    pub fn clear_tainted(&mut self) {
        self.tainted = false;
    }
    pub fn set_tainted(&mut self) {
        info!("mark self tainted2");
        self.tainted = true;
    }
    fn wrote_tombstone(&self) -> bool {
        self.wrote_tombstone
    }
    pub fn set_wrote_tombstone(&mut self) {
        self.wrote_tombstone = true;
    }
    pub fn clear_wrote_tombstone(&mut self) {
        self.wrote_tombstone = false;
    }
    pub fn is_message_apply(&self) -> bool {
        self.is_message_apply
    }
    pub fn assert_mutable(&self) {
        if !self.is_mutable {
            panic!(
                "Error: Attempt to modify database from outside of Message apply! \
                It is not permissible to modify data in any other case except from \
                the apply method in a Message."
            );
        }
    }
    fn record_dependency(&mut self, observee: SequenceNr, observer: SequenceNr) {
        assert!(observee.is_valid());
        assert!(observer.is_valid());
        self.tainted = true;
        trace!(
            "Recording dependency observer: {:?} observing {:?}",
            observer,
            observee
        );
        // #Safety:
        // No code holds this reference while calling other code that does.
        // Generally, it is not long held. DatabaseContext is neither Sync nor Send.
        let keys = unsafe { self.get_deptrack_keys() };

        if observee.index() >= keys.len() {
            keys.grow(self, observee.index() + 1);
        }
        let key_place = unsafe {
            keys.get_mut(self, observee.index())
                .map_unchecked_mut(|x| &mut x.dep)
        };

        let new_entry: &mut DepTrackLinkedListEntry = self.allocate_internal();

        self.write_storable(*key_place, unsafe {
            Pin::new_unchecked(&mut new_entry.next)
        });
        self.write_storable(observer, unsafe { Pin::new_unchecked(&mut new_entry.seq) });

        let new_entry_index = self.index_of_sized(new_entry);
        self.write_storable(new_entry_index, key_place);
    }
    fn record_reverse_dependency(
        &mut self,
        observee: SequenceNr,
        observer: SequenceNr,
        last_overwriter: SequenceNr,
        can_be_deleted_early: bool,
        wrote_tombstone: bool,
    ) {
        assert!(observee.is_valid());
        assert!(observer.is_valid());

        // #Safety:
        // No code holds this reference while calling other code that does.
        // Generally, it is not long held.
        let keys = unsafe { self.get_deptrack_keys() };

        if observer.index() >= keys.len() {
            keys.grow(self, observer.index() + 1);
        }
        let key_place = unsafe {
            keys.get_mut(self, observer.index())
                .map_unchecked_mut(|x| &mut x.reverse_dep)
        };

        let new_entry: &mut ReverseDepTrackLinkedListEntry = self.allocate_internal();

        // TODO(future): Create more efficient undo-construct for adjacent fields. Can be used
        // here and in other places.

        assert!(key_place.0 < 1 << 62);

        let next_and_flag = key_place.0 as u64
            | (if can_be_deleted_early { 1 << 63 } else { 0 })
            | (if wrote_tombstone { 1 << 62 } else { 0 });
        self.write_storable(next_and_flag, unsafe {
            Pin::new_unchecked(&mut new_entry.next_and_flag)
        });
        self.write_storable(observee, unsafe { Pin::new_unchecked(&mut new_entry.seq) });
        self.write_storable(last_overwriter, unsafe {
            Pin::new_unchecked(&mut new_entry.last_overwriter)
        });

        let new_entry_index = self.index_of_sized(new_entry);
        self.write_storable(new_entry_index, key_place);
    }

    pub(crate) fn read_dependency(
        &self,
        observee: SequenceNr,
    ) -> impl Iterator<Item = SequenceNr> + '_ {
        let keys: &RawDatabaseVec<DepTrackEntry> = &self.get_aux_header().deptrack_keys;

        let mut cur: ThinPtr = if observee.index() < keys.len() {
            unsafe { keys.get_mut(self, observee.index()).dep }
        } else {
            ThinPtr(0)
        };

        iter::from_fn(move || {
            if cur.0 == 0 {
                return None;
            }
            let entry: &DepTrackLinkedListEntry = unsafe { self.access_storable(cur) };
            cur = entry.next;
            Some(entry.seq)
        })
    }

    pub(crate) fn read_reverse_dependency(
        &self,
        observer: SequenceNr,
    ) -> impl Iterator<
        Item = (
            SequenceNr,
            SequenceNr, /* last overwriter */
            bool,       /*can be deleted early*/
            bool,       /*wrote tombstone*/
        ),
    > + '_ {
        let keys: &RawDatabaseVec<DepTrackEntry> = &self.get_aux_header().deptrack_keys;

        let mut cur: ThinPtr = if observer.index() < keys.len() {
            unsafe { keys.get_mut(self, observer.index()).reverse_dep }
        } else {
            ThinPtr(0)
        };

        iter::from_fn(move || {
            if cur.0 == 0 {
                return None;
            }
            let entry: &ReverseDepTrackLinkedListEntry = unsafe { self.access_storable(cur) };
            cur = ThinPtr((entry.next_and_flag & (0x3fff_ffff_ffff_ffff)) as usize);
            let can_be_deleted_early = entry.next_and_flag & (1 << 63) != 0;
            let wrote_tombstone = entry.next_and_flag & (1 << 62) != 0;
            Some((
                entry.seq,
                entry.last_overwriter,
                can_be_deleted_early,
                wrote_tombstone,
            ))
        })
    }

    /// The next sequence number we expect to be added.
    /// I.e, later rewinding to this sequence number would undo the event that carries
    /// this sequece number.
    ///
    /// For an initialized database with no message applied, this method returns #0.
    /// This is because message #0 is the next that is expected to be added.
    ///
    /// Note that it is always possible to rewind to message sequence #0, but not
    /// necessary to any other index, since indices may not always be populated
    /// (only after gc of the index-data structure).
    #[inline(always)]
    pub fn next_seqnr(&self) -> SequenceNr {
        let header: &MainDbHeader =
            unsafe { &*(self.main_db_mmap.map_const_ptr() as *const MainDbHeader) };
        header.next_seqnr
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
        //println!("Rewind to #{}", new_value);
        header.next_seqnr = new_value;
    }

    /// Returns true if database was previously clean
    pub fn mark_dirty(&mut self) -> Result<bool> {
        let header: &mut MainDbHeader =
            unsafe { &mut *(self.main_db_mmap.map_mut_ptr() as *mut MainDbHeader) };

        let was_clean = header.is_clean();
        header.status = MainDbStatus(MAIN_DB_STATUS_DIRTY);

        if !self.filesystem_sync_disabled {
            self.main_db_mmap
                .sync_range(0, std::mem::size_of::<MainDbHeader>())?;
        }

        Ok(was_clean)
    }

    /// Db is clean in memory
    #[inline]
    pub fn mark_hot_clean(&mut self) {
        let header: &mut MainDbHeader =
            unsafe { &mut *(self.main_db_mmap.map_mut_ptr() as *mut MainDbHeader) };

        header.status = MainDbStatus(MAIN_DB_STATUS_HOT_CLEAN);
    }
    #[inline]
    pub fn mark_fully_clean(&mut self) -> Result<()> {
        let header: &mut MainDbHeader =
            unsafe { &mut *(self.main_db_mmap.map_mut_ptr() as *mut MainDbHeader) };

        header.status = MainDbStatus(MAIN_DB_STATUS_FULLY_CLEAN);
        self.main_db_mmap
            .sync_range(0, std::mem::size_of::<MainDbHeader>())?;
        Ok(())
    }
    pub(crate) fn disable_filesystem_sync(&mut self) {
        self.filesystem_sync_disabled = true;
    }

    #[inline]
    pub fn is_dirty(&self) -> bool {
        let header: &MainDbHeader =
            unsafe { &*(self.main_db_mmap.map_mut_ptr() as *const MainDbHeader) };

        !header.is_clean()
    }
    pub fn clear(&mut self) -> Result<()> {
        self.main_db_mmap.truncate(0)?;
        self.main_db_mmap
            .grow(size_of::<MainDbHeader>() + size_of::<MainDbAuxHeader>())?;
        Self::write_initial_header(&mut self.main_db_mmap);
        self.write_initial_aux_header();
        self.undo_log.clear()?;
        self.unused_messages.clear();

        Ok(())
    }

    pub fn clear_unused_tracking(&mut self) {
        self.unused_messages.clear();
    }

    pub(crate) fn get_aux_header(&self) -> &MainDbAuxHeader {
        let slice = self
            .main_db_mmap
            .map_const_ptr()
            .wrapping_add(size_of::<MainDbHeader>());
        let slice = unsafe { std::slice::from_raw_parts(slice, size_of::<MainDbAuxHeader>()) };
        let aux_header: &MainDbAuxHeader = from_bytes(slice);
        aux_header
    }
    unsafe fn get_deptrack_keys<'a>(&self) -> &'a mut RawDatabaseVec<DepTrackEntry> {
        unsafe {
            &mut *(self.main_db_mmap.map_mut_ptr().wrapping_add(
                size_of::<MainDbHeader>() + offset_of!(MainDbAuxHeader, deptrack_keys),
            ) as *mut RawDatabaseVec<DepTrackEntry>)
        }
    }
    pub(crate) unsafe fn get_unused_list<'a>(&self) -> &'a mut RawDatabaseVec<UnusedInfo> {
        unsafe {
            &mut *(self.main_db_mmap.map_mut_ptr().wrapping_add(
                size_of::<MainDbHeader>() + offset_of!(MainDbAuxHeader, unused_messages),
            ) as *mut RawDatabaseVec<UnusedInfo>)
        }
    }

    unsafe fn get_uses<'a>(&self) -> &'a mut RawDatabaseVec<RegistrarInfo> {
        unsafe {
            &mut *(self
                .main_db_mmap
                .map_mut_ptr()
                .wrapping_add(size_of::<MainDbHeader>() + offset_of!(MainDbAuxHeader, uses))
                as *mut RawDatabaseVec<RegistrarInfo>)
        }
    }

    pub(crate) fn write_initial_aux_header(&mut self) {
        let aux_header: &mut MainDbAuxHeader = from_bytes_mut(
            &mut self.main_db_mmap.map_mut()[size_of::<MainDbHeader>()
                ..size_of::<MainDbHeader>() + size_of::<MainDbAuxHeader>()],
        );
        *aux_header = MainDbAuxHeader::default();
        assert!(self.pointer() >= size_of::<MainDbHeader>() + size_of::<MainDbAuxHeader>());
    }

    fn write_initial_header(mmap: &mut FileAccessor) {
        assert_eq!(
            mmap.used_space(),
            size_of::<MainDbHeader>() + size_of::<MainDbAuxHeader>()
        );
        let header: &mut MainDbHeader =
            from_bytes_mut(&mut mmap.map_mut()[0..size_of::<MainDbHeader>()]);
        header.next_seqnr = SequenceNr::INVALID;

        header.status = MainDbStatus(MAIN_DB_STATUS_DIRTY);
        header.usize_size = size_of::<usize>()
            .try_into()
            .expect("The size of an 'usize' must be less than 256 bytes");

        header.last_boot = get_boot_checksum();
    }

    pub(crate) fn new<S: Disk>(s: &mut S, name: &Target, max_size: usize) -> Result<Self> {
        let (mut main_db_file, _existed) = s
            .open_file(name, "maindb", 0, max_size)
            .context("opening main store file")?;

        let mut is_new = false;
        if main_db_file.used_space() < size_of::<MainDbHeader>() + size_of::<MainDbAuxHeader>() {
            main_db_file
                .grow(size_of::<MainDbHeader>() + size_of::<MainDbAuxHeader>())
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

        let mut t = Self {
            main_db_mmap: main_db_file,
            undo_log: UndoLog::new(s, name, max_size)?,
            unused_messages: Vec::default(),
            is_mutable: false,
            is_message_apply: false,
            filesystem_sync_disabled: false,
            tainted: false,
            wrote_tombstone: false,
        };
        // It's a bit of a code smell that at this precise point in the execution,
        // a DatabaseContext exists, but it's not actually fully initialized until we write the
        // aux header. This is functionally correct, no-one can observe this half-initialized
        // DatabaseContext at this point in the code. But ideally we'd find a way to not have a
        // half-initialized object. The trick is that writing the initial header requires
        // allocation, which we currently can't do without a (partially) initialized
        // DatabaseContext
        if is_new {
            t.write_initial_aux_header();
        }
        Ok(t)
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
        if self.next_seqnr() <= new_time {
            return;
        }

        info!("Rewinding from {} to {:?}", self.next_seqnr(), new_time);
        //println!("Rewinding from {} to {:?}", self.next_seqnr(), new_time);
        let result = self.undo_log.rewind(|entry| match entry {
            UndoLogEntry::SetPointer(new_pointer) => {
                let cur = Self::pointer_of(&self.main_db_mmap);
                debug_assert!(new_pointer <= cur);
                unsafe {
                    Self::mut_byte_slice(self.main_db_mmap.map_mut_ptr(), new_pointer..cur).fill(0)
                };
                Self::set_pointer_of(&self.main_db_mmap, new_pointer);
                HowToProceed::PopAndContinue
            }
            UndoLogEntry::ZeroOut { start, len } => {
                unsafe {
                    Self::mut_byte_slice(self.main_db_mmap.map_mut_ptr(), start..start + len)
                        .fill(0)
                };
                HowToProceed::PopAndContinue
            }
            /*UndoLogEntry::RestoreUninit { start, data } => {
                unsafe {
                    Self::mut_slice_uninit(self.main_db_mmap.map_mut_ptr_uninit(), start..start + data.len())
                        .copy_from_slice(data)
                };
                HowToProceed::PopAndContinue
            }*/
            UndoLogEntry::RestorePod { start, data } => {
                unsafe {
                    Self::mut_byte_slice(self.main_db_mmap.map_mut_ptr(), start..start + data.len())
                        .copy_from_slice(data)
                };
                HowToProceed::PopAndContinue
            }
            UndoLogEntry::Rewind(time) => {
                if time == new_time {
                    Self::raw_set_next_seqnr_of(&self.main_db_mmap, new_time);
                    HowToProceed::DontPopAndStop
                } else if time > new_time {
                    HowToProceed::PopAndContinue
                } else {
                    error!(
                        "Couldn't rewind time to {}, ended up back at {}",
                        new_time, time
                    );

                    panic!("Couldn't rewind time to {new_time}, ended up back at {time}");
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
    pub fn start_ptr_mut(&self) -> *mut u8 {
        self.main_db_mmap.map_mut_ptr()
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
        let header: &mut MainDbHeader =
            from_bytes_mut(&mut self.main_db_mmap.map_mut()[0..size_of::<MainDbHeader>()]);

        header.root_ptr = genptr.into();
    }

    pub fn get_root_ptr<Ptr: Pointer + Any + 'static>(&self) -> Ptr {
        let root_ptr = unsafe {
            *(self
                .main_db_mmap
                .map_mut_ptr()
                .wrapping_add(offset_of!(MainDbHeader, root_ptr))
                as *mut SerializableGenPtr)
        };
        if root_ptr.ptr == 0 {
            panic!("Invalid root pointer!");
        }
        let root_ptr: GenPtr = root_ptr.into();

        match root_ptr {
            GenPtr::Thin(ptr) => {
                if TypeId::of::<Ptr>() == TypeId::of::<ThinPtr>() {
                    return unsafe { transmute_copy(&ptr) };
                }
                panic!(
                    "Wrong type of root pointer in database. Has schema changed significantly since last access?"
                );
            }
            GenPtr::Fat(ptr) => {
                if TypeId::of::<Ptr>() == TypeId::of::<FatPtr>() {
                    return unsafe { transmute_copy(&ptr) };
                }
                panic!(
                    "Wrong type of root pointer in database. Has schema changed significantly since last access?"
                );
            }
        }
    }
    pub fn zero(&mut self, dst: FatPtr) {
        unsafe {
            //dbg!(&source, &dest_index);

            self.undo_log.record(UndoLogEntry::RestorePod {
                start: dst.start,
                data: self.access_slice_mut(dst),
            });

            let dest = self.access_slice_mut::<u8>(FatPtr {
                start: dst.start,
                count: dst.count,
            });

            dest.fill(0);
        }
    }
    pub fn copy_bytes(&mut self, source: FatPtr, dest_index: ThinPtr) {
        unsafe {
            self.undo_log.record(UndoLogEntry::RestorePod {
                start: dest_index.0,
                data: self.access_slice_mut(FatPtr::from_idx_count(dest_index.0, source.count)),
            });

            let dest = self.access_slice_mut(FatPtr {
                start: dest_index.0,
                count: source.count,
            });

            let src = self.access_slice::<u8>(source);

            dest.copy_from_slice(src);
        }
    }

    pub fn copy_bytes_len(&mut self, source: ThinPtr, dest_index: ThinPtr, num_bytes: usize) {
        self.copy_bytes(FatPtr::from_idx_count(source.0, num_bytes), dest_index)
    }
    pub fn copy_storable<T: NoatunStorable>(&mut self, source: &T, dest: &mut T) {
        let dest_index = self.index_of_sized(dest);
        self.undo_log.record(UndoLogEntry::RestorePod {
            start: dest_index.0,
            data: bytes_of_mut(dest),
        });
        dest.copy_from(source);
    }
    pub fn copy_uninit<T: NoatunStorable>(&mut self, source: &T, dest: &mut MaybeUninit<T>) {
        let dest_index = self.index_of_sized(dest);
        self.undo_log.record(UndoLogEntry::RestorePod {
            start: dest_index.0,
            data: bytes_of_mut_uninit(dest),
        });
        T::initialize(dest, source);
    }
    pub fn allocate_storable<T: NoatunStorable>(&mut self) -> Pin<&mut T> {
        let bytes = self.allocate_raw(std::mem::size_of::<T>(), std::mem::align_of::<T>());
        unsafe { Pin::new_unchecked(&mut *(bytes as *mut T)) }
    }
    pub fn allocate_obj<T: Object>(&mut self) -> Pin<&mut T> {
        let bytes = self.allocate_raw(std::mem::size_of::<T>(), std::mem::align_of::<T>());
        unsafe { Pin::new_unchecked(&mut *(bytes as *mut T)) }
    }
    pub(crate) fn allocate_internal<'a, T: NoatunStorable>(&mut self) -> &'a mut T {
        let bytes = self.allocate_raw(std::mem::size_of::<T>(), std::mem::align_of::<T>());
        unsafe { &mut *(bytes as *mut T) }
    }
    pub fn allocate_raw(&mut self, size: usize, align: usize) -> *mut u8 {
        if align > 256 {
            panic!("Noatun arbitrarily does not support types with alignment > 256");
        }
        let main_db_ptr = self.main_db_mmap.map_mut_ptr();
        // Ensure that main_db_ptr is always 16 bytes offset from a 256-byte alignment boundary.
        // This is so that we're sure that process restarts won't destroy alignment
        debug_assert_eq!((main_db_ptr as usize - 16) % 256, 0);

        // Calculate real address in memory. This is the address that must respect
        // the alignment request.
        let raw_ptr_usize = main_db_ptr as usize + self.pointer();
        let alignment_adjusted_usize =
            index_rounded_up_to_custom_align(raw_ptr_usize, align).unwrap();
        let alignment_adjusted = alignment_adjusted_usize - (main_db_ptr as usize);
        self.undo_log
            .record(UndoLogEntry::SetPointer(self.pointer()));

        let new_pointer = alignment_adjusted.checked_add(size).unwrap();
        self.main_db_mmap
            .grow(new_pointer)
            .expect("Failed to allocate memory");
        main_db_ptr.wrapping_add(alignment_adjusted)
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
    pub unsafe fn access_slice_at<'a, T: NoatunStorable>(
        &self,
        offset: usize,
        size: usize,
    ) -> &'a [T] {
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
    /// The source must not contain any uninitialized bytes.
    pub unsafe fn access_slice_at_mut<'a, T: NoatunStorable>(
        &self,
        offset: usize,
        size: usize,
    ) -> &'a mut [T] {
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
    pub unsafe fn access_slice<'a, T: NoatunStorable>(&self, range: FatPtr) -> &'a [T] {
        assert!(range.start + range.count * size_of::<T>() <= self.main_db_mmap.used_space());
        unsafe {
            std::slice::from_raw_parts(
                self.main_db_mmap.map_const_ptr().wrapping_add(range.start) as *const T,
                range.count,
            )
        }
    }

    /// # Safety
    /// The returned range must not overlap any other reference
    /// Alignment must be right.
    pub unsafe fn access_slice_mut<'a, T: NoatunStorable>(&self, range: FatPtr) -> &'a mut [T] {
        assert!(range.start + range.count * size_of::<T>() <= self.main_db_mmap.used_space());
        unsafe {
            std::slice::from_raw_parts_mut(
                self.main_db_mmap.map_mut_ptr().wrapping_add(range.start) as *mut T,
                range.count,
            )
        }
    }

    /// # Safety
    /// The given range must point to valid memory, and must not overlap any other reference
    pub unsafe fn mut_byte_slice<'a>(data: *mut u8, range: Range<usize>) -> &'a mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(data.wrapping_add(range.start), range.end - range.start)
        }
    }

    /// # Safety
    /// Caller must ensure no mutable reference exists to the requested object
    pub unsafe fn access_storable<'a, T: NoatunStorable>(&self, index: ThinPtr) -> &'a T {
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

    /// # Safety
    /// Caller must ensure no mutable reference exists to the requested object
    #[inline]
    pub unsafe fn access_thin<'a, T: ?Sized>(&self, ptr: ThinPtr) -> &'a T {
        assert_eq!(size_of::<&T>(), size_of::<*const u8>());
        let ret = unsafe { transmute_copy(&self.main_db_mmap.map_const_ptr().wrapping_add(ptr.0)) };

        // Note: If the below fails, there has already been UB (because we'd already have produced
        // a reference that overlaps invalid memory. However, this check can be best-effort,
        // since this method is unsafe, and it is up to the caller to ensure validity.
        if ptr
            .0
            .checked_add(size_of_val(ret))
            .expect("invalid address for pointer")
            > self.main_db_mmap.used_space()
        {
            panic!("invalid pointer value");
        }

        ret
    }
    /// # Safety
    /// Caller must ensure no mutable or shared reference exists to the requested object
    #[inline]
    pub unsafe fn access_thin_mut<'a, T: ?Sized>(&self, ptr: ThinPtr) -> &'a mut T {
        assert_eq!(size_of::<&mut T>(), size_of::<*const u8>());
        let ret: &mut _ =
            unsafe { transmute_copy(&self.main_db_mmap.map_mut_ptr().wrapping_add(ptr.0)) };

        // Note: If the below fails, there has already been UB (because we'd already have produced
        // a reference that overlaps invalid memory. However, this check can be best-effort,
        // since this method is unsafe, and it is up to the caller to ensure validity.
        if ptr
            .0
            .checked_add(size_of_val(ret))
            .expect("invalid address for pointer")
            > self.main_db_mmap.used_space()
        {
            panic!("invalid pointer value");
        }
        ret
    }

    /// # Safety
    /// Caller must ensure no mutable reference exists to the requested object
    #[inline]
    pub unsafe fn access_fat<'a, T: ?Sized>(&self, ptr: FatPtr) -> &'a T {
        assert_eq!(size_of::<&T>(), 2 * size_of::<*const u8>());
        let raw = RawFatPtr {
            data: self.main_db_mmap.map_const_ptr().wrapping_add(ptr.start),
            size: ptr.count,
        };
        let ret = unsafe { transmute_copy(&raw) };

        println!("Raw: {ptr:#?}: {raw:#?}");
        println!("size-of: {}", size_of_val(ret));
        println!("T: {}", std::any::type_name::<T>());

        // Note: If the below fails, there has already been UB (because we'd already have produced
        // a reference that overlaps invalid memory. However, this check can be best-effort,
        // since this method is unsafe, and it is up to the caller to ensure validity.
        if ptr
            .start
            .checked_add(size_of_val(ret))
            .expect("invalid address for pointer")
            > self.main_db_mmap.used_space()
        {
            panic!("invalid pointer value");
        }

        ret
    }

    /// # Safety
    /// Caller must ensure no mutable reference exists to the requested object
    #[inline]
    pub unsafe fn access_fat_mut<'a, T: ?Sized>(&self, ptr: FatPtr) -> &'a mut T {
        assert_eq!(size_of::<&T>(), 2 * size_of::<*const u8>());
        let raw = RawFatPtr {
            data: self.main_db_mmap.map_mut_ptr().wrapping_add(ptr.start),
            size: ptr.count,
        };
        let ret: &mut _ = unsafe { transmute_copy(&raw) };

        // Note: If the below fails, there has already been UB (because we'd already have produced
        // a reference that overlaps invalid memory. However, this check can be best-effort,
        // since this method is unsafe, and it is up to the caller to ensure validity.
        if ptr
            .start
            .checked_add(size_of_val(ret))
            .expect("invalid address for pointer")
            > self.main_db_mmap.used_space()
        {
            panic!("invalid pointer value");
        }

        ret
    }

    /// # Safety
    /// Caller must ensure no references exists to the requested object
    pub unsafe fn access_storable_mut<'a, T: NoatunStorable>(
        &self,
        index: ThinPtr,
    ) -> Pin<&'a mut T> {
        if index
            .0
            .checked_add(size_of::<T>())
            .expect("invalid address for pointer")
            > self.main_db_mmap.used_space()
        {
            panic!("invalid pointer value");
        }
        unsafe {
            let ptr = self.main_db_mmap.map_mut_ptr().wrapping_add(index.0);
            assert!((ptr as *mut T).is_aligned());
            Pin::new_unchecked(from_bytes_mut(std::slice::from_raw_parts_mut(
                ptr,
                size_of::<T>(),
            )))
        }
    }

    pub fn write_storable<T: NoatunStorable>(&mut self, src: T, dest: Pin<&mut T>) {
        let dest = unsafe { dest.get_unchecked_mut() };
        let dest_index = self.index_of_sized(dest);

        self.undo_log.record(UndoLogEntry::RestorePod {
            start: dest_index.0,
            data: bytes_of_mut(dest),
        });
        *dest = src;
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)] //False positive, we check the bounds
    pub fn write_storable_ptr<T: NoatunStorable>(&mut self, src: T, dest: *mut T) {
        let dest_index = self.index_of_ptr(dest);
        assert!(dest_index.0 + size_of::<T>() <= self.main_db_mmap.used_space());

        self.undo_log.record(UndoLogEntry::RestorePod {
            start: dest_index.0,
            data: unsafe { slice::from_raw_parts_mut(dest as *mut u8, size_of::<T>()) },
        });
        unsafe { dest.write_unaligned(src) };
    }
    pub fn index_of_sized<T: Sized>(&self, t: &T) -> ThinPtr {
        ThinPtr::create(t, self.main_db_mmap.map_const_ptr())
    }
    pub fn index_of<T: Object + ?Sized>(&self, t: &T) -> T::Ptr {
        T::Ptr::create(t, self.main_db_mmap.map_const_ptr())
    }
    pub(crate) fn index_of_rel<T: Object + ?Sized>(mmap_ptr: *const u8, t: &T) -> T::Ptr {
        T::Ptr::create(t, mmap_ptr)
    }
    pub fn index_of_ptr<T>(&self, t: *const T) -> ThinPtr {
        ThinPtr((t as *const u8 as usize).wrapping_sub(self.main_db_mmap.map_const_ptr() as usize))
    }

    /// Call after writing a message.
    pub fn finalize_message(&mut self, seqnr: SequenceNr) {
        self.rt_finalize_message(seqnr);
    }
    /// Call after a complete update, i.e, applying multiple messages
    /// Returns all messages that can now be removed.
    pub(crate) fn calculate_stale_messages<MSG: Message + Debug>(
        &mut self,
        message_store: &mut OnDiskMessageStore<MSG>,
    ) -> Result<Vec<SequenceNr>> {
        Ok(self
            .first_stale_message_step(message_store)?
            .into_iter()
            .collect())
    }

    pub fn update_registrar(&mut self, registrar_point: &mut SequenceNr, opaque: bool) {
        let current_registrar = self.next_seqnr();
        if current_registrar == *registrar_point {
            return; // Updating registrar to same value must not transiently free use and then re-add, it should be a no-op, like this!
        }
        if registrar_point.is_valid() {
            self.rt_decrease_use(
                *registrar_point,
                current_registrar,
                self.tainted,
                !opaque
            );
        }
        if current_registrar.is_invalid() {
            // We're in the 'initialize root' method
            return;
        }
        self.rt_increase_use(current_registrar);

        self.write_storable(current_registrar, Pin::new(registrar_point))
    }

    pub fn update_registrar_ptr_impl(
        &mut self,
        registrar_point: *mut SequenceNr,
        actor: SequenceNr,
        actor_tainted: bool,
        actor_wrote_non_opaque: bool,
    ) {
        let registrar_point_value = unsafe { registrar_point.read_unaligned() };
        let current_registrar = actor;
        if current_registrar == registrar_point_value {
            return; // Updating registrar to same value must not transiently free use and then re-add, it should be a no-op, like this!
        }
        let is_valid = registrar_point_value.is_valid();
        if is_valid {
            self.rt_decrease_use(
                registrar_point_value,
                current_registrar,
                actor_tainted,
                actor_wrote_non_opaque
            );
        }
        if current_registrar.is_invalid() {
            if is_valid {
                self.write_storable_ptr(current_registrar, registrar_point);
            }
            // We're in the 'initialize root' method
            return;
        }
        self.rt_increase_use(current_registrar);

        self.write_storable_ptr(current_registrar, registrar_point)
    }
    pub fn update_registrar_ptr(&mut self, registrar_point: *mut SequenceNr, opaque: bool) {
        self.update_registrar_ptr_impl(
            registrar_point,
            self.next_seqnr(),
            self.tainted,
            !opaque,
        );
    }
    pub fn clear_registrar_ptr(&mut self, registrar_point: *mut SequenceNr, opaque: bool) {
        self.wrote_tombstone = true;
        self.update_registrar_ptr_impl(
            registrar_point,
            SequenceNr::INVALID,
            self.tainted,
            !opaque,
        );
    }

    // Signify that the current message has observed data previously written
    // by 'registrar'.
    pub fn observe_registrar(&mut self, observee: SequenceNr) {
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

    pub(crate) fn rt_finalize_message(&mut self, message_seqnr: SequenceNr) {
        debug_assert!(message_seqnr.is_valid());
        //let aux_header = self.get_aux_header();

        // #SAFETY
        // We only hold this for this method, and we call no other code that
        // uses the same memory. So do all other users of 'get_uses'.
        let uses = unsafe { self.get_uses() };

        if uses.len() <= message_seqnr.index() {
            // This is a bit of a special case. This is a message
            // that did not actually modify any state at all during its projection.
            trace!(
                "Message modified nothing: {:?} (tainted: {})",
                message_seqnr,
                self.tainted
            );
            self.unused_messages.push(UnusedInfo {
                seq: message_seqnr,
                last_overwriter: message_seqnr,
                can_be_deleted_early: !self.tainted,
                wrote_tombstone: self.wrote_tombstone,
                padding: 0,
            });
            return;
        }
        let mut track = unsafe { uses.get_mut(self, message_seqnr.index()) };

        if self.wrote_tombstone {
            // TODO: We probably only need to do this write if get_use() != 0 below.
            // In other cases, I believe nothing ever reads this `uses` slot.
            unsafe {
                track.as_mut().get_unchecked_mut().set_wrote_tombstone(self);
            }
        }

        if track.get_use() == 0 {
            // Same special case as above - message is not in use, even immediately
            // after having been projected. This is currently impossible, since 'uses'
            // was increased, this must mean that data was written, but since the 'use' is
            // 0, this means something else overwrote that data. Which is not possible.
            trace!(
                "Message modified nothing2: {:?} (tainted: {})",
                message_seqnr,
                self.tainted
            );
            self.unused_messages.push(UnusedInfo {
                seq: message_seqnr,
                last_overwriter: message_seqnr,
                can_be_deleted_early: !self.tainted,
                wrote_tombstone: self.wrote_tombstone,
                padding: 0,
            });
        }
    }

    /*
    About deletions:

    We can delete messages that no longer have any effect on the state, but only in these
    circumstances:

     1: Messages that only updated OpaqueData, did not read from the database, and whose
        written data was itself overwritten unconditionally (overwritten by a message before that
        message raid any database state).
     2: Messages that have never been transmitted, and upon which no existing message depends
     3: Messages that are older than MAX_PARTITION_TIME, and have no trace in the state at
        MAX_PARTITION_TIME
    */

    /// Called in two situations:
    /// 1) Immediately when noticing a message is stale
    /// 2) During advancement of the cutoff time.
    pub(crate) fn rt_calculate_stale_messages_impl<M: Message + Debug>(
        &mut self,
        messages: &mut OnDiskMessageStore<M>,
        mut unused_messages: Vec<UnusedInfo>,
        // TODO: It may seem this should be determined by looking at id:s, not by boolean.
        // However, any such refactor should probably be aware of the fact that the `before_cutoff`
        // currently depends on HOW we ended up here. It is true when called from within the
        // advance_cutoff mechanism.
        // TODO: Check what happens if we receive a message that is old enough to be before
        // cutoff, or which makes messages before cutoff stale.
        before_cutoff: bool,
    ) -> anyhow::Result<Vec<SequenceNr>> {
        let mut deleted = Vec::new();
        let mut deferred = Vec::new();
        let mut new_unused_list = Vec::new();
        trace!("Unused batch: {:?}", unused_messages);
        if unused_messages.is_empty() {
            return Ok(vec![]);
        }
        trace!("Calculating staleness, cutoff: {:?}", before_cutoff);
        #[cfg(debug_assertions)]
        {
            debug!(
                "Total message-list: {:#?}",
                messages.get_all_messages_with_children().unwrap()
            );
        }
        'outer: while let Some(msg) = unused_messages.pop() {
            //TODO: Don't do this read here? It's only used for logging, which seems inefficient
            let msgobj = messages.read_message_header_and_children_by_index(msg.seq);
            debug!("considering {:?} = {:?} for deletion", msgobj, msg);
            debug!(
                "unconditionally overwritten: {:?}",
                msg.can_be_deleted_early
            );

            // Condition 1 (referenced below)
            if (!msg.wrote_tombstone
                && (!messages.may_have_been_transmitted(msg.seq)? || msg.can_be_deleted_early))
                || before_cutoff
            {
                for observer in self.read_dependency(msg.seq) {
                    debug!("considered its observer {:?}", observer);
                    if !deleted.contains(&observer) {
                        debug_assert!(!msg.can_be_deleted_early, "opaque data can't be observed, so a message that wrote only opaques must have no observers. And a message that didn't write only opaques, will have can_be_deleted_early == false");

                        // 'msg' can't be deleted, because it's observed by
                        // 'observer' - i.e a later message that has not been deleted.
                        debug!(
                            "can't delete {:?}/{:?} because of observer {:?}",
                            msgobj.map(|x2| x2.map(|x| x.0.id)),
                            msg,
                            observer
                        );

                        // The things 'deferred' are carried out at the end of this function (i.e, quickly)
                        deferred.push(move |tself: &mut DatabaseContextData| {
                            //println!("Record reverse dependency for {:?}. Wrote tombstone: {}", msg.seq, msg.wrote_tombstone);
                            // Remember/record_reverse_dependency
                            tself.record_reverse_dependency(
                                msg.seq,
                                observer,
                                msg.last_overwriter,
                                msg.can_be_deleted_early,
                                msg.wrote_tombstone,
                            );
                        });

                        continue 'outer;
                    }
                }
            } else {
                debug!("can't delete {:?}{:?} yet because it's been transmitted and is after cutoff: {:?} and not unconditionally overwritten", msgobj.map(|x2|x2.map(|x|x.0.id)), msg, before_cutoff);
                new_unused_list.push(msg);
                continue 'outer;
            }

            info!(
                "Deleting {:?} (before cutoff: {:?}), may have been transmitted: {:?}",
                msg,
                before_cutoff,
                messages.may_have_been_transmitted(msg.seq)?
            );
            for (revdep, last_overwriter, can_be_deleted_early, wrote_tombstone) in
                self.read_reverse_dependency(msg.seq)
            {
                //println!("Read reverse dependency for {:?}. Wrote tombstone: {}", revdep, wrote_tombstone);
                // Get messages that depend on the message that we just decided to delete
                unused_messages.push(UnusedInfo {
                    seq: revdep,
                    last_overwriter,
                    can_be_deleted_early,
                    wrote_tombstone,
                    padding: 0,
                });
            }

            deleted.push(msg.seq);
        }
        let unused_list = unsafe { self.get_unused_list() };

        for new_unused in new_unused_list.iter().rev() {
            //println!("Pushing unused: {:?}", new_unused);
            unused_list.push_untracked(self, *new_unused);
        }

        for action in deferred {
            action(self);
        }

        Ok(deleted)
    }

    /// Called immediately after noticing a message has no live written data.
    /// In some cases, the message can be removed immediately (non-transmitted or
    /// opaque for example).
    pub(crate) fn first_stale_message_step<M: Message + Debug>(
        &mut self,
        messages: &mut OnDiskMessageStore<M>,
    ) -> anyhow::Result<Vec<SequenceNr>> {
        let mut unused_messages = take(&mut self.unused_messages);
        unused_messages.sort(); //Sort in seq-nr order
        self.rt_calculate_stale_messages_impl(messages, unused_messages, false)
    }
    pub(crate) fn rt_increase_use(&mut self, registrar: SequenceNr) {
        let uses = unsafe { self.get_uses() };
        if uses.len() <= registrar.index() {
            uses.grow(self, registrar.index() + 1);
        }
        let mut info = unsafe { uses.get_mut(self, registrar.index()) };
        info.increase_use(self);
        trace!(
            "increased use of {:?} to {} (tainted:{})",
            registrar,
            info.get_use(),
            info.tainted()
        );
    }

    pub(crate) fn rt_decrease_use(
        &mut self,
        registrar: SequenceNr,
        overwriter: SequenceNr,
        overwriter_tainted: bool,
        wrote_non_opaque: bool
    ) {
        let uses = unsafe { self.get_uses() };
        let mut cur = unsafe { uses.get_mut(self, registrar.index()) };
        let cur_use = cur.get_use();
        if cur_use == 0 {
            panic!("Corrupt use count for sequence nr {registrar:?}, use = {cur_use}");
        }

        unsafe {
            cur.as_mut().get_unchecked_mut().decrease_use(
                self,
                overwriter_tainted,
                wrote_non_opaque,
            )
        };
        trace!(
            "decreased use of {:?} is {} (taint:{}) (because overwriter: {:?}(tainted:{}))",
            registrar,
            cur.get_use(),
            cur.tainted(),
            overwriter,
            overwriter_tainted
        );
        if cur.get_use() == 0 {
            // This is the normal way messages end up in 'unused_messages'
            trace!(
                "Adding {:?} as unused (overwriter.tainted: {}, registrar tainted: {}, cur tombstone: {})",
                registrar,
                overwriter_tainted,
                cur.tainted(),
                cur.wrote_tombstones()
            );
            self.unused_messages.push(UnusedInfo {
                seq: registrar,
                //opaque: cur.get_opaque() as u32,
                last_overwriter: overwriter,
                // ALL overwriters must be non-tainted. There can't be a single one that is.
                // `cur.tainted()` tracks this per registrar.
                can_be_deleted_early: !cur.tainted() && !cur.wrote_non_opaques(),
                wrote_tombstone: cur.wrote_tombstones(),
                padding: 0,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::disk_abstraction::InMemoryDisk;
    use crate::sequence_nr::SequenceNr;
    use crate::{DatabaseContextData, Target};
    use std::time::Instant;
    #[test]
    fn smoke_deptrack() {
        let mut disk = InMemoryDisk::default();
        let mut tracker =
            DatabaseContextData::new(&mut disk, &Target::CreateNew("ctx".into()), 10000).unwrap();

        tracker.record_dependency(SequenceNr::from_index(1), SequenceNr::from_index(2));

        let result: Vec<_> = tracker.read_dependency(SequenceNr::from_index(1)).collect();
        assert_eq!(result, vec![SequenceNr::from_index(2)]);
    }

    #[test]
    fn smoke_deptrack_many() {
        let mut disk = InMemoryDisk::default();
        let mut tracker =
            DatabaseContextData::new(&mut disk, &Target::CreateNew("ctx".into()), 10000).unwrap();

        let t = Instant::now();
        for i in 0..100_usize {
            tracker.record_dependency(
                SequenceNr::from_index((i as f64).sqrt() as usize),
                SequenceNr::from_index(i),
            );
        }
        println!("Time: {:?}", t.elapsed());

        let result: Vec<_> = tracker
            .read_dependency(SequenceNr::from_index(8))
            .map(|x| x.index())
            .collect();
        assert_eq!(
            result,
            vec![80, 79, 78, 77, 76, 75, 74, 73, 72, 71, 70, 69, 68, 67, 66, 65, 64]
        );
    }
}
