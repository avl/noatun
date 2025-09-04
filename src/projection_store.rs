use crate::boot_checksum::get_boot_checksum;
use crate::data_types::{NoatunUntrackedCell, NoatunVecRaw, RawDatabaseVec};
use crate::disk_abstraction::{Disk, DISK_MAP_ALIGNMENT};
use crate::disk_access::FileAccessor;
use crate::message_store::OnDiskMessageStore;
use crate::undo_store::{HowToProceed, UndoLog, UndoLogEntry};
use crate::{
    bytes_of_mut, dprintln, from_bytes, from_bytes_mut, FatPtr, GenPtr, Message, NoatunStorable,
    Object, Pointer, RawFatPtr, SchemaHasher, SerializableGenPtr, Target, ThinPtr,
};
use anyhow::{bail, Context, Result};
use std::any::{Any, TypeId};
use std::fmt::Debug;
use std::mem::{offset_of, transmute_copy};
use std::ops::Range;
use std::slice;

use crate::projection_store::registrar_info::{RegistrarInfo, UnusedInfo};
use crate::sequence_nr::{SequenceNr, Tracker};
use std::pin::Pin;

use tracing::{error, info, trace};

mod registrar_info {
    use crate::sequence_nr::SequenceNr;
    use crate::{DatabaseContextData, NoatunStorable, SchemaHasher};
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

    /// Safety: RegistrarInfo contains only NoatunStorable types
    unsafe impl NoatunStorable for RegistrarInfo {
        fn hash_schema(hasher: &mut SchemaHasher) {
            hasher.write_str("noatun::RegistrarInfo /1")
        }
    }

    impl Debug for RegistrarInfo {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.get_use())
        }
    }
    impl RegistrarInfo {
        pub fn overwriter_tainted(&self) -> bool {
            self.uses & 0x8000_0000 != 0
        }
        pub fn wrote_non_opaques(&self) -> bool {
            self.uses & 0x4000_0000 != 0
        }
        pub fn wrote_tombstones(&self) -> bool {
            self.uses & 0x2000_0000 != 0
        }
        pub fn get_use(&self) -> u32 {
            self.uses & 0x1FFF_FFFF
        }
        pub fn increase_use(&mut self, context: &mut DatabaseContextData, wrote_non_opaques: bool) {
            if self.get_use() >= 0x1FFF_FFFF {
                return;
            }
            let mut raw_uses = self.uses;
            if wrote_non_opaques {
                debug!(
                    "mark wrote non-opaque (raw use={}, ptr = {:x?})",
                    self.uses, &self.uses as *const u32
                );
                raw_uses |= 0x4000_0000;
            }
            //TODO(future): We could have a special "increment 1" noatun primitive.

            // Safety: All materialized view types visible to safe code are always pinned.
            context.write_storable(raw_uses + 1, unsafe { Pin::new_unchecked(&mut self.uses) });
        }
        pub fn set_wrote_tombstone(&mut self, context: &mut DatabaseContextData) {
            let mut raw_uses = self.uses;
            if raw_uses & 0x2000_0000 != 0 {
                return;
            }
            raw_uses |= 0x2000_0000;
            // Safety: All materialized view types visible to safe code are always pinned.
            context.write_storable(raw_uses, unsafe { Pin::new_unchecked(&mut self.uses) });
        }
        pub fn decrease_use(
            &mut self,
            context: &mut DatabaseContextData,
            overwriter_tainted: bool,
            //wrote_non_opaques: bool,
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
            if overwriter_tainted {
                debug!(
                    "mark overwriter tainted (raw use={}, ptr = {:x?})",
                    self.uses, &self.uses as *const u32
                );
                raw_uses |= 0x8000_0000;
            }

            // TODO(future): We could have a special "decrement 1" noatun primitive.

            // Safety: All materialized view types visible to safe code are always pinned.
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
        /// overwrote this tracker's data.
        ///
        /// This is used in the advance_cutoff function, to know which not-yet-deleted
        /// messages that now have their last-overwriter overtaken by the cutoff-frontier,
        /// so that we can prune messages for the long-term pre-cutoff-life.
        pub last_overwriter: SequenceNr,
        /// The message that is no longer used (to be deleted, possibly).
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
            Some(self.cmp(other))
        }
    }
    impl Ord for UnusedInfo {
        #[inline]
        fn cmp(&self, other: &Self) -> Ordering {
            self.seq.cmp(&other.seq)
        }
    }

    /// Safety: UnusedInfo contains only NoatunStorable fields
    unsafe impl NoatunStorable for UnusedInfo {
        fn hash_schema(hasher: &mut SchemaHasher) {
            hasher.write_str("noatun::UnusedInfo/1")
        }
    }
}

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

/// Safety: MainDbStatus contains only NoatunStorable fields.
unsafe impl NoatunStorable for MainDbStatus {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::MainDbStatus/1")
    }
}

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
    materialized_view_schema_hash: [u8; 16],
    root_ptr: SerializableGenPtr,
}

impl MainDbHeader {
    pub fn is_clean(&self) -> bool {
        self.status.0 == MAIN_DB_STATUS_FULLY_CLEAN
            || (get_boot_checksum() == self.last_boot && self.status.0 == MAIN_DB_STATUS_HOT_CLEAN)
    }
}

/// Safety: MainDbHeader contains only NoatunStorable fields.
unsafe impl NoatunStorable for MainDbHeader {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::MainDbHeader/1")
    }
}

#[derive(Debug)]
#[repr(C)]
struct DepTrackEntry {
    outgoing_read_dep: NoatunVecRaw<NoatunUntrackedCell<SequenceNr>, DatabaseContextData>,
    incoming_read_dep: NoatunVecRaw<NoatunUntrackedCell<SequenceNr>, DatabaseContextData>,
    last_overwriter_of: NoatunVecRaw<NoatunUntrackedCell<SequenceNr>, DatabaseContextData>,
}

impl Object for DepTrackEntry {
    type Ptr = ThinPtr;
    type NativeType = ();
    type NativeOwnedType = ();

    fn export(&self) -> Self::NativeOwnedType {
        unimplemented!()
    }

    fn destroy(self: Pin<&mut Self>) {
        unimplemented!()
    }

    fn init_from(self: Pin<&mut Self>, _detached: &Self::NativeType) {
        unimplemented!()
    }

    unsafe fn allocate_from<'a>(_detached: &Self::NativeType) -> Pin<&'a mut Self> {
        unimplemented!()
    }
    fn hash_object_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::DepTrackEntry/1");
    }
}

/// Safety: DepTrackEntry contains only NoatunStorable fields.
unsafe impl NoatunStorable for DepTrackEntry {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::DepTrackEntry/1")
    }
}

#[derive(Debug)]
#[repr(C)]
pub(crate) struct MainDbAuxHeader {
    deptrack_keys: NoatunVecRaw<DepTrackEntry, DatabaseContextData>,
    uses: RawDatabaseVec<RegistrarInfo>,
    unused_messages: RawDatabaseVec<UnusedInfo>,
}

impl Default for MainDbAuxHeader {
    fn default() -> Self {
        Self {
            deptrack_keys: Default::default(),
            uses: Default::default(),
            unused_messages: Default::default(),
        }
    }
}

/// Safety: MainDbAuxHeader contains only NoatunStorable fields.
unsafe impl NoatunStorable for MainDbAuxHeader {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::MainDbAuxHeader/1")
    }
}

// Note, this type is in a private module and isn't nameable from other crates.
// It has to be public since it's named in methods in the Pointer trait, and the
// Pointer trait must be public because it's implemented by the object macro.
#[doc(hidden)]
pub struct DatabaseContextData {
    main_db_mmap: FileAccessor,
    //root_index: Option<GenPtr>,
    undo_log: UndoLog,

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

    fn record_overwrite(&mut self, overwritten: Tracker, overwriter: SequenceNr) {
        // Safety: No other live references exist
        let keys = unsafe { self.get_deptrack_keys_mut() };
        keys.ensure_size(overwriter.index() + 1, self);

        let value = keys.get_index_mut(overwriter.index(), self);

        value
            .last_overwriter_of
            .push(NoatunUntrackedCell(overwritten.owner), self);
    }

    /// Record a read dependency between A and B: A <- B.
    /// observee <- observer.
    /// I.e, observer has read from (data written by) observee.
    fn record_dependency(&mut self, observee: SequenceNr, observer: SequenceNr) {
        if observee == observer {
            return;
        }

        self.tainted = true;
        // Safety: No other live references exist
        let keys = unsafe { self.get_deptrack_keys_mut() };

        assert!(observee.is_valid());
        assert!(observer.is_valid());
        debug_assert!(observer > observee);

        keys.ensure_size(observee.max(observer).index() + 1, self);

        trace!(
            "Recording dependency observer: {:?} observing {:?}",
            observer,
            observee
        );
        // #Safety:
        // No code holds this reference while calling other code that does.
        // Generally, it is not long held. DatabaseContext is neither Sync nor Send.

        let left_key_place = keys.get_index_mut(observee.index(), self);

        if !left_key_place
            .outgoing_read_dep
            .iter(self)
            .any(|x| **x == observer)
        {
            left_key_place
                .outgoing_read_dep
                .push(NoatunUntrackedCell(observer), self);
        }

        let right_key_place = keys.get_index_mut(observer.index(), self);

        if !right_key_place
            .incoming_read_dep
            .iter(self)
            .any(|x| **x == observee)
        {
            right_key_place
                .incoming_read_dep
                .push(NoatunUntrackedCell(observee), self);
        }
    }

    /// # Safety:
    ///
    /// Outgoing read dependencies must not already be accessed in any way, not for any message.
    /// No accesses other than through the returned reference are allowed while the returned
    /// reference is live.
    pub(crate) unsafe fn incoming_read_dependencies_mut<'a>(
        &mut self,
        observee: SequenceNr,
    ) -> &'a mut NoatunVecRaw<NoatunUntrackedCell<SequenceNr>, DatabaseContextData> {
        // Safety: No other live references exist
        let keys = unsafe { self.get_deptrack_keys_mut() };

        &mut keys.get_index_mut(observee.index(), self).incoming_read_dep
    }

    pub(crate) fn get_live_values(&self, seq: SequenceNr) -> (u32, &'static str /*flags*/) {
        // Safety: No other live references exist
        let uses = unsafe { self.get_uses() };
        if uses.len() <= seq.index() {
            return (0, "");
        }
        // Safety: No other live references exist
        let info = unsafe { uses.get_mut(self, seq.index()) };

        let flags = match (
            info.overwriter_tainted(),
            info.wrote_tombstones(),
            info.wrote_non_opaques(),
        ) {
            (false, false, false) => "",
            (true, true, true) => "TSN",
            (true, false, false) => "T",
            (false, true, false) => "S",
            (false, false, true) => "N",
            (false, true, true) => "SN",
            (true, false, true) => "TN",
            (true, true, false) => "TS",
        };
        (info.get_use(), flags)
    }

    pub(crate) fn incoming_read_dependencies(
        &self,
        observee: SequenceNr,
    ) -> &NoatunVecRaw<NoatunUntrackedCell<SequenceNr>, DatabaseContextData> {
        let keys = self.get_deptrack_keys();
        if keys.len() <= observee.index() {
            return &NoatunVecRaw::EMPTY;
        }
        &keys.get_index(observee.index(), self).incoming_read_dep
    }

    pub(crate) fn overwriter_of(
        &self,
        observee: SequenceNr,
    ) -> &NoatunVecRaw<NoatunUntrackedCell<SequenceNr>, DatabaseContextData> {
        let keys = self.get_deptrack_keys();
        if keys.len() <= observee.index() {
            return &NoatunVecRaw::EMPTY;
        }
        &keys.get_index(observee.index(), self).last_overwriter_of
    }

    pub(crate) fn outgoing_read_dependencies(
        &self,
        observer: SequenceNr,
    ) -> &NoatunVecRaw<NoatunUntrackedCell<SequenceNr>, DatabaseContextData> {
        let keys: &NoatunVecRaw<DepTrackEntry, DatabaseContextData> =
            &self.get_aux_header().deptrack_keys;
        &keys.get_index(observer.index(), self).outgoing_read_dep
    }

    /// # Safety:
    ///
    /// Outgoing read dependencies must not already be accessed in any way, not for any message.
    /// No accesses other than through the returned reference are allowed while the returned
    /// reference is live.
    pub(crate) unsafe fn outgoing_read_dependencies_mut<'a>(
        &mut self,
        observer: SequenceNr,
    ) -> &'a mut NoatunVecRaw<NoatunUntrackedCell<SequenceNr>, DatabaseContextData> {
        // Safety: No other live references exist
        let keys = unsafe { self.get_deptrack_keys_mut() };
        &mut keys.get_index_mut(observer.index(), self).outgoing_read_dep
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
    ///
    /// By definition, all messages before the `next_index` have been applied to the
    /// in memory database.
    #[inline(always)]
    pub fn next_seqnr(&self) -> SequenceNr {
        // Safety: The map is large enough, and all bit patterns are valid
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
    fn raw_set_next_seqnr(&self, new_value: SequenceNr) {
        // Safety: The map is large enough, and all bit patterns are valid
        let header: &mut MainDbHeader =
            unsafe { &mut *(self.main_db_mmap.map_mut_ptr() as *mut MainDbHeader) };
        header.next_seqnr = new_value;
    }
    #[inline(always)]
    fn set_pointer_of(main_db_mmap: &FileAccessor, new_value: usize) {
        main_db_mmap.set_used_space(new_value);
    }
    #[inline(always)]
    fn pointer_of(main_db_mmap: &FileAccessor) -> usize {
        main_db_mmap.used_space()
    }
    #[inline(always)]
    fn raw_set_next_seqnr_of(main_db_mmap: &FileAccessor, new_value: SequenceNr) {
        // Safety: The map is large enough, and all bit patterns are valid
        let header: &mut MainDbHeader =
            unsafe { &mut *(main_db_mmap.map_mut_ptr() as *mut MainDbHeader) };

        header.next_seqnr = new_value;
    }

    /// Returns true if database was previously clean
    pub fn mark_dirty(&mut self) -> Result<bool> {
        // Safety: The map is large enough, and all bit patterns are valid
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
        // Safety: The map is large enough, and all bit patterns are valid
        let header: &mut MainDbHeader =
            unsafe { &mut *(self.main_db_mmap.map_mut_ptr() as *mut MainDbHeader) };

        header.status = MainDbStatus(MAIN_DB_STATUS_HOT_CLEAN);
    }

    #[inline]
    pub fn mark_fully_clean(&mut self) -> Result<()> {
        // Safety: The map is large enough, and all bit patterns are valid
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

    pub fn is_wrong_version(&self, correct_hash: [u8; 16]) -> bool {
        // Safety: The map is large enough, and all bit patterns are valid
        let header: &MainDbHeader =
            unsafe { &*(self.main_db_mmap.map_mut_ptr() as *const MainDbHeader) };
        header.materialized_view_schema_hash != correct_hash
    }

    #[inline]
    pub fn is_dirty(&self) -> bool {
        // Safety: The map is large enough, and all bit patterns are valid
        let header: &MainDbHeader =
            unsafe { &*(self.main_db_mmap.map_mut_ptr() as *const MainDbHeader) };

        !header.is_clean()
    }
    pub fn clear(&mut self, schema_hash: [u8; 16]) -> Result<()> {
        self.main_db_mmap.truncate(0)?;
        self.main_db_mmap
            .grow(size_of::<MainDbHeader>() + size_of::<MainDbAuxHeader>())?;
        Self::write_initial_header(&mut self.main_db_mmap, schema_hash);
        self.write_initial_aux_header();
        self.undo_log.clear()?;
        //self.unused_messages.clear();

        Ok(())
    }

    /*pub fn clear_unused_tracking(&mut self) {
        self.unused_messages.clear();
    }*/

    pub(crate) fn get_aux_header(&self) -> &MainDbAuxHeader {
        let slice = self
            .main_db_mmap
            .map_const_ptr()
            .wrapping_add(size_of::<MainDbHeader>());
        // Safety: The map is large enough, and all bit patterns are valid
        let slice = unsafe { std::slice::from_raw_parts(slice, size_of::<MainDbAuxHeader>()) };
        let aux_header: &MainDbAuxHeader = from_bytes(slice);
        aux_header
    }
    fn get_deptrack_keys<'a>(&self) -> &'a NoatunVecRaw<DepTrackEntry, DatabaseContextData> {
        // Safety: The calculated position is always home to a valid NoatunVecRaw
        unsafe {
            &mut *(self.main_db_mmap.map_mut_ptr().wrapping_add(
                size_of::<MainDbHeader>() + offset_of!(MainDbAuxHeader, deptrack_keys),
            ) as *mut NoatunVecRaw<DepTrackEntry, DatabaseContextData>)
        }
    }
    // # SAFETY
    // Must ensure no other access exists, or will exist during the lifetime of the return value
    unsafe fn get_deptrack_keys_mut<'a>(
        &mut self,
    ) -> &'a mut NoatunVecRaw<DepTrackEntry, DatabaseContextData> {
        // Safety: The calculated position is always home to a valid NoatunVecRaw
        unsafe {
            &mut *(self.main_db_mmap.map_mut_ptr().wrapping_add(
                size_of::<MainDbHeader>() + offset_of!(MainDbAuxHeader, deptrack_keys),
            ) as *mut NoatunVecRaw<DepTrackEntry, DatabaseContextData>)
        }
    }
    unsafe fn get_uses<'a>(&self) -> &'a mut RawDatabaseVec<RegistrarInfo> {
        // Safety: The map is large enough, and all bit patterns are valid
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

    fn write_initial_header(mmap: &mut FileAccessor, schema_hash: [u8; 16]) {
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
        header.materialized_view_schema_hash = schema_hash;
    }

    pub(crate) fn new<S: Disk>(
        s: &mut S,
        name: &Target,
        min_size: usize,
        max_size: usize,
        schema_hash: [u8; 16],
    ) -> Result<Self> {
        let (mut main_db_file, _existed) = s
            .open_file(
                name,
                "maindb",
                min_size,
                max_size,
                "main_db",
                "Main materialized view",
            )
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
            Self::write_initial_header(&mut main_db_file, schema_hash);
        }

        // Safety: The map is large enough, and all bit patterns are valid
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
            undo_log: UndoLog::new(s, name, min_size, max_size)?,
            //unused_messages: Vec::default(),
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

        let result = self.undo_log.rewind(|entry| match entry {
            UndoLogEntry::SetPointer(new_pointer) => {
                let cur = Self::pointer_of(&self.main_db_mmap);
                debug_assert!(new_pointer <= cur);
                // Safety: The map is large enough, and no live references exist
                unsafe {
                    Self::mut_byte_slice(self.main_db_mmap.map_mut_ptr(), new_pointer..cur).fill(0)
                };
                Self::set_pointer_of(&self.main_db_mmap, new_pointer);
                HowToProceed::PopAndContinue
            }
            UndoLogEntry::ZeroOut { start, len } => {
                // Safety: The map is large enough, and no live references exist
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
                // Safety: The map is large enough, and no live references exist
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
        // Safety: The map is large enough, and all bit patterns are valid
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
                    // Safety: The type check guarantees that the transmute will be an identity transmute
                    return unsafe { transmute_copy(&ptr) };
                }
                panic!(
                    "Wrong type of root pointer in database. Has schema changed significantly since last access?"
                );
            }
            GenPtr::Fat(ptr) => {
                if TypeId::of::<Ptr>() == TypeId::of::<FatPtr>() {
                    // Safety: The type check guarantees that the transmute will be an identity transmute
                    return unsafe { transmute_copy(&ptr) };
                }
                panic!(
                    "Wrong type of root pointer in database. Has schema changed significantly since last access?"
                );
            }
        }
    }
    pub fn zero_storable<T: NoatunStorable>(&mut self, storable: Pin<&mut T>) {
        // Safety: We don't move the data out
        unsafe { self.zero_internal(storable.get_unchecked_mut()) }
    }
    pub fn zero_internal<T: NoatunStorable>(&mut self, storable: &mut T) {
        let thin = self.index_of_ptr(storable as *mut _);
        let fat = FatPtr::from_thin_size(thin, size_of::<T>());
        self.zero_impl(fat, storable);
    }

    fn zero_impl<T: NoatunStorable>(&mut self, dst: FatPtr, dataref: &mut T) {
        self.undo_log.record(UndoLogEntry::RestorePod {
            start: dst.start,
            data: bytes_of_mut(dataref),
        });

        *dataref = T::zeroed();
    }

    /// # Safety
    /// The 'dst' pointer must be valid
    pub unsafe fn zero(&mut self, dst: FatPtr) {
        // Safety: 'dst' is a valid pointer
        unsafe {
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

    /// # Safety
    /// the pointers must be valid and unaliased.
    pub unsafe fn copy_bytes(&mut self, source: FatPtr, dest_index: ThinPtr) {
        // Safety: The pointers are valid and unaliased
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

    /// # Safety
    /// The pointers must be valid and not aliasing
    pub unsafe fn copy_bytes_len(
        &mut self,
        source: ThinPtr,
        dest_index: ThinPtr,
        num_bytes: usize,
    ) {
        // Safety: The pointers are valid and not aliasing
        unsafe { self.copy_bytes(FatPtr::from_idx_count(source.0, num_bytes), dest_index) }
    }
    pub fn copy_storable<T: NoatunStorable>(&mut self, source: &T, dest: &mut T) {
        let dest_index = self.index_of_sized(dest);
        self.undo_log.record(UndoLogEntry::RestorePod {
            start: dest_index.0,
            data: bytes_of_mut(dest),
        });
        dest.copy_from(source);
    }
    pub fn allocate_storable<T: NoatunStorable>(&mut self) -> Pin<&mut T> {
        let bytes = self.allocate_raw(std::mem::size_of::<T>(), std::mem::align_of::<T>());
        // Safety: bytes is a pointer that is valid until the end of the lifetime of the current
        // root object. The lifetime of Self has this same lifetime.
        unsafe { Pin::new_unchecked(&mut *(bytes as *mut T)) }
    }
    pub fn allocate_obj<T: Object>(&mut self) -> Pin<&mut T> {
        let bytes = self.allocate_raw(std::mem::size_of::<T>(), std::mem::align_of::<T>());
        // Safety: 'bytes' will not have been moved.
        unsafe { Pin::new_unchecked(&mut *(bytes as *mut T)) }
    }
    /// The pointer is only valid for the lifetime of the current root object
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
        // Safety: The 'start' ptr points to an object of 'size' bytes.
        // It is valid for lifetime of Self.
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
        // Safety: offset and size designate a valid slice
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
        // Safety: offset and size designate a valid slice
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
        // Safety: range designates a valid slice
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
        // Safety: range designates a valid slice
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
        // Safety: range designates a valid slice
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
        const {
            assert!(align_of::<T>() <= DISK_MAP_ALIGNMENT);
        }
        if !index.0.is_multiple_of(align_of::<T>()) {
            panic!("invalid pointer alignment");
        }
        // Safety: We've checked the value is in range and correctly aligned.
        // Caller promises no aliasing.
        unsafe {
            from_bytes(std::slice::from_raw_parts(
                self.main_db_mmap.map_const_ptr().wrapping_add(index.0),
                size_of::<T>(),
            ))
        }
    }

    /// # Safety
    /// Caller must ensure no mutable reference exists to the requested object
    /// Pointer must be valid and correctly aligned.
    #[inline]
    pub unsafe fn access_thin<'a, T: ?Sized>(&self, ptr: ThinPtr) -> &'a T {
        assert_eq!(size_of::<&T>(), size_of::<*const u8>());
        // Safety: 'ptr' is valid and correctly aligned
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
    /// Caller must ensure no mutable or shared reference exists to the requested object.
    /// ptr must be valid and correctly aligned.
    #[inline]
    pub unsafe fn access_thin_mut<'a, T: ?Sized>(&self, ptr: ThinPtr) -> &'a mut T {
        assert_eq!(size_of::<&mut T>(), size_of::<*const u8>());
        // Safety: ptr is valid
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
    /// ptr must be valid and correctly aligned.
    #[inline]
    pub unsafe fn access_fat<'a, T: ?Sized>(&self, ptr: FatPtr) -> &'a T {
        assert_eq!(size_of::<&T>(), 2 * size_of::<*const u8>());
        let raw = RawFatPtr {
            data: self.main_db_mmap.map_const_ptr().wrapping_add(ptr.start),
            size: ptr.count,
        };
        // Safety: ptr is valid
        let ret = unsafe { transmute_copy(&raw) };

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
        // Safety: ptr must be valid and correctly aligned
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
        if !index.0.is_multiple_of(align_of::<T>()) {
            panic!("invalid pointer alignment");
        }
        // Safety: index has been validated
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
        // Safety: We won't be moving out of dest
        let dest = unsafe { dest.get_unchecked_mut() };
        let dest_index = self.index_of_sized(dest);

        self.undo_log.record(UndoLogEntry::RestorePod {
            start: dest_index.0,
            data: bytes_of_mut(dest),
        });
        *dest = src;
    }

    /// #Safety:
    /// No references to dest must exist.
    /// 'dest' must be a valid pointer
    pub unsafe fn write_storable_ptr<T: NoatunStorable>(&mut self, src: T, dest: *mut T) {
        let dest_index = self.index_of_ptr(dest);
        assert!(
            dest_index.0 <= self.main_db_mmap.used_space()
                && dest_index.0 + size_of::<T>() <= self.main_db_mmap.used_space()
        );

        self.undo_log.record(UndoLogEntry::RestorePod {
            start: dest_index.0,
            // Safety: 'dest' is valid
            data: unsafe { slice::from_raw_parts_mut(dest as *mut u8, size_of::<T>()) },
        });
        // Safety: dest is a valid ptr
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

    /*
    /// Call after a complete update, i.e, applying multiple messages
    /// Returns all messages that can now be removed.
    pub(crate) fn calculate_stale_messages<MSG: Message + Debug>(
        &mut self,
        message_store: &OnDiskMessageStore<MSG>,
    ) -> Result<Vec<SequenceNr>> {
        Ok(self
            .first_stale_message_step(message_store)?
            )
    }*/

    pub fn update_registrar(&mut self, registrar_point: &mut Tracker, opaque: bool) {
        let current_registrar = self.next_seqnr();

        // Safety: We're creating the pointer from a mutable ref, so it is guaranteed
        // valid and unique.
        unsafe {
            self.update_registrar_ptr_impl(
                registrar_point,
                current_registrar,
                self.tainted,
                opaque,
                false,
            );
        }
    }

    /// # Safety
    /// 'registrar_point' must be a valid unaliased pointer
    pub unsafe fn update_registrar_ptr_impl(
        &mut self,
        registrar_point: *mut Tracker,
        actor: SequenceNr,
        actor_tainted: bool,
        actor_wrote_non_opaque: bool,
        is_clear: bool,
    ) {
        // Safety: The caller guarantees the pointer is valid
        let registrar_point_value = unsafe { registrar_point.read_unaligned() };

        if actor == registrar_point_value.owner {
            return; // Updating tracker to same value must not transiently free use and then re-add, it should be a no-op, like this!
        }

        let is_valid = registrar_point_value.owner.is_valid();
        if is_valid {
            self.rt_decrease_use(registrar_point_value, actor, actor_tainted);
        }
        if is_clear {
            // Clear tracker should only happen in "destroy", and destroyed memory
            // should never be reused. In fact, I think correctness hinges on it not being
            // reused, since we won't find the read-dependency if it is reused!
            // So we never need to actually write to memory in this case.
            return;
        }
        //TODO(future): We don't need to actually increase use for every write, we could collect
        //all writes and do a single write at the end of message apply!
        self.rt_increase_use(actor, actor_wrote_non_opaque);

        // Safety: The caller guarantees the pointer is valid
        unsafe { self.write_storable_ptr(Tracker { owner: actor }, registrar_point) }
    }

    /// # Safety
    /// registrar_point must be a valid pointer
    pub unsafe fn update_tracker_ptr(&mut self, registrar_point: *mut Tracker, opaque: bool) {
        self.update_registrar_ptr_impl(
            registrar_point,
            self.next_seqnr(),
            self.tainted,
            !opaque,
            false,
        );
    }
    /// # Safety
    /// registrar_point must be a valid pointer
    pub unsafe fn clear_registrar_ptr(&mut self, registrar_point: *mut Tracker, opaque: bool) {
        self.wrote_tombstone = true;
        self.update_registrar_ptr_impl(
            registrar_point,
            self.next_seqnr(),
            self.tainted,
            !opaque,
            true,
        );
    }

    // Signify that the current message has observed data tracker by 'Tracker'
    // (and thus read data written by that tracker's owner, if any).
    pub fn observe_registrar(&mut self, observee: Tracker) {
        if self.next_seqnr().is_invalid() {
            return;
        }
        if observee.owner.is_invalid() {
            return;
        }
        let observer = self.next_seqnr();
        if observer != observee.owner {
            self.record_dependency(observee.owner, observer);
        }
    }
    /*
    pub(crate) fn unused_push(&mut self, unused_info: UnusedInfo) {
        unsafe {
            let unused_list = self.get_unused_list();
            unused_list.push_untracked(self, unused_info);
        }
    }*/

    pub(crate) fn rt_finalize_message<M: Message>(
        &mut self,
        message_seqnr: SequenceNr,
        must_remove: &mut Vec<SequenceNr>,
        messages: &OnDiskMessageStore<M>,
    ) -> Result<()> {
        debug_assert!(message_seqnr.is_valid());
        //let aux_header = self.get_aux_header();

        // SAFETY:
        // We only hold this for this method, and we call no other code that
        // uses the same memory. So do all other users of 'get_uses'.
        let uses = unsafe { self.get_uses() };

        while self.wrote_tombstone && uses.len() <= message_seqnr.index() {
            uses.push_untracked(self, RegistrarInfo::default());
        }

        if uses.len() <= message_seqnr.index() {
            // This is a bit of a special case. This is a message
            // that did not actually leave any modified state at all after its projection.
            // Note, the message can still have cleared data.
            trace!(
                "Message modified nothing: {:?} (tainted: {})",
                message_seqnr,
                self.tainted
            );

            self.record_overwrite(
                Tracker {
                    owner: message_seqnr,
                },
                message_seqnr,
            );
            self.try_delete(message_seqnr, message_seqnr, must_remove, messages)?;

            return Ok(());
        }

        // Safety: There are not other active views
        let mut track = unsafe { uses.get_mut(self, message_seqnr.index()) };

        if self.wrote_tombstone {
            // TODO(future): We probably only need to do this write if get_use() != 0 below.
            // In other cases, I believe nothing ever reads this `uses` slot.

            // Safety: We don't move out of the reference
            unsafe {
                track.as_mut().get_unchecked_mut().set_wrote_tombstone(self);
            }
        }

        {
            // Safety: There are no other views of this data
            let keys = unsafe { self.get_deptrack_keys_mut() };
            dprintln!("Finalize messages, checking for overwriting");
            if let Some(dep_info) = keys.try_get_index(message_seqnr.index(), self) {
                let l = dep_info.last_overwriter_of.len();
                dprintln!("OVerwrite count: {}", l);
                for i in 0..l {
                    let item = **dep_info.last_overwriter_of.get_index(i, self);
                    dprintln!("Overwrite: {}", item);
                    self.try_delete(item, message_seqnr, must_remove, messages)?;
                }
            }
        }

        if track.get_use() == 0 {
            self.record_overwrite(
                Tracker {
                    owner: message_seqnr,
                },
                message_seqnr,
            );
            self.try_delete(message_seqnr, message_seqnr, must_remove, messages)?;
        }
        Ok(())
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

    /// Precondition:
    /// `seq` must have been completely overwritten.
    ///
    /// cutoff is the first SequenceNr that is not before the cutoff time.
    /// cutoff is thus on the right side of the cutoff split.
    pub(crate) fn try_delete<M: Message>(
        &mut self,
        seq: SequenceNr,
        last_overwriter: SequenceNr,
        must_remove: &mut Vec<SequenceNr>,
        messages: &OnDiskMessageStore<M>,
    ) -> Result<()> {
        let mut delet_tasks = Vec::new();
        delet_tasks.push((seq, last_overwriter));

        while let Some(task) = delet_tasks.pop() {
            if self.outgoing_read_dependencies(seq).len() != 0 {
                continue;
            }

            // Note: 'last_overwriter' will be 'seq' if message had no overwriters.
            // This is just a convention, but it turns out to work well.
            let (seq, last_overwriter) = task;

            // Safety: There are not other active views
            let uses = unsafe { self.get_uses() };
            let mark_delete = if seq.index() >= uses.len() {
                dprintln!(
                    "@{} {:?} mark_delete because index >= uses.len()",
                    crate::cur_node(),
                    crate::test_elapsed()
                );
                // If current message made no imprint at all, it can just as well be deleted.
                // The message has neither read nor written any data.
                true
            } else {
                // cur = the candidate we might wish to delete
                let cur = uses.get(self, seq.index());

                let cutoff = messages.cutoff_index();
                #[allow(clippy::if_same_then_else)]
                if last_overwriter < cutoff {
                    dprintln!(
                        "@{} {:?} {} mark_delete because last_overwriter<cutoff",
                        crate::cur_node(),
                        crate::test_elapsed(),
                        seq
                    );
                    true
                } else if !cur.wrote_non_opaques() && (!cur.wrote_tombstones() || seq < cutoff) {
                    dprintln!("@{} {:?} {} mark_delete because !tombstones and seq(!tombstones({}) or {}) < cutoff({})", crate::cur_node(), crate::test_elapsed(), seq, cur.wrote_tombstones(), seq, cutoff);
                    true
                } else if !messages.may_have_been_transmitted(seq)? && !cur.wrote_tombstones() {
                    dprintln!(
                        "@{} {:?} {} mark_delete because !transmitted && !tombstone",
                        crate::cur_node(),
                        crate::test_elapsed(),
                        seq
                    );
                    true
                } else {
                    dprintln!("@{} {:?} {} can't be deleted yet: last_ovr: {}, cutoff: {}, non-opaq: {}, tomb: {}, transmitted: {}", crate::cur_node(), crate::test_elapsed(), seq,
                        last_overwriter,cutoff,
                        cur.wrote_non_opaques(),cur.wrote_tombstones(),
                        messages.may_have_been_transmitted(seq)?
                    );
                    false
                }
            };

            if mark_delete {
                must_remove.push(seq);

                // #Safety:
                // Dependencies are only used here and in a few other places. The references
                // aren't kept alive over longer periods. Specifically, no references are
                // alive at this point in the code.
                let outgoing_deps = unsafe { self.outgoing_read_dependencies_mut(seq) };

                for i in 0..outgoing_deps.len() {
                    let right = **outgoing_deps.get_index(i, self);
                    assert!(right > seq);
                    // Safety: No other active references to the dependencies exist
                    // (see assert above)
                    unsafe {
                        self.incoming_read_dependencies_mut(right).retain(
                            |x| **x != seq,
                            self,
                            |_| { /*no-op destroy*/ },
                        );
                    }
                }

                // Safety: No other active references to the dependencies exist
                let incoming_deps = unsafe { self.incoming_read_dependencies_mut(seq) };
                for i in 0..incoming_deps.len() {
                    let left = **incoming_deps.get_index(i, self);

                    assert!(left < seq);
                    // Safety: No other active references to the same dependencies exist
                    // (see assert above)
                    unsafe {
                        self.outgoing_read_dependencies_mut(left).retain(
                            |x| **x != seq,
                            self,
                            |_| { /*no-op destroy*/ },
                        )
                    };

                    delet_tasks.push((left, seq));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn try_delete_all_that_were_overwritten_by_range<M: Message + Debug>(
        &mut self,
        range: Range<usize>,
        messages: &OnDiskMessageStore<M>,
        must_remove: &mut Vec<SequenceNr>,
        // The new cutoff value, while advancing cutoff
    ) -> Result<()> {
        let mut temp = Vec::new();
        for seq_index in range {
            temp.clear();
            {
                // Safety: No other views of this data existg
                let keys = unsafe { self.get_deptrack_keys_mut() };
                if let Some(overwritten) = keys.try_get_index_mut(seq_index, self) {
                    let l = overwritten.last_overwriter_of.len();
                    for i in 0..l {
                        temp.push(
                            **overwritten
                                .last_overwriter_of
                                .try_get_index(i, self)
                                .unwrap(),
                        );
                    }
                    overwritten.last_overwriter_of.clear_fast(self);
                }
            }
            for item in temp.drain(..) {
                self.try_delete(
                    item,
                    SequenceNr::from_index(seq_index),
                    must_remove,
                    messages,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn rt_increase_use(&mut self, seq: SequenceNr, wrote_non_opaque: bool) {
        // Safety: No other active view exist
        let uses = unsafe { self.get_uses() };
        if uses.len() <= seq.index() {
            uses.grow(self, seq.index() + 1);
        }
        // Safety: No other view exists
        let mut info = unsafe { uses.get_mut(self, seq.index()) };
        info.increase_use(self, wrote_non_opaque);
        trace!(
            "increased use of {:?} to {} (tainted:{}, cur: {})",
            seq,
            info.get_use(),
            info.overwriter_tainted(),
            self.next_seqnr()
        );
    }

    pub(crate) fn rt_decrease_use(
        &mut self,
        tracker: Tracker,
        overwriter: SequenceNr,
        overwriter_tainted: bool,
        //wrote_non_opaque: bool,
    ) {
        // Safety: No other view exists
        let uses = unsafe { self.get_uses() };
        // Safety: No other view exists
        let mut cur = unsafe { uses.get_mut(self, tracker.owner.index()) };
        let cur_use = cur.get_use();
        if cur_use == 0 {
            println!("Corrupt use count for sequence nr {tracker:?}, use = {cur_use}");
            std::process::abort();
        }

        // Safety: We don't move out of the ref
        unsafe {
            cur.as_mut()
                .get_unchecked_mut()
                .decrease_use(self, overwriter_tainted)
        };
        trace!(
            "decreased use of {:?} is {} (taint:{}) (because overwriter: {:?}(tainted:{}))",
            tracker,
            cur.get_use(),
            cur.overwriter_tainted(),
            overwriter,
            overwriter_tainted
        );
        if cur.get_use() == 0 {
            // This is the normal way messages end up in 'unused_messages'
            trace!(
                "Adding {:?} as unused (overwriter.tainted: {}, tracker owner tainted: {}, cur tombstone: {})",
                tracker,
                overwriter_tainted,
                cur.overwriter_tainted(),
                cur.wrote_tombstones()
            );
            self.record_overwrite(tracker, overwriter);
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
    fn basic_test_deptrack1() {
        let mut disk = InMemoryDisk::default();
        let mut tracker = DatabaseContextData::new(
            &mut disk,
            &Target::CreateNew("ctx".into()),
            0,
            20000,
            [0; 16],
        )
        .unwrap();

        tracker.record_dependency(SequenceNr::from_index(1), SequenceNr::from_index(2));

        let result: Vec<_> = tracker
            .incoming_read_dependencies(SequenceNr::from_index(2))
            .iter(&tracker)
            .map(|x| x.0)
            .collect();
        assert_eq!(result, vec![SequenceNr::from_index(1)]);

        let result: Vec<_> = tracker
            .outgoing_read_dependencies(SequenceNr::from_index(1))
            .iter(&tracker)
            .map(|x| x.0)
            .collect();
        assert_eq!(result, vec![SequenceNr::from_index(2)]);
    }

    #[test]
    fn basic_test_deptrack_many() {
        let mut disk = InMemoryDisk::default();
        let mut tracker = DatabaseContextData::new(
            &mut disk,
            &Target::CreateNew("ctx".into()),
            0,
            30000,
            [0; 16],
        )
        .unwrap();

        let t = Instant::now();
        for i in 0..100_usize {
            tracker.record_dependency(
                SequenceNr::from_index((i as f64).sqrt() as usize),
                SequenceNr::from_index(i),
            );
        }
        println!("Time: {:?}", t.elapsed());

        let result: Vec<_> = tracker
            .outgoing_read_dependencies(SequenceNr::from_index(8))
            .iter(&tracker)
            .map(|x| x.index())
            .collect();
        assert_eq!(
            result,
            vec![64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80]
        );

        let result: Vec<_> = tracker
            .incoming_read_dependencies(SequenceNr::from_index(64))
            .iter(&tracker)
            .map(|x| x.index())
            .collect();
        assert_eq!(result, vec![8]);
    }
}
