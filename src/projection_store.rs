use crate::boot_checksum::get_boot_checksum;
use crate::data_types::{DatabaseCell, DatabaseVec, RawDatabaseVec};
use crate::disk_abstraction::{Disk, StandardDisk};
use crate::disk_access::FileAccessor;
use crate::message_store::OnDiskMessageStore;
use crate::platform_specific::get_boot_time;
use crate::undo_store::{HowToProceed, UndoLog, UndoLogEntry};
use crate::{
    Application, FatPtr, FixedSizeObject, GenPtr, MessageId, MessagePayload, Object, Pointer,
    Target, ThinPtr,
};
use anyhow::{bail, Context, Result};
use bumpalo::Bump;
use bytemuck::{bytes_of, from_bytes, from_bytes_mut, AnyBitPattern, Pod, Zeroable};
use indexmap::{IndexMap, IndexSet};
use std::alloc::Layout;
use std::any::{Any, TypeId};
use std::cell::{RefCell};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::{offset_of, transmute_copy};
use std::ops::Range;
use std::path::Path;
use std::slice::SliceIndex;
use std::{iter, slice};

use crate::projection_store::registrar_info::{RegistrarInfo, UnusedInfo};
use crate::sequence_nr::SequenceNr;
use std::pin::Pin;
mod registrar_info {
    use crate::disk_abstraction::Disk;
    use bumpalo::Bump;
    use bytemuck::{Pod, Zeroable};
    use indexmap::IndexMap;
    use std::fmt::Debug;
    use std::pin::Pin;
    use crate::message_store::OnDiskMessageStore;
    use crate::sequence_nr::SequenceNr;
    use crate::{DatabaseContextData, MessagePayload, NoatunContext, Target, ThinPtr};

    #[derive(Debug, Clone, Copy, Default, Pod, Zeroable)]
    #[repr(C)]
    pub(crate) struct RegistrarInfo {
        uses: u32,
    }
    impl RegistrarInfo {
        pub fn get_opaque(&self) -> bool {
            self.uses & 0x8000_0000 == 0
        }
        pub fn set_non_opaque(&mut self) {
            self.uses |= 0x8000_0000;
        }
        pub fn get_use(&self) -> u32 {
            self.uses & 0x7FFF_FFFF
        }
        pub fn increase_use(&mut self, context: &mut DatabaseContextData) {
            if self.get_use() >= 0x7FFF_FFFF {
                return;
            }
            //TODO: We could have a special "inc 1" noatun primitive.
            context.write_pod(self.uses + 1, unsafe { Pin::new_unchecked(&mut self.uses) } );
        }
        pub fn decrease_use(&mut self, context: &mut DatabaseContextData) {
            let cur_uses = self.get_use();
            if cur_uses == 0 {
                panic!("Internal error, use count wrong");
            }
            if cur_uses >= 0x7FFF_FFFF {
                return;
            }
            //TODO: We could have a special "dec 1" noatun primitive.
            context.write_pod(self.uses - 1, unsafe{Pin::new_unchecked(&mut self.uses)});
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
}

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

#[derive(Debug, Clone, Copy, Default, Zeroable, Pod)]
#[repr(C)]
pub struct MainDbAuxHeader {
    deptrack_keys: RawDatabaseVec<ThinPtr>,
    uses: RawDatabaseVec<RegistrarInfo>,
}

pub struct DatabaseContextData {
    main_db_mmap: FileAccessor,
    root_index: Option<GenPtr>,
    undo_log: UndoLog,

    // The current message being written (or None if not open for writing)

    unused_messages: Vec<UnusedInfo>,
    // Set to true when run from within message apply
    pub(crate) is_mutable: bool,
    // The next message expected to be applied.
    // Starts at 0. When a message is being applied, this field
    // will have the seqnr of the message being applied, not the next one.
    //next_seqnr: SequenceNr,
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

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub(crate) struct DepTrackLinkedListEntry {
    // TODO: Possibly make this struct 4-byte aligned, and remove the padding
    pub next: ThinPtr,
    pub seq: SequenceNr,
    pub padding: u32,
}

impl DatabaseContextData {
    fn record_dependency(&mut self, observee: SequenceNr, observer: SequenceNr) {
        assert!(observee.is_valid());
        assert!(observer.is_valid());

        // #Safety:
        // No code holds this reference while calling other code that does.
        // Generally, it is not long held. DatabaseContext is neither Sync nor Send.
        let keys = unsafe { self.get_deptrack_keys() };

        if observee.index() >= keys.len() {
            keys.grow(self, observee.index() + 1);
        }
        let key_place = keys.get_mut(self, observee.index());

        let mut new_entry: &mut DepTrackLinkedListEntry = self.allocate_pod_internal();

        self.write_pod(*key_place, unsafe{Pin::new_unchecked(&mut new_entry.next)});
        self.write_pod(observer,   unsafe{Pin::new_unchecked(&mut new_entry.seq)});

        let new_entry_index = self.index_of_sized(new_entry);
        self.write_pod(new_entry_index, key_place);
    }
    fn read_dependency(&self, observee: SequenceNr) -> impl Iterator<Item = SequenceNr> + '_ {
        let keys: &RawDatabaseVec<ThinPtr> = &self.get_aux_header().deptrack_keys;

        let mut cur: ThinPtr = if observee.index() < keys.len() {
            *keys.get_mut(self, observee.index())
        } else {
            ThinPtr(0)
        };

        iter::from_fn(move || {
            if cur.0 == 0 {
                return None;
            }
            let entry: &DepTrackLinkedListEntry = unsafe { self.access_pod(cur) };
            cur = entry.next;
            Some(entry.seq)
        })
    }

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
        self.main_db_mmap
            .grow(size_of::<MainDbHeader>() + size_of::<MainDbAuxHeader>())?;
        Self::write_initial_header(&mut self.main_db_mmap);
        self.write_initial_aux_header();
        self.undo_log.clear();
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
        let aux_header: &MainDbAuxHeader = bytemuck::from_bytes(slice);
        aux_header
    }
    unsafe fn get_deptrack_keys<'a>(&self) -> &'a mut RawDatabaseVec<ThinPtr> {
        unsafe {
            &mut *(self.main_db_mmap.map_mut_ptr().wrapping_add(
                size_of::<MainDbHeader>() + offset_of!(MainDbAuxHeader, deptrack_keys),
            ) as *mut RawDatabaseVec<ThinPtr>)
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
        let aux_header: &mut MainDbAuxHeader = bytemuck::from_bytes_mut(
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
            bytemuck::from_bytes_mut(&mut mmap.map_mut()[0..size_of::<MainDbHeader>()]);
        header.next_seqnr = SequenceNr::INVALID;

        header.status = MainDbStatus(MAIN_DB_STATUS_DIRTY);
        header.usize_size = size_of::<usize>()
            .try_into()
            .expect("The size of an 'usize' must be less than 256 bytes");

        header.last_boot = get_boot_checksum();
    }

    pub(crate) fn new<S: Disk>(s: &mut S, name: &Target, max_size: usize) -> Result<Self> {
        let mut main_db_file = s
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
            root_index: None,
            undo_log: UndoLog::new(s, name, max_size)?,
            unused_messages: Vec::default(),
            is_mutable: false,
        };
        // TODO: It's a bit of a code smell that at this precise point in th execution,
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
        //println!("Rewinding from {} to {:?}", self.next_seqnr(), new_time);

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
                    HowToProceed::DontPopAndStop
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

    pub fn copy(&mut self, source: FatPtr, dest_index: ThinPtr) {
        unsafe {
            //dbg!(&source, &dest_index);

            self.undo_log.record(UndoLogEntry::Restore {
                start: dest_index.0,
                data: self.access_slice(FatPtr::from(dest_index.0, source.len)),
            });

            let dest = self.access_slice_mut::<u8>(FatPtr {
                start: dest_index.0,
                len: source.len,
            });

            let src = self.access_slice(source);

            dest.copy_from_slice(src);
        }
    }
    pub fn copy_sized(&mut self, source: ThinPtr, dest_index: ThinPtr, size_bytes: usize) {
        self.copy(FatPtr::from(source.0, size_bytes), dest_index)
    }
    pub fn copy_pod<T: Pod>(&mut self, source: &T, dest: &mut T) {
        unsafe {
            let dest_index = self.index_of_sized(dest);
            self.undo_log.record(UndoLogEntry::Restore {
                start: dest_index.0,
                data: bytes_of(dest),
            });
            *dest = *source;
        }
    }
    #[allow(clippy::mut_from_ref)]
    pub fn allocate_pod<T: AnyBitPattern>(&mut self) -> Pin<&mut T> {
        let bytes = self.allocate_raw(std::mem::size_of::<T>(), std::mem::align_of::<T>());
        unsafe { Pin::new_unchecked(&mut *(bytes as *mut T)) }
    }
    #[allow(clippy::mut_from_ref)]
    pub(crate) fn allocate_pod_internal<'a, T: Pod>(&mut self) -> &'a mut T {
        let bytes = self.allocate_raw(std::mem::size_of::<T>(), std::mem::align_of::<T>());
        unsafe { &mut *(bytes as *mut T) }
    }
    pub fn allocate_raw(&mut self, size: usize, align: usize) -> *mut u8 {
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
    pub unsafe fn access_slice_at<'a, T: AnyBitPattern>(&self, offset: usize, size: usize) -> &'a [T] {
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
    pub unsafe fn access_slice_at_mut<'a, T: AnyBitPattern>(
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
    /// The returned range must not overlap any mutable reference
    /// Alignment must be right.
    pub unsafe fn access_object_slice<'a, T: FixedSizeObject>(&self, range: FatPtr) -> &'a [T] {
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
    pub unsafe fn access_object_slice_mut<'a, T: FixedSizeObject>(&self, range: FatPtr) -> &'a mut [T] {
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

    /// # Safety
    /// Caller must ensure no mutable reference exists to the requested object
    pub unsafe fn access_pod<'a, T: AnyBitPattern>(&self, index: ThinPtr) -> &'a T {
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
    pub unsafe fn access_object<'a, T: FixedSizeObject>(&self, index: ThinPtr) -> &'a T {
        unsafe { self.access_pod(index) }
    }
    /// # Safety
    /// Caller must ensure no references exists to the requested object
    pub unsafe fn access_pod_mut<'a, T: Pod>(&self, index: ThinPtr) -> Pin<&'a mut T> {
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
    /// # Safety
    /// Caller must ensure no references exists to the requested object
    pub unsafe fn access_object_mut<'a, T: FixedSizeObject>(&self, index: ThinPtr) -> Pin<&'a mut T> {
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
            Pin::new_unchecked(crate::from_bytes_mut(std::slice::from_raw_parts_mut(
                ptr,
                size_of::<T>(),
            )))
        }
    }
    pub fn write(&mut self, index: usize, data: &[u8]) {
        assert!(index + data.len() <= self.main_db_mmap.used_space());
        let fat = FatPtr {
            start: index,
            len: data.len(),
        };
        let target = unsafe { self.access_slice_mut(fat) };
        target.copy_from_slice(data);
    }
    pub fn write_pod<T: Pod>(&mut self, src: T, dest: Pin<&mut T>) {
        let dest = unsafe { dest.get_unchecked_mut() };
        let dest_index = self.index_of_sized(dest);

        self.undo_log.record(UndoLogEntry::Restore {
            start: dest_index.0,
            data: bytes_of(dest),
        });
        *dest = src;
    }
    pub fn write_object<T: FixedSizeObject>(&mut self, src: T, dest: Pin<&mut T>) {
        let dest = unsafe { dest.get_unchecked_mut() };
        let dest_index = self.index_of_sized(dest);

        self.undo_log.record(UndoLogEntry::Restore {
            start: dest_index.0,
            data: crate::bytes_of(dest),
        });
        *dest = src;
    }
    #[allow(clippy::not_unsafe_ptr_arg_deref)] //False positive, we check the bounds
    pub fn write_pod_ptr<T: Pod>(&mut self, src: T, dest: *mut T) {
        let dest_index = self.index_of_ptr(dest);
        assert!(dest_index.0 + size_of::<T>() <= self.main_db_mmap.used_space());

        self.undo_log.record(UndoLogEntry::Restore {
            start: dest_index.0,
            data: unsafe { slice::from_raw_parts(dest as *const u8, size_of::<T>()) },
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
        ThinPtr((t as *const u8 as usize) - (self.main_db_mmap.map_const_ptr() as usize))
    }

    /// Call after writing a message.
    pub fn finalize_message(&mut self, seqnr: SequenceNr) {
        self.rt_finalize_message(seqnr);
    }
    /// Call after a complete update, i.e, applying multiple messages
    /// Returns all messages that can now be removed.
    pub(crate) fn calculate_stale_messages<MSG: MessagePayload + Debug>(
        &mut self,
        message_store: &mut OnDiskMessageStore<MSG>,
        is_before_cutoff: bool,
    ) -> Result<Vec<SequenceNr>> {
        Ok(self
            .rt_calculate_stale_messages(message_store, is_before_cutoff)?
            .into_iter()
            .collect())
    }

    pub fn update_registrar(&mut self, registrar_point: &mut SequenceNr, opaque: bool) {
        if registrar_point.is_valid() {
            self.rt_decrease_use(unsafe { *registrar_point });
        }
        let current_registrar = self.next_seqnr();
        if current_registrar.is_invalid() {
            // We're in the 'initialize root' method
            return;
        }
        if !opaque {
            self.rt_set_non_opaque(current_registrar);
        }
        self.rt_increase_use(current_registrar);

        self.write_pod(current_registrar, Pin::new(registrar_point))
    }
    pub fn update_registrar_ptr(&mut self, registrar_point: *mut SequenceNr, opaque: bool) {
        let registrar_point_value = unsafe { registrar_point.read_unaligned() };
        if registrar_point_value.is_valid() {
            self.rt_decrease_use(registrar_point_value);
        }
        let current_registrar = self.next_seqnr();
        if current_registrar.is_invalid() {
            // We're in the 'initialize root' method
            return;
        }
        if !opaque {
            self.rt_set_non_opaque(current_registrar);
        }
        self.rt_increase_use(current_registrar);

        self.write_pod_ptr(current_registrar, registrar_point)
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

    pub(crate) fn rt_finalize_message(&mut self, message_id: SequenceNr) {
        debug_assert!(message_id.is_valid());
        let aux_header = self.get_aux_header();

        // #SAFETY
        // We only hold this for this method, and we call no other code that
        // uses the same memory. So does all other users of 'get_uses'.
        let uses = unsafe { self.get_uses() };


        if uses.len() <= message_id.index() {
            // This is a bit of a special case. This is a message
            // that did not actually modify any state at all during its projection.
            self.unused_messages.push(UnusedInfo {
                seq: message_id,
                opaque: true,
            });
            return;
        }
        let track = uses.get(self, message_id.index());

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

    pub(crate) fn rt_calculate_stale_messages<M: MessagePayload + Debug>(
        &mut self,
        messages: &mut OnDiskMessageStore<M>,
        is_before_cutoff: bool,
    ) -> anyhow::Result<Vec<SequenceNr>> {
        self.unused_messages.sort(); //Sort in seq-nr order
        let mut deleted = Vec::new();
        let mut parent_lists = Bump::new();

        //println!("Calculating staleness, cutoff: {:?}", is_before_cutoff);
        'outer: for msg in self.unused_messages.iter().rev() {
            if msg.opaque {
                // This can be deleted
            } else if !messages.may_have_been_transmitted(msg.seq)? || is_before_cutoff {
                for observer in self.read_dependency(msg.seq) {
                    if !deleted.contains(&observer) {
                        // 'msg' can't be deleted, because it's observed by
                        // 'observer' - i.e a later message that has not been deleted.
                        /*println!(
                            "Can't delete {:?} because it's observed by {:?}",
                            msg, observer
                        );*/
                        continue 'outer;
                    }
                }
            } else {
                /*println!(
                    "Can't delete {:?}, because we can't know if someone will use its output",
                    msg
                );*/
                continue 'outer;
            }

            println!("Deleting {:?}", msg);

            deleted.push(msg.seq);
        }
        self.unused_messages.clear();

        // 'deleted' ends up in reverse seqnr-order
        // iterate in seq-nr order.
        /*let mut parent_remap: IndexMap<SequenceNr, Vec<SequenceNr>> = IndexMap::new();
        for deleted in deleted.iter().rev() {
            let mut parent_list = vec![];

            let Some(msg) = messages.read_message_by_index(deleted.index())? else {
                panic!("Attempt to delete already-deleted message.");
            };
            for parent in msg.header.parents.iter() {
                let parent_index = SequenceNr::from_index(messages.get_index_of(*parent)?
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
        }*/

        Ok(deleted)
    }
    pub(crate) fn rt_increase_use(&mut self, registrar: SequenceNr) {
        let uses = unsafe { self.get_uses() };
        if uses.len() <= registrar.index() {
            uses.grow(self, registrar.index() + 1);
        }
        uses.get_mut(self, registrar.index()).increase_use(self);
    }
    pub(crate) fn rt_set_non_opaque(&mut self, registrar: SequenceNr) {
        let uses = unsafe { self.get_uses() };
        if uses.len() <= registrar.index() {
            uses.grow(self, registrar.index() + 1);
        }
        uses.get_mut(self, registrar.index()).set_non_opaque();
    }
    pub(crate) fn rt_decrease_use(&mut self, registrar: SequenceNr) {
        let uses = unsafe { self.get_uses() };
        let mut cur = uses.get_mut(self, registrar.index());
        if cur.get_use() == 0 {
            panic!("Corrupt use count for sequence nr {:?}", registrar);
        }
        unsafe { cur.as_mut().get_unchecked_mut().decrease_use(self) };
        if cur.get_use() == 0 {
            // This is the normal way messages end up in 'unused_messags'
            self.unused_messages.push(UnusedInfo {
                seq: registrar,
                opaque: cur.get_opaque(),
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
