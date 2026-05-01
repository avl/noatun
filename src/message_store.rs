use crate::cutoff::{Acceptability, CutOffConfig, CutOffHashPos, CutOffTime, CutoffHash};
use crate::disk_abstraction::{Disk, DISK_MAP_ALIGNMENT};
use crate::disk_access::FileAccessor;
use crate::sequence_nr::SequenceNr;
use crate::sha2_helper::{sha2, sha2_message};
use crate::update_head_tracker::UpdateHeadTracker;
use crate::{
    bytes_of, bytes_of_mut, dyn_cast_slice, dyn_cast_slice_mut, from_bytes, from_bytes_mut,
    Message, MessageExt, MessageFrame, MessageHeader, MessageId, NoatunStorable, NoatunTime,
    SchemaHasher, Target,
};
use anyhow::{anyhow, bail, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
#[allow(unused)]
use itertools::Itertools;
use metrics::{counter, describe_counter, describe_gauge, gauge, Counter, Gauge, Unit};
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem::offset_of;
use std::ops::Range;
use tracing::{debug, info, trace, warn};

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct FileOffset(u64);

impl Debug for FileOffset {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(offset) = self.offset() {
            write!(f, "{offset}")
        } else {
            write!(f, "deleted")
        }
    }
}

/// Safety: FileOffset is just a transparent wrapper around u64, and is NoatunStorable
unsafe impl NoatunStorable for FileOffset {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::FileOffset/1")
    }
}

/// Size of header for each individual message in store
const MSG_HEADER_SIZE: usize = size_of::<FileHeaderEntry>();

const HASH_SIZE: usize = 16;

struct EmbVecAccessor<T> {
    handle: T,
    /// Location of u16-length value
    len_offset: usize,
    /// Location of u16-capacity value
    capacity_offset: usize,
    /// Start of actual payload data
    start_offset: usize,
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct EmbVecHeader {
    num: u16,
    num_allocated: u16,
}

impl EmbVecHeader {
    fn free(&self) -> usize {
        self.num_allocated.saturating_sub(self.num) as usize
    }
}

impl<T: Read + Seek> EmbVecAccessor<T> {
    pub fn new_parents(handle: T, offset: u64) -> Self {
        Self::new_internal(
            handle,
            offset as usize + size_of::<FileHeaderEntry>(),
            offset as usize + offset_of!(FileHeaderEntry, num_parents),
        )
    }
    pub fn new_children(handle: T, offset: u64) -> Result<Self> {
        let mut np = Self::new_parents(handle, offset);
        let parent_capacity = np.capacity()?;

        let EmbVecAccessor { handle, .. } = np;

        Ok(Self::new_internal(
            handle,
            offset as usize
                + size_of::<FileHeaderEntry>()
                + size_of::<MessageId>() * parent_capacity,
            offset as usize + offset_of!(FileHeaderEntry, num_children),
        ))
    }

    fn new_internal(
        t: T,
        start_offset: usize,         //Offset of data
        embvec_header_offset: usize, //Offset of header
    ) -> EmbVecAccessor<T> {
        Self {
            handle: t,
            len_offset: embvec_header_offset + offset_of!(EmbVecHeader, num),
            capacity_offset: embvec_header_offset + offset_of!(EmbVecHeader, num_allocated),
            start_offset,
        }
    }

    pub fn all(&mut self) -> Result<Vec<MessageId>> {
        let count = self.len()?;
        let mut temp = Vec::with_capacity(count);
        self.handle
            .seek(SeekFrom::Start(self.start_offset as u64))?;
        for _i in 0..count {
            let msg: MessageId = self.handle.read_pod()?;
            temp.push(msg);
        }
        Ok(temp)
    }
    pub fn capacity(&mut self) -> Result<usize> {
        self.handle
            .seek(SeekFrom::Start((self.capacity_offset) as u64))?;
        Ok(self.handle.read_u16::<LittleEndian>()? as usize)
    }
    pub fn len(&mut self) -> Result<usize> {
        self.handle
            .seek(SeekFrom::Start((self.len_offset) as u64))?;
        Ok(self.handle.read_u16::<LittleEndian>()? as usize)
    }
    pub fn set_len(&mut self, val: usize) -> Result<()>
    where
        T: Write,
    {
        self.handle.seek(SeekFrom::Start(self.len_offset as u64))?;
        Ok(self
            .handle
            .write_u16::<LittleEndian>(val.try_into().unwrap())?)
    }
    pub fn clear(&mut self) -> Result<()>
    where
        T: Write,
    {
        self.set_len(0)?;
        Ok(())
    }

    pub fn retain(&mut self, mut f: impl FnMut(MessageId) -> bool) -> Result<()>
    where
        T: Write,
    {
        let mut i = 0;
        let mut len = self.len()?;
        while i < len {
            let item = self.get(i)?;
            if !f(item) {
                if i + 1 != len {
                    let last = self.get(len - 1)?;
                    self.set(i, last)?;
                }
                len -= 1;
            } else {
                i += 1;
            }
        }
        self.set_len(len)?;
        Ok(())
    }

    pub fn get(&mut self, index: usize) -> Result<MessageId> {
        debug_assert!(index < self.len()?);
        let target = self.start_offset + size_of::<MessageId>() * index;
        self.handle.seek(SeekFrom::Start(target as u64))?;
        self.handle.read_pod()
    }
    pub fn set(&mut self, index: usize, val: MessageId) -> Result<()>
    where
        T: Write,
    {
        debug_assert!(index < self.len()?);
        let target = self.start_offset + size_of::<MessageId>() * index;
        self.handle.seek(SeekFrom::Start(target as u64))?;
        self.handle.write_pod(&val)
    }

    pub fn remove(&mut self, id: MessageId) -> Result<()>
    where
        T: Write,
    {
        let count = self.len()?;
        self.handle
            .seek(SeekFrom::Start(self.start_offset as u64))?;
        for i in 0..count {
            let msg: MessageId = self.handle.read_pod()?;
            if msg == id {
                if i != count - 1 {
                    let last = self.get(count - 1)?;
                    self.set(i, last)?;
                }
                self.set_len(count - 1)?;
                return Ok(());
            }
        }
        Ok(())
    }
    pub fn extend<'a>(&mut self, ids: impl Iterator<Item = &'a MessageId>) -> Result<()>
    where
        T: Write,
    {
        for id in ids {
            self.push(*id)?;
        }
        Ok(())
    }
    pub fn push(&mut self, id: MessageId) -> Result<bool>
    where
        T: Write,
    {
        let self_len = self.len()?;
        if self_len < self.capacity()? {
            let target = self.start_offset + size_of::<MessageId>() * self_len;
            self.handle.seek(SeekFrom::Start(target as u64))?;
            self.handle.write_pod(&id)?;
            self.set_len(self_len + 1)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

const MAGIC: [u8; 8] = [b'N', 152, 202, 45, 103, 197, 68, b'N'];

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct FileHeaderEntry {
    header_checksum: [u8; HASH_SIZE],
    magic: [u8; 8],
    // Checksum of message
    message_sha2: [u8; HASH_SIZE],
    // The following field, and below, are included in the header checksum!
    payload_size: u64,
    message_id: MessageId,
    has_been_transmitted: u8,
    padding1: u8,
    padding2: u16,
    num_parents: EmbVecHeader,
    num_children: EmbVecHeader,
    padding4: u32,
}

// Safety: FileHeaderEntry contains only NoatunStorable types
unsafe impl NoatunStorable for FileHeaderEntry {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::FileHeaderEntry/1")
    }
}

impl FileHeaderEntry {
    pub fn is_deleted(&self) -> bool {
        self.message_sha2 == [0u8; 16]
    }
    pub fn set_deleted(&mut self) {
        self.message_sha2 = [0u8; 16];
    }

    pub fn total_size(&self) -> usize {
        (size_of::<FileHeaderEntry>()
            + (self.num_parents.num_allocated as usize + self.num_children.num_allocated as usize)
                * size_of::<MessageId>()
            + self.payload_size as usize)
            .next_multiple_of(align_of::<FileHeaderEntry>())
    }
    /// Size of all headers, including parent + child lists.
    pub fn offset_of_payload(&self) -> usize {
        size_of::<FileHeaderEntry>()
            + (self.num_parents.num_allocated as usize + self.num_children.num_allocated as usize)
                * size_of::<MessageId>()
    }
    /// Size of all headers, including parent + child lists.
    pub fn start_of_data_covered_by_header_checksum(&self) -> usize {
        offset_of!(FileHeaderEntry, payload_size)
    }
}

pub trait ReadPod {
    fn read_pod<T: NoatunStorable>(&mut self) -> Result<T>;
}
pub trait WritePod {
    fn write_pod<T: NoatunStorable>(&mut self, pod: &T) -> Result<()>;
}
impl<R: Read> ReadPod for R {
    fn read_pod<T: NoatunStorable>(&mut self) -> Result<T> {
        let mut zeroed = T::zeroed();
        let bytes = bytes_of_mut(&mut zeroed);
        self.read_exact(bytes)?;
        Ok(zeroed)
    }
}
impl<W: Write> WritePod for W {
    fn write_pod<T: NoatunStorable>(&mut self, pod: &T) -> Result<()> {
        let bytes = bytes_of(pod);
        self.write_all(bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct U1(u8);

// Safety: u8's are NoatunStorable
unsafe impl NoatunStorable for U1 {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::U1/1")
    }
}

impl Display for U1 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.0)
    }
}
impl U1 {
    const ONE: U1 = U1(1);
    const ZERO: U1 = U1(0);
    fn index(self) -> usize {
        if self.0 == 1 {
            1
        } else {
            0
        }
    }
    fn swap(&mut self) {
        *self = Self::from_index(self.other());
    }
    fn other(self) -> usize {
        (self.0 as usize + 1) % 2
    }
    fn from_index(index: usize) -> Self {
        assert!(index < 2);
        Self(index as u8)
    }
}

impl FileOffset {
    #[cfg(test)]
    fn bits(self) -> u64 {
        self.0
    }

    #[cfg(test)]
    fn from_bits(bits: u64) -> FileOffset {
        Self(bits)
    }

    fn new(offset: u64, file: U1) -> Self {
        assert!(offset < u64::MAX / 2); //Note, must <, not <=, to avoid magic value u64::MAX
        Self((offset << 1) | file.index() as u64)
    }
    fn set_deleted(&mut self) {
        self.0 = u64::MAX;
    }

    #[cfg(test)]
    pub(crate) fn deleted() -> Self {
        Self(u64::MAX)
    }
    pub(crate) fn is_deleted(self) -> bool {
        self.0 == u64::MAX
    }
    fn offset(self) -> Option<u64> {
        (self.0 != u64::MAX).then_some(self.0 >> 1)
    }

    fn file_and_offset(self) -> Option<(U1, u64)> {
        (self.0 != u64::MAX).then_some((
            if self.0 & 1 == 1 { U1::ONE } else { U1::ZERO },
            self.0 >> 1,
        ))
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct IndexEntry {
    pub(crate) message: MessageId,
    /// Offset into the logical file-area, or deletion-marker
    pub(crate) file_offset: FileOffset,
    /// This is the size with header, parents and payload.
    pub(crate) file_total_size: u64,
}

impl IndexEntry {
    pub(crate) fn is_deleted(&self) -> bool {
        self.file_offset.is_deleted()
    }
}

// Safety: IndexEntry contains only NoatunStorable types
unsafe impl NoatunStorable for IndexEntry {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::IndexEntry/1")
    }
}

impl PartialEq for IndexEntry {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
    }
}
impl Eq for IndexEntry {}
impl PartialOrd for IndexEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for IndexEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.message.cmp(&other.message)
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct DataFileEntry {
    /// The size of this file, in bytes.
    nominal_size: u64,
    /// The number of bytes actually used.
    /// If this becomes small, compaction may be in order
    /// This figure includes the file header (and any padding we might want to add later).
    /// This means that an unfragmented file has `bytes_used` == `nominal_size`
    ///
    /// NOTE! When the compaction pointer is moved, 'bytes_used' is subtracted from. This
    /// increases fragmentation, meaning the rest of the file is likely to be compacted.
    /// However, during recovery, items before the compaction pointer are also recovered.
    /// Note that if the file has been corrupted at runtime, it's conceivable that the
    /// index could reference entries before the compaction pointer. In this case, bytes_used
    /// would become negative at deletion of such an entry. Saturating subtraction is used
    /// for this error case, with a benign result.
    bytes_used: u64,

    /// All file data before this offset has been moved
    compaction_pointer: u64,
}

impl DataFileEntry {
    pub fn fragmentation(&self) -> u32 {
        let data_hole_bytes = self.nominal_size.saturating_sub(self.bytes_used);
        let perc_fragmentation = data_hole_bytes.saturating_mul(100) / self.nominal_size.max(1);
        perc_fragmentation as u32
    }
}

// Safety: DataFileEntry contains only NoatunStorable types
unsafe impl NoatunStorable for DataFileEntry {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::DataFileEntry/1")
    }
}

#[derive(Debug)]
struct DataFileInfo {
    file: FileAccessor,
    file_number: U1,
    outstanding: Vec<Range<u64>>,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct StoreHeader {
    entries: u32,
    /// The number of unused entries in this store-header.
    ///
    /// When this count grows too large compared to 'entries', we should
    /// compact
    holes: u32,
    // Compact file=1 + old file being compacted=0
    data_files: [DataFileEntry; 2],
    /// Current cutoff information
    ///
    /// That is, the timestamp before which no new messages are ever expected
    /// to be encountered, plus the hash of all messages prior to this timestamp.
    cutoff: CutOffHashPos,
    /// Cutoff data for the previous period, i.e, one era earlier
    prev_cutoff: CutOffHashPos,
}

// Safety: StoreHeader contains only NoatunStorable types
unsafe impl NoatunStorable for StoreHeader {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::StoreHeader/1")
    }
}

pub(crate) struct OnDiskMessageStore<M> {
    index_mmap: FileAccessor,
    data_files: [DataFileInfo; 2],
    active_file: U1,
    phantom: PhantomData<M>,
    loaded_existing: bool,
    filesystem_sync_disabled: bool,
    auto_compact_enabled: bool,
    cutoff_index: usize,

    index_compaction: Counter,
    index_size: Gauge,
    index_holes: Gauge,
    messages_written: Counter,
    message_bytes_written: Counter,
    data_fragmentation_gauges: [Gauge; 2],
}

struct MessageWriteReport {
    start_pos: u64,
    total_size: u64,
}
#[derive(Default)]
struct RemainingChildAssignments {
    parent2child: HashMap<MessageId, Vec<MessageId>>,
}

/// Returns the new last element
fn dedup_slice<T: PartialEq + Copy>(slice: &mut [T]) -> usize {
    if slice.len() <= 1 {
        return slice.len();
    }
    let mut src_index = 1;
    let mut dst_index = 1;
    while src_index < slice.len() {
        if slice[src_index] == slice[src_index - 1] {
            src_index += 1;
            continue;
        }
        slice[dst_index] = slice[src_index];
        dst_index += 1;
        src_index += 1;
    }
    dst_index
}

impl<M> OnDiskMessageStore<M> {
    pub fn sync_all(&mut self) -> Result<()> {
        self.index_mmap.sync_all()?;
        Ok(())
    }
    pub(crate) fn disk_space_used_bytes(&self) -> u64 {
        self.index_mmap.disk_space_used_bytes()
            + self.data_files[0].file.disk_space_used_bytes()
            + self.data_files[1].file.disk_space_used_bytes()
    }

    pub fn sync_outstanding(&mut self) -> Result<()> {
        for file in self.data_files.iter_mut() {
            while !file.outstanding.is_empty() {
                let range = file.outstanding.last().unwrap();
                if !self.filesystem_sync_disabled {
                    file.file
                        .sync_range(range.start as usize, (range.end - range.start) as usize)?;
                }
                file.outstanding.pop();
            }
        }

        Ok(())
    }

    fn calculate_cutoff_index(header: &StoreHeader, index_slice: &[IndexEntry]) -> usize {
        let cutoff_time = header.cutoff.before_time.to_noatun_time();
        let (Ok(mut index) | Err(mut index)) =
            index_slice.binary_search_by_key(&cutoff_time, |x| x.message.timestamp());

        while index > 0 {
            if index_slice[index - 1].message.timestamp() == cutoff_time {
                index -= 1;
                continue;
            }
            break;
        }

        index
    }

    #[cfg(any(debug_assertions, feature = "debug"))]
    pub(crate) fn debug_verify_cutoff_index(&self) -> Result<()> {
        let (header, index) = self.header_and_index()?;
        let mut correct_index = 0;
        let mut holes = 0;
        for item in index {
            if item.is_deleted() {
                holes += 1;
            }
        }
        for item in index {
            if item.message.timestamp() < header.cutoff.before_time {
                correct_index += 1;
            } else {
                break;
            }
        }
        if holes != header.holes {
            println!("Index: {index:#?})");
        }
        assert_eq!(
            holes, header.holes,
            "actual hole acount != memoized hole count"
        );
        let calc = Self::calculate_cutoff_index(header, index);
        if correct_index != calc {
            println!("Index: {index:#?}");
            println!("Correct: {correct_index}, according to calc: {calc}");
            std::process::abort();
        }
        assert_eq!(correct_index, Self::calculate_cutoff_index(header, index));
        assert_eq!(correct_index, self.cutoff_index);
        Ok(())
    }
    pub(crate) fn cutoff_index(&self) -> SequenceNr {
        SequenceNr::from_index(self.cutoff_index)
    }
    pub(crate) fn set_cutoff_index(&mut self, cutoff_index: SequenceNr) {
        self.cutoff_index = cutoff_index.index();
        #[cfg(any(debug_assertions, feature = "debug"))]
        {
            self.debug_verify_cutoff_index().unwrap();
        }
    }

    pub fn get_upstream_of(
        &self,
        message_ids: impl DoubleEndedIterator<Item = (MessageId, /*query count*/ usize)>,
    ) -> Result<impl Iterator<Item = (MessageHeader, usize /*query count*/)> + '_>
    where
        M: Message,
    {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;
        let data_files = &self.data_files;

        let mut index = 0;

        let mut breadth = HashMap::new();
        #[cfg(debug_assertions)]
        let mut prev = None;

        let first = true;
        for (message_id, count) in message_ids.rev() {
            #[cfg(debug_assertions)]
            {
                if let Some(prev) = prev {
                    assert!(prev > message_id); //All unique, all sorted.
                }
                prev = Some(message_id);
            }
            breadth.insert(
                message_id,
                (
                    count, /*original count*/
                    count, /*remaining count*/
                ),
            );

            if first {
                if let Ok(cand) = message_index.binary_search_by_key(&message_id, |x| x.message) {
                    index = index.max(cand + 1);
                }
            }
        }

        Ok(message_index[0..index]
            .iter()
            .rev()
            .map(move |x| {
                if let Some((original_count, remaining_count)) = breadth.remove(&x.message) {
                    if x.file_offset.is_deleted() {
                        return (None, breadth.is_empty());
                    }
                    let msg = match Self::read_msg_header(x, data_files, None) {
                        Ok(msg) => msg,
                        Err(err) => {
                            warn!("Failed to parse message: {:?}", err);
                            return (None, false);
                        }
                    };

                    if remaining_count > 0 {
                        for parent in msg.parents.iter() {
                            let (entry_orig_count, entry_remaining_count) =
                                breadth.entry(*parent).or_insert((0, 0));
                            *entry_orig_count = (*entry_orig_count).max(original_count);
                            *entry_remaining_count += remaining_count - 1;
                        }
                    }
                    (Some((msg, original_count)), false)
                } else {
                    (None, breadth.is_empty())
                }
            })
            .take_while(|(_, done)| !*done)
            .filter_map(|(msg, _)| msg))
    }

    #[inline]
    fn provide_index_map(
        map: &mut FileAccessor,
        extra: usize, //items, not bytes
    ) -> Result<()> {
        let xlen = map.used_space();

        if xlen < size_of::<StoreHeader>() {
            map.grow(size_of::<StoreHeader>())?;
            map.map_mut().fill(0);
        }

        let slice: &mut [u8] = map.map_mut();
        let header: &StoreHeader = from_bytes(&slice[0..size_of::<StoreHeader>()]);
        let file_capacity =
            (xlen.saturating_sub(size_of::<StoreHeader>())) / size_of::<IndexEntry>();
        let used = header.entries as usize;

        if file_capacity.saturating_sub(used) >= extra {
            return Ok(());
        }

        Self::do_grow_file(map, file_capacity + extra)
    }

    #[cold]
    fn do_grow_file(map: &mut FileAccessor, base_capacity: usize) -> Result<()> {
        if base_capacity >= u32::MAX as usize {
            return Err(anyhow!("Database max-size reached (~2^32 entries)"));
        }

        // Need to realloc
        let mut new_entries = base_capacity.saturating_mul(2);
        if new_entries > (u32::MAX / 2 + u32::MAX / 4) as usize {
            new_entries = u32::MAX as usize;
        }
        let new_file_size = (new_entries) * (size_of::<IndexEntry>()) + (size_of::<StoreHeader>());

        map.grow(new_file_size)
            .with_context(|| format!("resizing index file to {new_file_size}"))?;

        Ok(())
    }

    #[inline]
    fn header_mut(map: &mut FileAccessor) -> Result<&mut StoreHeader> {
        Self::provide_index_map(map, 0)?;

        let slice: &mut [u8] = map.map_mut();
        let header: &mut StoreHeader = from_bytes_mut(&mut slice[0..size_of::<StoreHeader>()]);
        Ok(header)
    }

    #[allow(unused)]
    pub(crate) fn contains_index(&self, index: usize) -> Result<bool> {
        let (_header, index_entries) = self.header_and_index()?;

        match index_entries.get(index) {
            Some(found) => {
                info!(
                    "Looked up index {}: {:?}, msg: {:?}",
                    index, found.file_offset, found.message
                );
                Ok(!found.file_offset.is_deleted())
            }
            None => Ok(false),
        }
    }

    /// NOTE! This returns also unallocated message-entries, for writing, not just those
    /// that are initialized with data
    #[inline]
    fn header_and_index_mut_uninit(
        map: &mut FileAccessor,
        extra: usize,
    ) -> Result<(&mut StoreHeader, &mut [IndexEntry])> {
        Self::provide_index_map(map, extra)?;
        let slice: &mut [u8] = map.map_mut();
        let (store_header_bytes, index_bytes) = slice.split_at_mut(size_of::<StoreHeader>());
        let header: &mut StoreHeader = from_bytes_mut(store_header_bytes);
        let rest = dyn_cast_slice_mut(index_bytes);
        Ok((header, rest))
    }
    #[inline]
    fn header_and_index_mut(
        map: &mut FileAccessor,
    ) -> Result<(&mut StoreHeader, &mut [IndexEntry])> {
        let (store, index) = Self::header_and_index_mut_uninit(map, 0)?;

        let entries = store.entries as usize;
        Ok((store, &mut index[..entries]))
    }
    /// NOTE! This does not return unwritten message entries. This is because
    /// since it doesn't support writing, there's no point in returning unwritten entries.
    #[inline]
    fn header_and_index(&self) -> Result<(&StoreHeader, &[IndexEntry])> {
        let slice: &[u8] = self.index_mmap.map();
        let (store_header_bytes, index_bytes) = slice.split_at(size_of::<StoreHeader>());
        let header: &StoreHeader = from_bytes(store_header_bytes);
        let rest = dyn_cast_slice(index_bytes);
        Ok((header, &rest[0..header.entries as usize]))
    }

    #[inline]
    fn header(&self) -> Result<&StoreHeader> {
        let slice: &[u8] = self.index_mmap.map();
        let (store_header_bytes, _index_bytes) = slice.split_at(size_of::<StoreHeader>());
        let header: &StoreHeader = from_bytes(store_header_bytes);
        Ok(header)
    }

    /// Compact index if needed.
    ///
    /// Returns Ok(true) if compaction needed.
    pub fn compact_index_if_needed(&mut self, force: bool) -> Result<bool> {
        let (header, _index_entries) = self.header_and_index()?;
        let remain = header.entries - header.holes;
        if force || remain <= header.holes / 4 {
            self.compact_index()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Compact the index.
    ///
    /// Note, this renumbers all messages in the index. The new cutoff index
    /// is returned.
    pub fn compact_index(&mut self) -> Result<()> {
        #[cfg(any(debug_assertions, feature = "debug"))]
        self.debug_verify_cutoff_index().unwrap();

        let (header, index_entries) = Self::header_and_index_mut(&mut self.index_mmap)?;

        let mut write_ptr = 0;
        for read_ptr in 0..header.entries {
            let used = !index_entries[read_ptr as usize].file_offset.is_deleted();
            if used {
                if write_ptr != read_ptr {
                    index_entries[write_ptr as usize] = index_entries[read_ptr as usize];
                }
                write_ptr += 1;
            }
        }

        header.entries = write_ptr;
        header.holes = 0;

        let (header, index_entries) = self.header_and_index()?;
        let new_cutoff_index = Self::calculate_cutoff_index(header, index_entries);
        self.cutoff_index = new_cutoff_index;

        self.index_compaction.increment(1);

        self.update_index_metrics()?;

        #[cfg(debug_assertions)]
        {
            let (header, index_entries) = self.header_and_index()?;
            Self::validate_holes(header, index_entries);
            self.debug_verify_cutoff_index().unwrap();
        }

        Ok(())
    }

    /// Recover from index corruption.
    /// You should never need to call this explicitly, it will be called automatically
    /// whenever a transaction is attempted and database is in a corrupted state
    pub fn recover(
        &mut self,
        mut report_inserted: impl FnMut(MessageId, &[MessageId]) -> Result<()>,
        now: NoatunTime,
        cutoff_config: &CutOffConfig,
    ) -> Result<()>
    where
        M: Message,
    {
        debug!("Start recovery procedure");
        self.index_mmap.truncate(0)?;

        let magic_finder = memchr::memmem::Finder::new(&MAGIC);

        self.index_mmap.write_zeroes(size_of::<StoreHeader>())?;

        const {
            assert!(align_of::<StoreHeader>() <= DISK_MAP_ALIGNMENT);
        }

        // Safety: The mmap is always large enough and aligned
        let pending_index_header: &mut StoreHeader =
            unsafe { &mut *(self.index_mmap.map_mut_ptr() as *mut StoreHeader) };
        self.index_mmap
            .seek(SeekFrom::Start(size_of::<StoreHeader>() as u64))?;

        debug_assert_eq!(size_of_val(&MAGIC), 8);

        assert_eq!(pending_index_header.entries, 0);

        for (file_info, file_entry) in self
            .data_files
            .iter_mut()
            .zip(pending_index_header.data_files.iter_mut())
        {
            info!("Reading main data file {:?}", file_info.file_number);
            let prev_used_space = file_info.file.used_space();
            file_info.file.set_used_space_to_full_file();
            let file_length = file_info.file.used_space() as u64;
            file_entry.compaction_pointer = 0;
            file_info.file.seek(SeekFrom::Start(0))?;
            let mut largest_used_offset = 0;
            loop {
                let file_offset = file_info.file.stream_position()?;
                if file_offset == file_length {
                    break;
                }

                fn read_header_and_parents(
                    file_info: &mut FileAccessor,
                ) -> Result<(FileHeaderEntry, Vec<MessageId>, usize)> {
                    let file_offset = file_info.stream_position()?;
                    let header = file_info.read_pod::<FileHeaderEntry>()?;
                    if header.magic != MAGIC {
                        bail!("Bad magic");
                    }
                    let tot_size = header.total_size();

                    if header.is_deleted() {
                        if tot_size == 0 {
                            bail!("Corrupt file");
                        }
                        file_info.seek(SeekFrom::Start(file_offset + tot_size as u64))?;
                        bail!("Header is deleted");
                    }

                    let mut parvec = EmbVecAccessor::new_parents(&mut *file_info, file_offset);
                    let parents = parvec.all()?;

                    let payload_start = file_offset as usize + header.offset_of_payload();
                    let checksum_start =
                        file_offset as usize + header.start_of_data_covered_by_header_checksum();
                    let header_checksum = file_info.with_bytes_at(
                        checksum_start,
                        payload_start - checksum_start,
                        sha2,
                    )?;
                    if header.header_checksum != header_checksum {
                        bail!(
                            "Header checksum mismatch: {:?} {:?}",
                            header.header_checksum,
                            header_checksum
                        );
                    }

                    file_info.seek(SeekFrom::Start(
                        file_offset + header.offset_of_payload() as u64,
                    ))?;

                    file_info.with_bytes(header.payload_size as usize, |payload_bytes| {
                        let actual_sha2 = sha2_message(header.message_id, payload_bytes);
                        if actual_sha2 != header.message_sha2 {
                            return Err(anyhow!("data corrupted on disk - checksum mismatch"));
                        }
                        Ok(())
                    })??;

                    file_info.seek(SeekFrom::Start(file_offset + tot_size as u64))?;
                    Ok((header, parents, tot_size))
                }

                let (header, parents, msg_size_bytes) =
                    match read_header_and_parents(&mut file_info.file) {
                        Ok((header, parents, msg_size_bytes)) => {
                            debug_assert!(file_info.file.stream_position()? >= largest_used_offset);
                            largest_used_offset = file_info.file.stream_position()?;
                            (header, parents, msg_size_bytes)
                        }
                        Err(err) => {
                            if file_offset < prev_used_space as u64 {
                                warn!("Error reading message: {} @ offset {}", err, file_offset);
                            } else {
                                trace!("Message not recovered: {} @ offset {}", err, file_offset);
                            }
                            let magic_search_start = file_offset as usize
                                + offset_of!(FileHeaderEntry, magic)
                                + size_of_val(&MAGIC);
                            let new_offset = file_info.file.with_all_bytes(|bytes| {
                                magic_finder
                                    .find(&bytes[magic_search_start..])
                                    .map(|x| {
                                        x + magic_search_start - offset_of!(FileHeaderEntry, magic)
                                    })
                                    .unwrap_or(file_length as usize)
                            });

                            file_info.file.seek_to(new_offset)?;
                            continue;
                        }
                    };

                report_inserted(header.message_id, &parents)?;
                file_entry.bytes_used += msg_size_bytes as u64;
                self.index_mmap.write_pod(&IndexEntry {
                    message: header.message_id,
                    file_offset: FileOffset::new(file_offset, file_info.file_number),
                    file_total_size: msg_size_bytes as u64,
                })?;
                pending_index_header.entries += 1;
            }
            file_info.file.set_used_space(largest_used_offset as usize);
            file_info.file.seek_to(largest_used_offset as usize)?;
            file_entry.nominal_size = largest_used_offset;
        }

        let (index_header, mut index) = Self::header_and_index_mut(&mut self.index_mmap)?;
        index.sort();
        index_header.entries = dedup_slice(index) as u32;
        index = &mut index[0..index_header.entries as usize];

        for item in index.iter() {
            debug_assert!(!item.file_offset.is_deleted());
            if let Some((file, offset)) = item.file_offset.file_and_offset() {
                let mut file = &mut self.data_files[file.index()].file;
                let mut parent_accessor = EmbVecAccessor::new_parents(&mut file, offset);

                parent_accessor
                    .retain(|cand| index.binary_search_by_key(&cand, |x| x.message).is_ok())?;

                let mut child_accesor = EmbVecAccessor::new_children(&mut file, offset)?;
                child_accesor
                    .retain(|cand| index.binary_search_by_key(&cand, |x| x.message).is_ok())?;
            }
        }

        self.cutoff_index = Self::recover_cutoff_state(now, index_header, index, cutoff_config)?;

        self.update_index_metrics()?;

        #[cfg(any(debug_assertions, feature = "debug"))]
        {
            self.debug_verify_cutoff_index()?;
        }

        Ok(())
    }

    fn update_index_metrics(&self) -> Result<()> {
        let index_header = self.header()?;
        self.index_size.set(index_header.entries);
        self.index_holes.set(index_header.holes);
        Ok(())
    }

    fn recover_cutoff_state(
        time_now: NoatunTime,
        index_header: &mut StoreHeader,
        index: &[IndexEntry],
        config: &CutOffConfig,
    ) -> Result<usize> {
        let cutoff_time = config.nominal_cutoff(CutOffTime::from_noatun_time(time_now));
        index_header.cutoff.before_time = cutoff_time;
        index_header.prev_cutoff.before_time = cutoff_time.saturating_sub(config.stride);
        index_header.cutoff.hash = CutoffHash::ZERO;
        index_header.prev_cutoff.hash = CutoffHash::ZERO;
        let cutoff_time_noatun: NoatunTime = cutoff_time.into();
        for item in index.iter() {
            if item.message.timestamp() >= cutoff_time_noatun {
                break;
            }
            index_header.cutoff.apply(item.message, "recovery");
            index_header.prev_cutoff.apply(item.message, "recovery");
        }

        Ok(Self::calculate_cutoff_index(index_header, index))
    }

    pub fn contains_message(&self, start: MessageId) -> Result<bool> {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;
        match message_index.binary_search_by_key(&start, |x| x.message) {
            Ok(index) => Ok({
                let del = !message_index[index].file_offset.is_deleted();
                del
            }),
            Err(_err) => Ok(false),
        }
    }
    /// Get all messages with id start or greater
    #[cfg(test)]
    fn query_greater(&self, start: MessageId) -> Result<impl Iterator<Item = &IndexEntry>> {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;

        let (Ok(index) | Err(index)) = message_index.binary_search_by_key(&start, |x| x.message);

        Ok(message_index[index..]
            .iter()
            .filter(|x| !x.file_offset.is_deleted()))
    }

    /// Get all messages with index `index` or greater. Note, the index series has holes,
    /// so the indices of the returned items don't need to be contiguous, or even start at 'index'
    pub fn query_by_index(
        &self,
        index: usize,
    ) -> Result<impl Iterator<Item = (usize /*seqnr/index*/, MessageFrame<M>)> + '_>
    where
        M: Message,
    {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;

        let data_files = &self.data_files;
        Ok(message_index[index..]
            .iter()
            .enumerate()
            .filter(|(_sub_index, x)| !x.file_offset.is_deleted())
            .map(move |(sub_index, x)| (index + sub_index, Self::read_msg(x, data_files, None)))
            .filter_map(|(index, x)| match x {
                Ok(x) => Some((index, x)),
                Err(x) => {
                    warn!("Failed to load message #{}: {:?}", index, x);
                    None
                }
            }))
    }

    /// This conservatively returns 'true' for deleted SequenceNrs
    pub(crate) fn may_have_been_transmitted(&self, msg: SequenceNr) -> Result<bool> {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;

        if let Some(index) = message_index.get(msg.index()) {
            if let Some((file, offset)) = index.file_offset.file_and_offset() {
                let mut file = self.data_files[file.index()].file.readonly();
                file.seek(SeekFrom::Start(offset))
                    .context("Seeking to message data")?;
                let header: FileHeaderEntry = file.read_pod()?;
                return Ok(header.has_been_transmitted != 0);
            }
            Ok(true)
        } else {
            Ok(true)
        }
    }
    fn read_msg_header(
        entry: &IndexEntry,
        data_files: &[DataFileInfo; 2],
        children: Option<&mut Vec<MessageId>>,
    ) -> Result<MessageHeader> {
        let Some((file, offset)) = entry.file_offset.file_and_offset() else {
            return Err(anyhow!(
                "Message not found in store (appears deleted): {:?}",
                entry.message
            ));
        };

        let mut file = data_files[file.index()].file.readonly();
        file.seek(SeekFrom::Start(offset))
            .context("Seeking to message data")?;
        let header: FileHeaderEntry = file.read_pod()?;

        let mut parent_accessor = EmbVecAccessor::new_parents(&mut file, offset);

        let parents = parent_accessor.all()?;

        if let Some(out_children) = children {
            let mut child_accessor = EmbVecAccessor::new_children(&mut file, offset)?;
            *out_children = child_accessor.all()?;
        }

        Ok(MessageHeader {
            id: header.message_id,
            parents,
        })
    }

    fn read_msg(
        entry: &IndexEntry,
        data_files: &[DataFileInfo; 2],
        children: Option<&mut Vec<MessageId>>,
    ) -> Result<MessageFrame<M>>
    where
        M: Message,
    {
        let Some((file, offset)) = entry.file_offset.file_and_offset() else {
            return Err(anyhow!(
                "Message not found in store (appears deleted): {:?}",
                entry.message
            ));
        };

        let mut file = data_files[file.index()].file.readonly();
        file.seek(SeekFrom::Start(offset))
            .context("Seeking to message data")?;
        let header: FileHeaderEntry = file.read_pod()?;

        let mut parent_accessor = EmbVecAccessor::new_parents(&mut file, offset);

        let parents = parent_accessor.all()?;

        if let Some(output_children) = children {
            let mut child_accessor = EmbVecAccessor::new_children(&mut file, offset)?;
            *output_children = child_accessor.all()?;
        }

        file.seek(SeekFrom::Start(offset + header.offset_of_payload() as u64))?;

        let msg_payload = file.with_bytes(header.payload_size as usize, |bytes| {
            if sha2_message(header.message_id, bytes) != header.message_sha2 {
                return Err(anyhow!("Data corruption detected!"));
            }
            M::deserialize(bytes)
        })??;

        debug_assert_eq!(header.message_id, entry.message);
        Ok(MessageFrame {
            header: MessageHeader {
                id: header.message_id,
                parents,
            },
            payload: msg_payload,
        })
    }

    /// Delete the data file, does not update the index
    fn delete_msg_from_file(
        entry: &mut IndexEntry,
        data_files: &mut [DataFileInfo; 2],
    ) -> Result<()> {
        let Some((file, offset)) = entry.file_offset.file_and_offset() else {
            return Err(anyhow!(
                "Message not found in store (appears deleted): {:?}",
                entry.message
            ));
        };
        let file = &mut data_files[file.index()].file;

        const {
            assert!(align_of::<FileHeaderEntry>() <= DISK_MAP_ALIGNMENT);
        }
        // Safety: file is large enough, and 'offset' is always aligned on FileHeaderEntry
        let header: &mut FileHeaderEntry = unsafe { file.access_pod_mut(offset as usize)? };

        header.set_deleted();

        entry.file_offset.set_deleted();

        Ok(())
    }

    /// Return the given message, or None if it does not exist.
    /// If the call itself fails, returns Err.
    pub fn read_message(&self, id: MessageId) -> Result<Option<MessageFrame<M>>>
    where
        M: Message,
    {
        let (_header, message_index) = self.header_and_index().context("Reading index file")?;

        let data_files = &self.data_files;
        match message_index.binary_search_by_key(&id, |x| x.message) {
            Ok(index) => {
                let entry = &message_index[index];
                if entry.file_offset.is_deleted() {
                    return Ok(None);
                }
                let msg = Self::read_msg(entry, data_files, None)?;
                Ok(Some(msg))
            }
            Err(_index) => Ok(None),
        }
    }

    /// Return the given message, or None if it does not exist.
    /// If the call itself fails, returns Err.
    pub fn read_message_and_children(
        &self,
        id: MessageId,
    ) -> Result<Option<(MessageFrame<M>, Vec<MessageId>)>>
    where
        M: Message,
    {
        let (_header, message_index) = self.header_and_index().context("Reading index file")?;

        let data_files = &self.data_files;
        match message_index.binary_search_by_key(&id, |x| x.message) {
            Ok(index) => {
                let entry = &message_index[index];
                if entry.file_offset.is_deleted() {
                    return Ok(None);
                }
                let mut children = vec![];
                let msg = Self::read_msg(entry, data_files, Some(&mut children))?;
                Ok(Some((msg, children)))
            }
            Err(_index) => Ok(None),
        }
    }
    /// Return the given message, or None if it does not exist.
    pub fn read_message_header_and_children_by_index(
        &self,
        index: SequenceNr,
    ) -> Result<Option<(MessageHeader, Vec<MessageId>)>> {
        let (_header, message_index) = self.header_and_index().context("Reading index file")?;

        let data_files = &self.data_files;
        let entry = &message_index[index.index()];
        if entry.file_offset.is_deleted() {
            return Ok(None);
        }
        let mut children = vec![];
        let msg = Self::read_msg_header(entry, data_files, Some(&mut children))?;
        Ok(Some((msg, children)))
    }

    /// Return the given message, or None if it does not exist.
    pub fn read_message_header_and_children(
        &self,
        id: MessageId,
    ) -> Result<Option<(MessageHeader, Vec<MessageId>)>> {
        let (_header, message_index) = self.header_and_index().context("Reading index file")?;

        let data_files = &self.data_files;
        match message_index.binary_search_by_key(&id, |x| x.message) {
            Ok(index) => {
                let entry = &message_index[index];
                if entry.file_offset.is_deleted() {
                    return Ok(None);
                }
                let mut children = vec![];
                let msg = Self::read_msg_header(entry, data_files, Some(&mut children))?;
                Ok(Some((msg, children)))
            }
            Err(_index) => Ok(None),
        }
    }

    /// Return message with the given id.
    /// If no such message exist, return the index of the next message with a larger id.
    /// If the given id is larger than the largest MessageId, return the index of the last message
    pub fn get_insertion_point(&self, id: MessageId) -> Result<usize> {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;
        if message_index.is_empty() == false && id > message_index.last().unwrap().message {
            return Ok(message_index.len());
        }
        let (Ok(i) | Err(i)) = message_index.binary_search_by_key(&id, |x| x.message);
        Ok(i)
    }

    /// Return the greatest index such that all messages with a smaller index occur before
    /// time. All returned messages will be < time, and all messages before < time will have
    /// a smaller index. If there are no messages, this returns 0.
    /// If all messages are < time, this returns an index "one after the end".
    /// The returned index may point at a removed message.
    pub fn get_index_at_or_after(&self, time: NoatunTime) -> Result<SequenceNr> {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;
        let needle = MessageId::from_parts_raw(time.0, [0; 10])?;
        match message_index.binary_search_by_key(&needle, |x| x.message) {
            Ok(needle_index) => Ok(SequenceNr::from_index(needle_index)),
            Err(index) => Ok(SequenceNr::from_index(index)),
        }
    }

    pub fn get_all_message_ids(&self) -> Result<Vec<MessageId>> {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;
        Ok(message_index
            .iter()
            .filter(|x| !x.file_offset.is_deleted())
            .map(|x| x.message)
            .collect())
    }

    pub fn get_all_messages(&self) -> Result<impl Iterator<Item = MessageFrame<M>> + use<'_, M>>
    where
        M: Message,
    {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;
        let data_files = &self.data_files;
        Ok(message_index
            .iter()
            .filter(|x| !x.file_offset.is_deleted())
            .filter_map(|x| match Self::read_msg(x, data_files, None) {
                Ok(v) => Some(v),
                Err(err) => {
                    warn!("failed to read message {}: {:?}", x.message, err);
                    None
                }
            }))
    }
    pub fn get_all_messages_with_index(
        &self,
    ) -> Result<impl Iterator<Item = (SequenceNr, MessageFrame<M>)> + use<'_, M>>
    where
        M: Message,
    {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;
        let data_files = &self.data_files;
        Ok(message_index
            .iter()
            .enumerate()
            .filter(|(_idx, x)| !x.file_offset.is_deleted())
            .filter_map(|(idx, x)| match Self::read_msg(x, data_files, None) {
                Ok(v) => Some((SequenceNr::from_index(idx), v)),
                Err(err) => {
                    warn!("failed to read message {}: {:?}", x.message, err);
                    None
                }
            }))
    }
    pub fn get_all_messages_with_children(&self) -> Result<Vec<(MessageFrame<M>, Vec<MessageId>)>>
    where
        M: Message,
    {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;
        let data_files = &self.data_files;
        message_index
            .iter()
            .filter(|x| !x.file_offset.is_deleted())
            .map(|x| {
                let mut children = vec![];
                let msg = Self::read_msg(x, data_files, Some(&mut children))?;
                Ok((msg, children))
            })
            .collect()
    }

    /// Delete any message with the given index. Idempotent, does nothing if index is already
    /// deleted or does not exist.
    /// If the call itself fails, returns Err.
    /// Returns true if deleted.
    pub fn mark_deleted_by_index(
        &mut self,
        delete_index: SequenceNr,
        head_tracker: &mut UpdateHeadTracker,
    ) -> Result<bool>
    where
        M: Message,
    {
        let Some((msg, children)) = self.read_message_header_and_children_by_index(delete_index)?
        else {
            warn!("Message {} was already deleted", delete_index);
            return Ok(false);
        };
        info!(
            "Actually mark index {} deleted: {:?} (children: {:?})",
            delete_index, msg.id, children
        );

        let (header, message_index) =
            Self::header_and_index_mut(&mut self.index_mmap).context("Reading index file")?;

        let Some(entry) = message_index.get_mut(delete_index.index()) else {
            trace!("message delete_index not found: {}", delete_index);
            return Ok(false);
        };
        if entry.file_offset.is_deleted() {
            // Already deleted, even though still in index (this should be unreachable, since
            // we already checked it above).
            debug!("internal error, deletion race");
            return Ok(false);
        }

        if let Some((file, _offset)) = entry.file_offset.file_and_offset() {
            header.cutoff.report_delete(entry.message);
            header.prev_cutoff.report_delete(entry.message);

            let id = entry.message;
            let datafile = &mut header.data_files[file.index()];

            datafile.bytes_used = datafile.bytes_used.saturating_sub(entry.file_total_size);

            let perc_fragmentation = datafile.fragmentation();

            debug_assert!(delete_index.index() < header.entries as usize);

            Self::delete_msg_from_file(entry, &mut self.data_files)?;
            debug_assert!(entry.is_deleted());
            header.holes += 1;

            head_tracker.remove_update_head(entry.message)?;

            #[cfg(debug_assertions)]
            Self::validate_holes(header, message_index);

            let parents = msg.parents;
            for parent in &parents {
                self.add_remove_parents_and_children(*parent, &[], None, &children, Some(id))?;
            }

            for child in &children {
                self.add_remove_parents_and_children(*child, &[], Some(id), &[], None)?;
            }
            self.update_index_metrics()?;

            self.data_fragmentation_gauges[file.index()].set(perc_fragmentation as f64);
            if self.auto_compact_enabled && perc_fragmentation > 25 {
                self.do_compact_data(|_, _| true)
                    .context("compacting data files")?;
            }

            Ok(true)
        } else {
            warn!("Message was already deleted");
            Ok(false)
        }
    }

    #[cfg(debug_assertions)]
    #[track_caller]
    fn validate_holes(header: &StoreHeader, entries: &[IndexEntry]) {
        let correct_hole_count = entries
            .iter()
            .filter(|x| x.file_offset.is_deleted())
            .count();
        assert_eq!(
            correct_hole_count as u32, header.holes,
            "actual hole count != memoized hole count"
        );
    }

    /// Advance the cutoff hash to a new later state
    pub fn advance_cutoff_hash(
        &mut self,
        new_prev_cutoff: CutOffHashPos,
        new_cutoff: CutOffHashPos,
    ) -> Result<()> {
        let (header, _message_index) =
            Self::header_and_index_mut(&mut self.index_mmap).context("Reading index file")?;
        header.prev_cutoff = new_prev_cutoff;
        header.cutoff = new_cutoff;
        Ok(())
    }

    pub fn loaded_existing_db(&self) -> bool {
        self.loaded_existing
    }

    /// Returns true if the message existed and was marked as transmitted.
    /// If this returns false, the message didn't (any longer) exist, and must NOT be transmitted
    pub fn mark_transmitted(&mut self, message_id: MessageId) -> Result<bool> {
        let (_header, message_index) =
            Self::header_and_index_mut(&mut self.index_mmap).context("Reading index file")?;
        let Ok(index) = message_index.binary_search_by_key(&message_id, |x| x.message) else {
            trace!("Message to mark as transmitted no longer existed");
            return Ok(false);
        };
        let index_entry = &message_index[index];

        if let Some((file, offset)) = index_entry.file_offset.file_and_offset() {
            // Safety: Offset has a valid alignment, and we have no other view of this map.
            unsafe {
                let file_header: &mut FileHeaderEntry = self.data_files[file.index()]
                    .file
                    .access_pod_mut(offset as usize)?;
                file_header.has_been_transmitted = 1;
            }
            Ok(true)
        } else {
            // Deleted
            Ok(false)
        }
    }

    pub fn get_children_of(&self, msg: MessageId) -> Result<Vec<MessageId>> {
        match self.read_message_header_and_children(msg)? {
            None => {
                bail!("Message not found");
            }
            Some((_hdr, children)) => Ok(children),
        }
    }

    /// Add the given messages.
    /// Note: You should call 'begin_transaction' before this, to mark db as dirty.
    ///
    /// This will determine the position they need to be added to, and will then
    /// merge with the index. Marks the db clean.
    ///
    /// Returns the index that the database has to be rewound to in order to reproject
    /// all added messages. This can be before the index of an added message, if
    /// that index did not correspond to a non-deleted message.
    pub fn append_many_sorted<'a>(
        &mut self,
        messages: impl ExactSizeIterator<Item = &'a MessageFrame<M>>,
        message_inserted: impl FnMut(MessageId, /*parents*/ &[MessageId]) -> Result<()>,
        local: bool,
    ) -> Result<Option<usize>>
    where
        M: Message + Debug + 'a,
    {
        self.do_append_many_sorted(messages, message_inserted, local)
    }

    /// Marks the db as dirty
    /// Find all the given messages and mark their entries in their data files as unused.
    /// Use heuristics to determine if files should be compacted.
    /// Compact files (moving to higher file if suitable).
    /// Delete from the index.
    /// Mark the db as clean.
    /// Messages that are not found are simply ignored. The operation does not stop or fail.
    ///
    /// Without force, transmitted messages are not deleted.
    ///
    /// Returns earliest sequence nr deleted, if any deleted
    ///
    /// This succeeds without deleting if either the iterator was empty,
    /// or all elements in the iterator were already deleted
    pub fn delete_many(
        &mut self,
        messages: impl Iterator<Item = MessageId>,
        update_head_tracker: &mut UpdateHeadTracker,
        force: bool,
    ) -> Result<Option<SequenceNr>>
    where
        M: Message,
    {
        self.do_delete_many(messages, update_head_tracker, force)
    }

    /// Returns earliest sequence nr deleted, if any deleted
    ///
    /// This succeeds without deleting if either the iterator was empty,
    /// or all elements in the iterator were already deleted
    fn do_delete_many(
        &mut self,
        messages: impl Iterator<Item = MessageId>,
        update_head_tracker: &mut UpdateHeadTracker,
        force: bool,
    ) -> Result<Option<SequenceNr>>
    where
        M: Message,
    {
        let mut earliest = None;
        for message in messages {
            let (_header, message_index) =
                Self::header_and_index_mut(&mut self.index_mmap).context("Reading index file")?;

            let Ok(index) = message_index.binary_search_by_key(&message, |x| x.message) else {
                continue;
            };
            let msg = message_index[index];
            if msg.file_offset.is_deleted() {
                continue;
            }

            if !force {
                if let Some((file, offset)) = msg.file_offset.file_and_offset() {
                    let mut file = self.data_files[file.index()].file.readonly();
                    file.seek(SeekFrom::Start(offset))
                        .context("Seeking to message data")?;
                    let header: FileHeaderEntry = file.read_pod()?;
                    if header.has_been_transmitted != 0 {
                        continue;
                    }
                }
            }

            let seq = SequenceNr::from_index(index);
            self.mark_deleted_by_index(seq, update_head_tracker)?;
            let earliest = earliest.get_or_insert(seq);
            if seq < *earliest {
                *earliest = seq
            }
        }

        if earliest.is_some() {
            if self.compact_index_if_needed(false)? {
                return Ok(Some(SequenceNr::from_index(0)));
            }
        }

        Ok(earliest)
    }

    // If remaining_child_assignments is None, don't rewrite parents by adding new children to them
    fn do_write_message(
        files: &mut [DataFileInfo; 2],
        file_index: U1,
        msg: &MessageFrame<M>,
        children: &[MessageId],
        local: bool,
        remaining_child_assignments: Option<&mut RemainingChildAssignments>,
        message_bytes_written: &Counter,
    ) -> Result<MessageWriteReport>
    where
        M: Message,
    {
        for parent in msg.header.parents.iter() {
            if *parent >= msg.id() {
                bail!("parent must be temporally before child")
            }
        }

        let file_info = &mut files[file_index.index()];
        let file = &mut file_info.file;
        let start_pos = file.used_space() as u64;
        file.seek(SeekFrom::Start(start_pos))?;

        // Serialize the message and write to data store
        file.write_zeroes(MSG_HEADER_SIZE)?;
        let parent_overalloc = 2;
        let parent_count_alloc = msg.header.parents.len() + parent_overalloc;
        let child_count_alloc = parent_count_alloc.max(children.len());

        for parent in msg.header.parents.iter() {
            file.write_pod(parent)?;
        }
        file.write_zeroes(parent_overalloc * size_of::<MessageId>())?;

        for child in children.iter() {
            file.write_pod(child)?;
        }
        file.write_zeroes((child_count_alloc - children.len()) * size_of::<MessageId>())?;

        let pre_payload_pos = file.stream_position()?;
        msg.payload.serialize(&mut *file)?;
        let end_pos = file.stream_position()?;

        message_bytes_written.increment(end_pos - pre_payload_pos);

        let payload_size = end_pos - pre_payload_pos;

        let sha2_hash =
            file.with_bytes_at(pre_payload_pos as usize, payload_size as usize, |bytes| {
                sha2_message(msg.header.id, bytes)
            })?;

        let size_with_padding =
            (end_pos - start_pos).next_multiple_of(align_of::<FileHeaderEntry>() as u64);
        let padding = size_with_padding - (end_pos - start_pos);
        file.write_zeroes(padding as usize)?;

        let msg_total_size = size_with_padding;

        // Write data store header
        file.seek(SeekFrom::Start(start_pos))?;
        let header = FileHeaderEntry {
            header_checksum: [0; HASH_SIZE],
            message_sha2: sha2_hash,
            magic: MAGIC,
            message_id: msg.id(),
            payload_size,
            has_been_transmitted: (!local) as u8,
            padding1: 0,
            padding2: 0,
            num_parents: EmbVecHeader {
                num: msg.header.parents.len().try_into().unwrap(),
                num_allocated: parent_count_alloc.try_into().unwrap(),
            },
            num_children: EmbVecHeader {
                num: children.len().try_into().unwrap(),
                num_allocated: child_count_alloc.try_into().unwrap(),
            },
            padding4: 0,
        };
        debug_assert_eq!(header.total_size(), msg_total_size as usize);
        file.write_pod(&header)?;

        if let Some(remaining_child_assignments) = remaining_child_assignments {
            for parent in msg.header.parents.iter() {
                remaining_child_assignments
                    .parent2child
                    .entry(*parent)
                    .or_default()
                    .push(msg.id());
            }
        }

        Self::do_header_checksum(file, start_pos)?;

        if let Some(last_write) = file_info.outstanding.last_mut() {
            let gap = start_pos.checked_sub(last_write.end);
            if gap.map(|x| x < 1024).unwrap_or(false) {
                last_write.start = start_pos.min(last_write.start);
                last_write.end = end_pos.max(last_write.end);
            } else {
                file_info.outstanding.push(start_pos..end_pos);
            }
        } else {
            file_info.outstanding.push(start_pos..end_pos);
        }

        let file = &mut files[file_index.index()].file;
        file.seek(SeekFrom::Start(end_pos))?;

        Ok(MessageWriteReport {
            start_pos,
            total_size: msg_total_size,
        })
    }
    fn do_header_checksum(file: &mut FileAccessor, start_pos: u64) -> Result<()> {
        let payload_pos = {
            // Safety: The start_pos is valid, and there are no other views.
            let header: &FileHeaderEntry = unsafe { file.access_pod(start_pos as usize)? };
            header.offset_of_payload()
        };

        let start_of_data_for_header_checksum =
            start_pos as usize + offset_of!(FileHeaderEntry, payload_size);
        let size_of_data_for_header_checksum =
            payload_pos - offset_of!(FileHeaderEntry, payload_size);
        let header_checksum = file.with_bytes_at(
            start_of_data_for_header_checksum,
            size_of_data_for_header_checksum,
            sha2,
        )?;

        // Safety: The start_pos is valid, and there are no other views.
        unsafe {
            let header: &mut FileHeaderEntry = file.access_pod_mut(start_pos as usize)?;
            header.header_checksum = header_checksum;
        }
        Ok(())
    }

    /// This removes the affected parent/child pointers, not the actual messages being pointed to.
    pub(crate) fn remove_all_parents_and_some_children(
        &mut self,
        id: MessageId,
        children_to_remove: &[MessageId],
    ) -> Result<()>
    where
        M: Message,
    {
        let (_header, search_index) = Self::header_and_index_mut(&mut self.index_mmap)?;
        let Ok(index) = search_index.binary_search_by_key(&id, |x| x.message) else {
            return Ok(());
        };
        if let Some((file, offset)) = search_index[index].file_offset.file_and_offset() {
            let file = &mut self.data_files[file.index()].file;

            let mut parents = EmbVecAccessor::new_parents(&mut *file, offset);
            parents.clear()?;

            let mut children = EmbVecAccessor::new_children(&mut *file, offset)?;
            for child in children_to_remove {
                children.remove(*child)?;
            }
            Self::do_header_checksum(file, offset)?;
        }

        Ok(())
    }

    pub(crate) fn add_remove_parents_and_children(
        &mut self,
        id: MessageId,
        new_parents: &[MessageId],
        removed_parent: Option<MessageId>,
        new_children: &[MessageId],
        removed_child: Option<MessageId>,
    ) -> Result<()>
    where
        M: Message,
    {
        let (_index_header, search_index) = Self::header_and_index_mut(&mut self.index_mmap)?;
        let Ok(index) = search_index.binary_search_by_key(&id, |x| x.message) else {
            info!(
                "message not found, can't addremove parents/children {:?}",
                id
            );
            return Ok(());
        };
        if let Some((file, offset)) = search_index[index].file_offset.file_and_offset() {
            let file = &mut self.data_files[file.index()].file;
            // Safety: The offset is valid, and there are no other views.
            let header: &FileHeaderEntry = unsafe { file.access_pod(offset as usize)? };

            if header.num_parents.free() < new_parents.len()
                || header.num_children.free() < new_children.len()
            {
                let local = header.has_been_transmitted == 0;
                self.slow_add_parents_and_children(id, new_parents, new_children, local)?;
                return Ok(());
            }
            let mut parents = EmbVecAccessor::new_parents(&mut *file, offset);
            if let Some(removed_parent) = removed_parent {
                parents.remove(removed_parent)?;
            }
            parents.extend(new_parents.iter())?;

            let mut children = EmbVecAccessor::new_children(&mut *file, offset)?;
            if let Some(removed_child) = removed_child {
                children.remove(removed_child)?;
            }
            children.extend(new_children.iter())?;

            Self::do_header_checksum(file, offset)?;
        }

        Ok(())
    }

    /// Add parents and children to message by rewriting it completely.
    fn slow_add_parents_and_children(
        &mut self,
        id: MessageId,
        new_parents: &[MessageId],
        new_children: &[MessageId],
        local: bool,
    ) -> Result<()>
    where
        M: Message,
    {
        let Some((mut msg, mut children)) = self.read_message_and_children(id)? else {
            return Ok(());
        };

        let (header, search_index) = Self::header_and_index_mut(&mut self.index_mmap)?;
        for new_child in new_children {
            if !children.contains(new_child) {
                children.push(*new_child);
            }
        }

        for new_parent in new_parents {
            if !msg.header.parents.contains(new_parent) {
                msg.header.parents.push(*new_parent);
            }
        }

        let write_report = Self::do_write_message(
            &mut self.data_files,
            self.active_file,
            &msg,
            &children,
            local,
            None,
            &self.message_bytes_written,
        )?;

        if let Ok(index) = search_index.binary_search_by_key(&msg.header.id, |x| x.message) {
            let index_entry = &mut search_index[index];
            debug_assert!(!index_entry.file_offset.is_deleted());
            debug_assert!(index < header.entries as usize);
            Self::delete_msg_from_file(index_entry, &mut self.data_files)?;
            index_entry.file_offset = FileOffset::new(write_report.start_pos, self.active_file);
            Ok(())
        } else {
            bail!("Couldn't find message in index");
        }
    }

    /// Handle child assignments where the children couldn't fit in available space reserved for
    /// children. This means we copy the entire message
    fn handle_remaining_child_assignments(
        &mut self,
        remaining_child_assignments: RemainingChildAssignments,
    ) -> Result<()>
    where
        M: Message,
    {
        for (parent, new_children) in remaining_child_assignments.parent2child {
            self.add_remove_parents_and_children(parent, &[], None, &new_children, None)?;
        }
        Ok(())
    }

    fn find_valid_index_at_or_before(mut index: usize, index_entries: &[IndexEntry]) -> usize {
        loop {
            if index == 0 {
                // index 0 is _always_ valid
                return index;
            }
            if !index_entries[index].file_offset.is_deleted() {
                return index;
            }
            index -= 1;
        }
    }

    fn do_append_many_sorted<'a>(
        &mut self,
        messages: impl ExactSizeIterator<Item = &'a MessageFrame<M>>,
        mut message_inserted: impl FnMut(MessageId, &[MessageId] /*parents*/) -> Result<()>,
        local: bool,
    ) -> Result<Option<usize>>
    where
        M: Message + Debug + 'a,
    {
        #[cfg(any(debug_assertions, feature = "debug"))]
        {
            self.debug_verify_cutoff_index()?;
        }

        let mut prev = None;

        let mut messages = messages.inspect(|test| {
            trace!("Inspecting for insert: {:?}", test);

            if prev.is_none() {
                prev = Some(test.header.id);
            } else {
                #[cfg(debug_assertions)]
                #[allow(clippy::unnecessary_unwrap)]
                {
                    if test.header.id <= prev.unwrap() {
                        dbg!(test.header.id, prev);
                        assert!(test.header.id > prev.unwrap());
                    }
                }
            }
        });
        let len = messages.len();

        if len == 0 {
            return Ok(None);
        }
        let info = &mut self.data_files[self.active_file.index()];
        let initial_file_position = info.file.stream_position()?;

        let (index_header, mmap_index) =
            Self::header_and_index_mut_uninit(&mut self.index_mmap, len)?;

        debug_assert!(mmap_index[0..index_header.entries as usize]
            .iter()
            .is_sorted_by_key(|x| x.message));

        let cutoff_time = index_header.cutoff.before_time;

        #[cfg(debug_assertions)]
        {
            let mut new_cutoff = index_header.cutoff;
            new_cutoff.hash = CutoffHash::default();
            for msg in mmap_index[0..index_header.entries as usize]
                .iter()
                .filter(|x| !x.is_deleted())
            {
                new_cutoff.apply(msg.message, "debug");
            }
            assert_eq!(new_cutoff, index_header.cutoff);
        }

        #[cfg(debug_assertions)]
        Self::check_duplicates(&mmap_index[0..index_header.entries as usize]);

        let mut last_msg_id = None;
        let mut carry_buffer: VecDeque<IndexEntry> = VecDeque::with_capacity(len);

        let first_input_message: &MessageFrame<M> = messages.next().unwrap();

        let initial_index_entries = index_header.entries as usize;
        let (Ok(first_index) | Err(first_index)) = mmap_index[0..index_header.entries as usize]
            .binary_search_by_key(&first_input_message.id(), |x| x.message);

        /*
        NO! Don't do the below: We've decided that duplicates must not exist at all (for binary search,
        and general easier-to-reason-about
        // If there are duplicates (which can only exist for deleted items), make sure we
        // deterministically start at the first
        while first_index > 0 && mmap_index[first_index-1].message == mmap_index[first_index].message {
            first_index -= 1;
        };
        */

        let mut cur_index = first_index;
        let mut index_we_must_rewind_db_to = None;

        let mut cur_input_message = Some(first_input_message);

        let mut remaining_child_assignments = RemainingChildAssignments::default();
        let mut cutoff_index_min = None;
        let mut cutoff_index_max = None;
        let mut insert_count = 0;
        let mut dbg_prev_index: isize = -1;
        let mut update_cutoff = |index: usize, time: NoatunTime| {
            assert!(index as isize > dbg_prev_index);
            dbg_prev_index = index as isize;
            let before = time < cutoff_time;
            if before {
                cutoff_index_min = Some(index + 1);
            } else if cutoff_index_max.is_none() {
                cutoff_index_max = Some(index);
            }
        };

        loop {
            if carry_buffer.is_empty() && cur_input_message.is_none() {
                break;
            }

            let cur_index_entry = &mmap_index[cur_index];

            let present_id = if cur_index < index_header.entries as usize {
                Some(cur_index_entry.message)
            } else {
                None
            };

            let cur_index_entry = &mut mmap_index[cur_index];

            #[derive(Clone, Copy, Debug)]
            #[allow(clippy::enum_variant_names)]
            enum Cases {
                /// The next value that should be written to the output is the one that is already
                /// present. I.e, a no-op. If multiple match, this matches first.
                NextFromPresent,
                /// The next should be from 'carry' - i.e, we're compacting
                NextFromCarry,
                /// The next that should be written is the current input message
                NextFromInput,
            }

            let cases = [
                (
                    Cases::NextFromInput,
                    cur_input_message.as_ref().map(|x| x.id()),
                ),
                (
                    Cases::NextFromCarry,
                    carry_buffer.front().map(|x| x.message),
                ),
                (Cases::NextFromPresent, present_id),
            ];
            let (now_case, _now_message_id) = cases
                .iter()
                .filter_map(|(case, val)| val.map(|x| (*case, x)))
                .min_by_key(|x| x.1)
                .expect("There must always be some case present");

            match now_case {
                Cases::NextFromPresent => {
                    let input_message_id = cur_input_message.as_ref().map(|x| x.id());

                    update_cutoff(cur_index, cur_index_entry.message.timestamp());

                    if present_id == input_message_id {
                        // input message (added message) was already present in store
                        cur_input_message = messages.next();
                        cur_index += 1;
                        continue;
                    }
                    cur_index += 1;
                    continue;
                }
                Cases::NextFromCarry => {
                    let temp = carry_buffer.pop_front().unwrap();

                    debug_assert!(carry_buffer.iter().is_sorted_by_key(|x| x.message));

                    if cur_index < initial_index_entries && !cur_index_entry.is_deleted() {
                        let (Ok(insert_at) | Err(insert_at)) = carry_buffer
                            .binary_search_by_key(&cur_index_entry.message, |x| x.message);
                        // TODO(future): Use BTreeMap for carry_buffer?
                        carry_buffer.insert(insert_at, *cur_index_entry);
                        debug_assert!(carry_buffer.iter().is_sorted_by_key(|x| x.message));
                    }

                    if cur_index_entry.is_deleted() && cur_index < initial_index_entries {
                        index_header.holes -= 1;
                    }

                    *cur_index_entry = temp;

                    if cur_index_entry.is_deleted() {
                        index_header.holes += 1;
                    }

                    update_cutoff(cur_index, cur_index_entry.message.timestamp());

                    cur_index += 1;
                }
                Cases::NextFromInput => {
                    let msg = cur_input_message.take().unwrap();

                    let mut was_in_carry = false;
                    // We're going to pick an entry from the 'input'. However,
                    // the carry could contain the same message. This is a corner case,
                    // that we've chosen to handle here in the 'input' path.
                    // The code here handles multiple duplicates, but that situation should
                    // not be possible (anymore).
                    while let Some(temp) = carry_buffer.front() {
                        let temp = *temp;
                        if temp.message == msg.header.id {
                            if temp.is_deleted() {
                                carry_buffer.pop_front();
                            } else {
                                carry_buffer.pop_front();
                                if !was_in_carry {
                                    if cur_index < initial_index_entries
                                        && !cur_index_entry.is_deleted()
                                    {
                                        let (Ok(insert_at) | Err(insert_at)) = carry_buffer
                                            .binary_search_by_key(&cur_index_entry.message, |x| {
                                                x.message
                                            });
                                        // TODO(future): Use BTreeMap for carry_buffer?
                                        carry_buffer.insert(insert_at, *cur_index_entry);
                                        debug_assert!(carry_buffer
                                            .iter()
                                            .is_sorted_by_key(|x| x.message));
                                    }

                                    assert!(!temp.file_offset.is_deleted());
                                    if cur_index_entry.is_deleted()
                                        && cur_index < initial_index_entries
                                    {
                                        index_header.holes -= 1;
                                    }

                                    *cur_index_entry = temp;

                                    if cur_index_entry.is_deleted() {
                                        index_header.holes += 1;
                                    }

                                    update_cutoff(cur_index, cur_index_entry.message.timestamp());

                                    cur_input_message = messages.next();

                                    cur_index += 1;
                                }
                                was_in_carry = true;
                            }
                        } else {
                            break;
                        }
                    }

                    if was_in_carry {
                        continue;
                    }
                    if cur_index < index_header.entries as usize {
                        let cur_index_entry = &mut mmap_index[cur_index];
                        if !cur_index_entry.is_deleted() && cur_index_entry.message == msg.header.id
                        {
                            cur_input_message = messages.next();

                            cur_index += 1;
                            continue;
                        }
                    }

                    trace!(
                        "Inserting message {:?} at index {}",
                        msg.header.id,
                        cur_index
                    );
                    index_header.cutoff.report_add(msg.header.id);
                    index_header.prev_cutoff.report_add(msg.header.id);

                    insert_count += 1;
                    message_inserted(msg.id(), &msg.header.parents)?;
                    if index_we_must_rewind_db_to.is_none() {
                        index_we_must_rewind_db_to =
                            Some(Self::find_valid_index_at_or_before(cur_index, mmap_index));
                    }

                    if let Some(last_msg_id) = last_msg_id {
                        if last_msg_id >= msg.id() {
                            bail!("MessageId was not applied in correct order.");
                        }
                    }
                    last_msg_id = Some(msg.id());

                    let MessageWriteReport {
                        total_size,
                        start_pos,
                    } = Self::do_write_message(
                        &mut self.data_files,
                        self.active_file,
                        msg,
                        &[],
                        local,
                        Some(&mut remaining_child_assignments),
                        &self.message_bytes_written,
                    )?;

                    let cur_index_entry = &mut mmap_index[cur_index];
                    // Append to the index (compacting while doing so)
                    if cur_index < index_header.entries as usize
                        && !cur_index_entry.file_offset.is_deleted()
                        && cur_index_entry.message != msg.id()
                    {
                        carry_buffer.push_back(*cur_index_entry);
                        debug_assert!(carry_buffer.iter().is_sorted_by_key(|x| x.message));
                    }

                    if cur_index_entry.is_deleted() && cur_index < initial_index_entries {
                        index_header.holes -= 1;
                    }

                    *cur_index_entry = IndexEntry {
                        message: msg.id(),
                        file_offset: FileOffset::new(start_pos, self.active_file),
                        file_total_size: total_size,
                    };

                    debug_assert!(!cur_index_entry.file_offset.is_deleted());

                    update_cutoff(cur_index, cur_index_entry.message.timestamp());

                    cur_index += 1;

                    let full_msg_size = total_size;

                    let data_file_header = &mut index_header.data_files[self.active_file.index()];
                    data_file_header.bytes_used += full_msg_size;
                    data_file_header.nominal_size += full_msg_size;
                    self.data_fragmentation_gauges[self.active_file.index()]
                        .set(data_file_header.fragmentation());

                    cur_input_message = messages.next();
                }
            }
        }

        let info = &mut self.data_files[self.active_file.index()];
        let final_file_position = info.file.stream_position()?;
        if !self.filesystem_sync_disabled {
            info.file
                .sync_range(
                    initial_file_position as usize,
                    (final_file_position - initial_file_position) as usize,
                )
                .context("flush file to disk")?;
        }

        self.messages_written.increment(insert_count);

        let Ok(cur_index_u32) = cur_index.try_into() else {
            bail!("max message count exceeded - database full");
        };

        #[cfg(debug_assertions)]
        {
            let correctly_sorted = mmap_index[0..index_header.entries as usize]
                .iter()
                .is_sorted_by_key(|x| x.message);
            debug_assert!(correctly_sorted);
        }

        index_header.entries = index_header.entries.max(cur_index_u32);

        debug_assert!(mmap_index[0..index_header.entries as usize]
            .iter()
            .is_sorted_by_key(|x| x.message));

        #[cfg(debug_assertions)]
        {
            let mut new_cutoff = index_header.cutoff;
            new_cutoff.hash = CutoffHash::default();
            for msg in mmap_index[0..index_header.entries as usize]
                .iter()
                .filter(|x| !x.file_offset.is_deleted())
            {
                new_cutoff.apply(msg.message, "debug2");
            }
            assert_eq!(new_cutoff, index_header.cutoff);
        }

        self.handle_remaining_child_assignments(remaining_child_assignments)?;

        let (index_header, mmap_index) = Self::header_and_index_mut(&mut self.index_mmap)?;

        #[cfg(debug_assertions)]
        Self::validate_holes(index_header, mmap_index);
        #[cfg(not(debug_assertions))]
        {
            _ = mmap_index;
        }

        let mut cutoff_index_cand = self.cutoff_index;
        if let Some(max) = cutoff_index_max {
            cutoff_index_cand = cutoff_index_cand.min(max);
        }
        if let Some(min) = cutoff_index_min {
            cutoff_index_cand = cutoff_index_cand.max(min);
        }
        cutoff_index_cand = cutoff_index_cand.min(index_header.entries as usize);

        #[cfg(debug_assertions)]
        {
            let correct_cutoff_index = Self::calculate_cutoff_index(index_header, mmap_index);

            if cutoff_index_cand != correct_cutoff_index {
                dbg!(
                    cutoff_index_min,
                    cutoff_index_max,
                    cutoff_index_cand,
                    correct_cutoff_index
                );
            }
            assert_eq!(cutoff_index_cand, correct_cutoff_index);
        }

        self.cutoff_index = cutoff_index_cand;

        #[cfg(any(debug_assertions, feature = "debug"))]
        {
            self.debug_verify_cutoff_index()?;
        }

        self.update_index_metrics()?;

        trace!(
            "Finished do_append_many_sorted, now with {} elements",
            cur_index
        );

        Ok(index_we_must_rewind_db_to)
    }
    #[cfg(debug_assertions)]
    fn check_duplicates(mmap_index: &[IndexEntry]) {
        for (index, (window0, window1)) in mmap_index.iter().tuple_windows().enumerate() {
            #[allow(clippy::nonminimal_bool)]
            if !(window0.message < window1.message) {
                panic!(
                    "Failed condition: {:?} < {:?} at {}",
                    window0.message, window1.message, index
                );
            }
        }
    }

    /// Compact all files to the given maximum degree of fragmentation.
    /// NOTE! Never call this method with a value smaller than 25. This can lead
    /// to abysmal performance. If disk space permits, consider allowing up to 50% fragmentation.
    fn compact_to_target(&mut self, tolerated_fragmentation_percent: u32) -> Result<()>
    where
        M: Message,
    {
        self.do_compact_data(|wasted_space, total_space| {
            let frag_perc = (100 * wasted_space) / total_space;
            frag_perc > tolerated_fragmentation_percent as u64
        })
    }

    /// Compact all files
    pub(crate) fn compact(&mut self) -> Result<()>
    where
        M: Message,
    {
        self.compact_to_target(25)
    }

    /// Active file is second (dst)
    fn get_src_dst<T>(entries: &mut [T; 2], active: U1) -> (&mut T, &mut T) {
        let data_files = entries.split_at_mut(1);
        let (active_data_file, passive_data_file) = if active.index() == 0 {
            let active = &mut data_files.0[0];
            let passive = &mut data_files.1[0];
            (active, passive)
        } else {
            let active = &mut data_files.1[0];
            let passive = &mut data_files.0[0];
            (active, passive)
        };
        let (src_file_entry, dst_file_entry) = (passive_data_file, active_data_file);
        (src_file_entry, dst_file_entry)
    }

    fn do_compact_data(
        &mut self,
        mut need_compact: impl FnMut(/*wasted bytes:*/ u64, /*total bytes: */ u64) -> bool,
    ) -> Result<()> {
        let completed_one_compaction = self.do_compact_data_impl(&mut need_compact)?;
        if completed_one_compaction {
            self.do_compact_data_impl(need_compact)?;
        }

        Ok(())
    }
    fn update_compaction_stats(&mut self) -> Result<()> {
        let (header, _index) = Self::header_and_index_mut(&mut self.index_mmap)?;
        let (file1, file0) = Self::get_src_dst(&mut header.data_files, U1::ZERO);

        self.data_fragmentation_gauges[0].set(file0.fragmentation() as f64);
        self.data_fragmentation_gauges[1].set(file1.fragmentation() as f64);
        Ok(())
    }
    /// Returns true if 'active' has been swapped
    fn do_compact_data_impl(
        &mut self,
        mut need_compact: impl FnMut(/*wasted bytes:*/ u64, /*total bytes:*/ u64) -> bool,
    ) -> Result<bool> {
        let (header, index) = Self::header_and_index_mut(&mut self.index_mmap)?;

        let active_file = self.active_file;

        //dst is 'active'
        let (src_file_info, dst_file_info) = Self::get_src_dst(&mut self.data_files, active_file);
        let (src_file_entry, dst_file_entry) =
            Self::get_src_dst(&mut header.data_files, active_file);

        let mut have_compacted = false;
        loop {
            let src_wasted = src_file_entry.nominal_size - src_file_entry.bytes_used;
            let dst_wasted = dst_file_entry.nominal_size - dst_file_entry.bytes_used;
            let src_tot = src_file_entry.nominal_size;
            let dst_tot = dst_file_entry.nominal_size;

            let src_position = src_file_entry.compaction_pointer;
            if src_file_entry.compaction_pointer >= src_file_entry.nominal_size {
                if !self.filesystem_sync_disabled {
                    dst_file_info.file.sync_all()?;
                }

                src_file_entry.nominal_size = 0;
                src_file_entry.bytes_used = 0;
                src_file_entry.compaction_pointer = 0;
                if !self.filesystem_sync_disabled {
                    src_file_info.file.sync_all()?;
                }
                src_file_info.file.seek_to(0)?;
                src_file_info.file.set_used_space(0);

                // Don't actually swap if current active isn't even fragmented at all,
                // or if target is reached
                if dst_file_entry.bytes_used != dst_file_entry.nominal_size
                    && need_compact(src_wasted + dst_wasted, src_tot + dst_tot)
                {
                    self.active_file.swap();
                    if have_compacted {
                        self.update_compaction_stats()?;
                    }
                    return Ok(true);
                }
                if have_compacted {
                    self.update_compaction_stats()?;
                }
                return Ok(false);
            }

            if !need_compact(src_wasted + dst_wasted, src_tot + dst_tot) {
                if have_compacted {
                    self.update_compaction_stats()?;
                }
                return Ok(false);
            }

            src_file_info.file.seek(SeekFrom::Start(src_position))?;
            let header = src_file_info.file.read_pod::<FileHeaderEntry>()?;
            let full_size = header.total_size() as u64;

            let new_offset = dst_file_entry.nominal_size;
            dst_file_info
                .file
                .seek(SeekFrom::Start(dst_file_entry.nominal_size))?;

            if header.is_deleted() {
                src_file_entry.compaction_pointer += full_size;
                continue;
            }

            if let Ok(msg_in_index) = index.binary_search_by_key(&header.message_id, |x| x.message)
            {
                let index_entry = &mut index[msg_in_index];
                if index_entry.file_offset.is_deleted() {
                    src_file_entry.compaction_pointer += full_size;
                    continue;
                }
                index_entry.file_offset = FileOffset::new(new_offset, active_file);
            }

            src_file_info.file.seek(SeekFrom::Start(src_position))?;
            src_file_info
                .file
                .copy_to(full_size as usize, &mut dst_file_info.file)?;
            src_file_entry.compaction_pointer += full_size;
            src_file_entry.bytes_used = src_file_entry.bytes_used.saturating_sub(full_size);
            dst_file_entry.bytes_used += full_size;
            dst_file_entry.nominal_size += full_size;
            have_compacted = true;
        }
    }

    pub fn new<D: Disk>(
        d: &mut D,
        target: &Target,
        min_file_size: usize,
        max_file_size: usize,
        auto_compact: bool,
    ) -> Result<OnDiskMessageStore<M>> {
        const {
            if size_of::<usize>() < size_of::<u32>() {
                panic!(
                    "This platform has a usize that is smaller than 4 bytes. This is not supported"
                );
            }
        }

        let mut db_existed = false;

        let data_files: [Result<DataFileInfo>; 2] = [0, 1].map(|file_number| {
            let file_name = format!("data{file_number}");
            let (data_file, existed) = d
                .open_file(
                    target,
                    &file_name,
                    min_file_size,
                    max_file_size,
                    &format!("store{file_number}"),
                    &format!("Message data store #{file_number}"),
                )
                .context("Opening data-file")?;

            db_existed = existed;

            data_file.try_lock_exclusive().with_context(|| {
                format!(
                    "While obtaining file lock on data file: {:?}",
                    target.path().join(&file_name)
                )
            })?;

            Ok(DataFileInfo {
                file: data_file,
                file_number: U1::from_index(file_number),
                outstanding: vec![],
            })
        });

        let data_files: [DataFileInfo; 2] = data_files
            .into_iter()
            .collect::<Result<Vec<DataFileInfo>>>()?
            .try_into()
            .unwrap_or_else(|_| unreachable!());

        let (mut index_file, _existed) = d
            .open_file(
                target,
                "index",
                size_of::<StoreHeader>().max(min_file_size),
                max_file_size,
                "index_file",
                "primary message index",
            )
            .context("Opening index-file")?;
        index_file
            .try_lock_exclusive()
            .context("While obtaining lock on index-file")?;

        let index = Self::header_mut(&mut index_file)?;

        // The active file is the one with the least percentage of wasted bytes
        let active = index
            .data_files
            .iter()
            .enumerate()
            .min_by_key(|(_index, data_file)| data_file.fragmentation())
            .unwrap()
            .0;

        let (header, index) = Self::header_and_index_mut(&mut index_file)?;
        let cutoff_index = Self::calculate_cutoff_index(header, index);

        let index_compaction = counter!("index_compaction_count");
        describe_counter!(
            "index_compaction_count",
            Unit::Count,
            "Number of compactions of the message index"
        );

        let index_size = gauge!("index_size");
        describe_gauge!(
            "index_size",
            Unit::Count,
            "Total number of entries in the message index"
        );

        let index_holes = gauge!("index_holes");
        describe_gauge!(
            "index_holes",
            Unit::Count,
            "Number of unused entries in the message index"
        );

        let messages_written = counter!("messages_written");
        describe_counter!(
            "messages_written",
            Unit::Count,
            "Total number of messages written to database"
        );

        let message_bytes_written = counter!("message_bytes_written");
        describe_counter!(
            "message_bytes_written",
            Unit::Bytes,
            "Total number of payload bytes written to database"
        );

        let frag0 = gauge!("data_frag0");
        describe_gauge!(
            "data_frag0",
            Unit::Percent,
            "Fragmentation of data file 0, in percent"
        );
        let frag1 = gauge!("data_frag1");
        describe_gauge!(
            "data_frag1",
            Unit::Percent,
            "Fragmentation of data file 1, in percent"
        );

        let data_fragmentation = [frag0, frag1];

        let mut this = Self {
            index_mmap: index_file,
            data_files,
            active_file: U1::from_index(active),
            phantom: Default::default(),
            loaded_existing: db_existed,
            filesystem_sync_disabled: false,
            auto_compact_enabled: auto_compact,
            cutoff_index,
            index_compaction,
            index_size,
            index_holes,
            messages_written,
            message_bytes_written,
            data_fragmentation_gauges: data_fragmentation,
        };

        this.update_index_metrics()?;
        this.update_compaction_stats()?;

        Ok(this)
    }

    pub(crate) fn disable_filesystem_sync(&mut self) {
        self.filesystem_sync_disabled = true;
    }

    /// The size of the store index.
    ///
    /// Each message in the store has a SequenceNr. Messages can be accessed very quickly
    /// by their SequenceNr. This method returns the number of sequence numbers in the store.
    /// This will be one larger than the highest existing sequence number.
    #[cfg(test)]
    fn index_count(&self) -> Result<usize> {
        Ok(self.header()?.entries as usize)
    }

    pub fn count_messages(&self) -> usize {
        let Ok((_header, index)) = self.header_and_index() else {
            return 0;
        };

        index.iter().filter(|x| !x.file_offset.is_deleted()).count()
    }

    pub fn current_cutoff_hash(&self) -> Result<CutOffHashPos> {
        let (header, _index) = self.header_and_index()?;
        Ok(header.cutoff)
    }
    pub fn prev_cutoff_hash(&self) -> Result<CutOffHashPos> {
        let (header, _index) = self.header_and_index()?;
        Ok(header.prev_cutoff)
    }

    /// Messages timestamped _earlier_ are in the cutoff-region
    /// (included in the cutoff hash). Messages with the exact timestamp of cutoff
    /// are considered to be on the right (more recent) side of the cutoff.
    pub fn current_cutoff_time(&self) -> Result<NoatunTime> {
        let (header, _index) = self.header_and_index()?;
        Ok(header.cutoff.before_time.to_noatun_time())
    }
    /// Warning! The returned slice may contain deleteds messages!
    pub(crate) fn get_messages_slice(&self) -> Result<&[IndexEntry]> {
        let (header, index) = self.header_and_index()?;
        Ok(&index[0..header.entries as usize])
    }

    pub(crate) fn get_messages_at_or_after(
        &self,
        message: MessageId,
        count: usize,
    ) -> Result<Vec<MessageId>> {
        let (_header, search_index) = self.header_and_index()?;
        let (Ok(index) | Err(index)) = search_index.binary_search_by_key(&message, |x| x.message);
        let result = search_index[index..]
            .iter()
            .filter(|x| !x.file_offset.is_deleted())
            .map(|x| x.message)
            .take(count)
            .collect();
        Ok(result)
    }

    pub fn is_acceptable_cutoff_hash(
        &self,
        hash: CutOffHashPos,
        cutoff_config: &CutOffConfig,
        now: NoatunTime,
    ) -> Result<Acceptability> {
        let (header, _index) = self.header_and_index()?;
        if header.prev_cutoff == hash {
            return Ok(Acceptability::Previous);
        }
        Ok(header
            .cutoff
            .is_acceptable_cutoff_hash(hash, cutoff_config, now))
    }
}

#[cfg(test)]
mod tests {
    use crate::simple_metrics::SimpleMetricsRecorder;
    use indexmap::IndexSet;
    use insta::assert_snapshot;
    use std::collections::BTreeSet;

    use crate::disk_abstraction::{InMemoryDisk, StandardDisk};
    use crate::message_store::{FileOffset, OnDiskMessageStore, U1};
    use crate::{
        DummyUnitObject, Message, MessageFrame, MessageId, NoatunMessageSerializer, NoatunTime,
        Target,
    };
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use std::io::{Cursor, Read, Write};

    use crate::cutoff::CutOffConfig;
    use crate::update_head_tracker::UpdateHeadTracker;
    use rand::rngs::SmallRng;
    use rand::Rng;
    use std::pin::Pin;
    use std::time::Instant;

    #[derive(Debug)]
    struct OnDiskMessage {
        id: u64,
        seq: u64,
        data: Vec<u8>,
    }

    struct OnDiskMessageSerializer;

    impl NoatunMessageSerializer<OnDiskMessage> for OnDiskMessageSerializer {
        fn deserialize(buf: &[u8]) -> anyhow::Result<OnDiskMessage>
        where
            Self: Sized,
        {
            let mut cursor = Cursor::new(buf);
            let id = cursor.read_u64::<LittleEndian>()?;
            let seq = cursor.read_u64::<LittleEndian>()?;
            let data_len = cursor.read_u64::<LittleEndian>()? as usize;
            let mut data = vec![0; data_len];
            cursor.read_exact(&mut data)?;
            Ok(OnDiskMessage { id, seq, data })
        }

        fn serialize<W: Write>(tself: &OnDiskMessage, mut writer: W) -> anyhow::Result<()> {
            writer.write_u64::<LittleEndian>(tself.id)?;
            writer.write_u64::<LittleEndian>(tself.seq)?;
            writer.write_u64::<LittleEndian>(tself.data.len() as u64)?;
            writer.write_all(&tself.data)?;
            Ok(())
        }
    }

    impl Message for OnDiskMessage {
        type Root = DummyUnitObject;
        type Serializer = OnDiskMessageSerializer;

        fn apply(&self, _time: MessageId, _root: Pin<&mut Self::Root>) {
            unimplemented!()
        }
    }

    #[test]
    fn add_and_recover_messages() {
        let mut store = OnDiskMessageStore::new(
            &mut StandardDisk,
            &Target::CreateNewOrOverwrite("test/add_and_recover_messages.bin".into()),
            0,
            10000,
            false,
        )
        .unwrap();

        const COUNT: usize = 5;

        let msgs: Vec<_> = (0..COUNT)
            .map(|i| {
                MessageFrame::new(
                    MessageId::new_debug(i as u32),
                    vec![],
                    OnDiskMessage {
                        id: i as u64,
                        seq: i as u64,
                        data: vec![42u8; 4],
                    },
                )
            })
            .collect();

        store
            .append_many_sorted(msgs.iter(), |_, _| Ok(()), true)
            .unwrap();

        {
            let _header =
                OnDiskMessageStore::<OnDiskMessage>::header_mut(&mut store.index_mmap).unwrap();
        }
        store
            .recover(|_, _| Ok(()), NoatunTime::EPOCH, &CutOffConfig::default())
            .unwrap();

        let msg = store
            .read_message(MessageId::new_debug(2))
            .expect("io should work")
            .expect("a message should be found");
        assert_eq!(msg.payload.id, 2);
    }
    #[test]
    fn add_and_read_single_message() {
        let mut store = OnDiskMessageStore::new(
            &mut StandardDisk,
            &Target::CreateNewOrOverwrite("test/add_and_read_messages1.bin".into()),
            0,
            5_000_000_000,
            false,
        )
        .unwrap();

        let msg = MessageFrame::new(
            MessageId::new_debug(0u32),
            vec![],
            OnDiskMessage {
                id: 0u64,
                seq: 0u64,
                data: vec![42u8; 4],
            },
        );

        store
            .append_many_sorted(std::iter::once(&msg), |_, _| Ok(()), true)
            .unwrap();

        let msg = store
            .read_message(MessageId::new_debug(0))
            .unwrap()
            .unwrap();
        assert_eq!(msg.payload.id, 0);
        assert_eq!(msg.payload.data, [42, 42, 42, 42]);
    }
    #[test]
    fn add_and_read_messages() {
        let mut store = OnDiskMessageStore::new(
            &mut StandardDisk,
            &Target::CreateNewOrOverwrite("test/add_and_read_messages2.bin".into()),
            0,
            5_000_000_000,
            false,
        )
        .unwrap();

        #[cfg(debug_assertions)]
        const COUNT: usize = 10usize;
        #[cfg(not(debug_assertions))]
        const COUNT: usize = 1000usize;

        let msgs: Vec<_> = (0..COUNT)
            .map(|i| {
                MessageFrame::new(
                    MessageId::new_debug(i as u32),
                    if i != 0 {
                        vec![MessageId::new_debug(0)]
                    } else {
                        vec![]
                    },
                    OnDiskMessage {
                        id: i as u64,
                        seq: i as u64,
                        data: vec![42u8; 4],
                    },
                )
            })
            .collect();

        store
            .append_many_sorted(msgs.iter(), |_, _| Ok(()), true)
            .unwrap();

        let mut all_children_of_0 = vec![];
        for i in 1..COUNT {
            all_children_of_0.push(MessageId::new_debug(i as u32))
        }
        assert_eq!(
            store.get_children_of(MessageId::new_debug(0)).unwrap(),
            all_children_of_0
        );

        let msg = store
            .read_message(MessageId::new_debug(2))
            .unwrap()
            .unwrap();
        assert_eq!(msg.payload.id, 2);
        assert_eq!(msg.payload.data, [42, 42, 42, 42]);
    }

    #[test]
    fn add_and_read_messages_twice() {
        let test_recorder = SimpleMetricsRecorder::default();
        let _guard = test_recorder.register_local();

        let mut store = OnDiskMessageStore::new(
            &mut StandardDisk,
            &Target::CreateNewOrOverwrite("test/add_and_read_messages_twice3.bin".into()),
            0,
            10000,
            false,
        )
        .unwrap();

        const COUNT: usize = 5;

        let msgs: Vec<_> = (0..COUNT)
            .map(|i| {
                MessageFrame::new(
                    MessageId::new_debug(2 * i as u32),
                    vec![],
                    OnDiskMessage {
                        id: 2 * i as u64,
                        seq: 2 * i as u64,
                        data: vec![42u8; 4],
                    },
                )
            })
            .collect();

        store
            .append_many_sorted(msgs.iter(), |_, _| Ok(()), true)
            .unwrap();
        let msgs: Vec<_> = (0..COUNT)
            .map(|i| {
                MessageFrame::new(
                    MessageId::new_debug(2 * i as u32 + 1),
                    vec![],
                    OnDiskMessage {
                        id: 2 * i as u64 + 1,
                        seq: 2 * i as u64 + 1,
                        data: vec![43u8; 4],
                    },
                )
            })
            .collect();

        store
            .append_many_sorted(msgs.iter(), |_, _| Ok(()), true)
            .unwrap();

        let msg = store
            .read_message(MessageId::new_debug(2))
            .unwrap()
            .unwrap();
        assert_eq!(msg.payload.id, 2);
        assert_eq!(msg.payload.data, [42, 42, 42, 42]);
        assert_snapshot!(test_recorder.get_metrics_text());
    }

    #[test]
    fn add_and_delete_messages() {
        let test_recorder = SimpleMetricsRecorder::default();
        let _guard = test_recorder.register_local();
        let target = Target::CreateNewOrOverwrite("test/add_and_delete_messages4.bin".into());
        let mut store =
            OnDiskMessageStore::new(&mut StandardDisk, &target, 0, 10000, false).unwrap();

        let mut head_tracker = UpdateHeadTracker::new(&mut StandardDisk, &target).unwrap();
        const COUNT: usize = 6;

        let msgs: Vec<_> = (0..COUNT)
            .map(|i| {
                MessageFrame::new(
                    MessageId::new_debug(i as u32),
                    vec![],
                    OnDiskMessage {
                        id: i as u64,
                        seq: i as u64,
                        data: vec![42u8; 4],
                    },
                )
            })
            .collect();

        store
            .append_many_sorted(msgs.iter(), |_, _| Ok(()), true)
            .unwrap();

        store
            .delete_many(
                [
                    MessageId::new_debug(0),
                    MessageId::new_debug(1),
                    MessageId::new_debug(2),
                    MessageId::new_debug(3),
                    MessageId::new_debug(4),
                    MessageId::new_debug(5),
                ]
                .into_iter(),
                &mut head_tracker,
                true,
            )
            .unwrap();

        assert_eq!(
            store
                .query_greater(MessageId::new_debug(0))
                .unwrap()
                .count(),
            0
        );

        assert_snapshot!(test_recorder.get_metrics_text());

        store.compact_to_target(0).unwrap();
    }

    #[test]
    fn add_and_delete_some_messages() {
        let test_recorder = SimpleMetricsRecorder::default();
        let _guard = test_recorder.register_local();
        let target = Target::CreateNewOrOverwrite("test/add_and_delete_messages5.bin".into());
        let mut store =
            OnDiskMessageStore::new(&mut StandardDisk, &target, 0, 200000, false).unwrap();

        let mut head_tracker = UpdateHeadTracker::new(&mut StandardDisk, &target).unwrap();
        const COUNT: usize = 6;

        let msgs: Vec<_> = (0..COUNT)
            .map(|i| {
                MessageFrame::new(
                    MessageId::new_debug(i as u32),
                    vec![],
                    OnDiskMessage {
                        id: i as u64,
                        seq: i as u64,
                        data: vec![42u8; 4],
                    },
                )
            })
            .collect();

        store
            .append_many_sorted(msgs.iter(), |_, _| Ok(()), true)
            .unwrap();

        store
            .delete_many(
                [
                    MessageId::new_debug(0),
                    MessageId::new_debug(1),
                    MessageId::new_debug(2),
                    MessageId::new_debug(3),
                    MessageId::new_debug(5),
                ]
                .into_iter(),
                &mut head_tracker,
                true,
            )
            .unwrap();

        let msg = store.get_all_messages().unwrap().next().unwrap();
        assert_eq!(msg.payload.seq, 4);

        assert_eq!(
            store
                .query_greater(MessageId::new_debug(0))
                .unwrap()
                .count(),
            1
        );

        assert_snapshot!(test_recorder.get_metrics_text());

        store.compact_to_target(0).unwrap();
    }
    #[test]
    fn test_compact() {
        let target = Target::CreateNewOrOverwrite("test/test_create_disk_store.bin".into());
        let mut store =
            OnDiskMessageStore::new(&mut InMemoryDisk::default(), &target, 0, 100_000_000, false)
                .unwrap();

        const COUNT: usize = 1_000;
        let msgs: Vec<_> = (0..COUNT)
            .map(|i| {
                MessageFrame::new(
                    MessageId::new_debug(i as u32),
                    vec![],
                    OnDiskMessage {
                        id: (1_000_000 + i) as u64,
                        seq: i as u64,
                        data: vec![42u8; 1024],
                    },
                )
            })
            .collect();

        store
            .append_many_sorted(msgs.iter(), |_, _| Ok(()), true)
            .unwrap();
        let mut tracker = UpdateHeadTracker::new(&mut InMemoryDisk::default(), &target).unwrap();

        store
            .delete_many(
                (0..1000).filter(|x| x % 7 != 0).map(MessageId::new_debug),
                &mut tracker,
                false,
            )
            .unwrap();

        assert_eq!(store.count_messages(), 143);
        assert_eq!(store.index_count().unwrap(), 143);
        for msg in store.get_all_messages().unwrap() {
            let val = msg.header.id.debug_value();
            assert_eq!(val % 7, 0);
        }
    }
    #[test]
    fn test_create_disk_store() {
        let mut store = OnDiskMessageStore::new(
            &mut StandardDisk,
            &Target::CreateNewOrOverwrite("test/test_create_disk_store.bin".into()),
            0,
            2000000,
            false,
        )
        .unwrap();

        const COUNT: usize = 1_000;

        let init = Instant::now();
        let msgs: Vec<_> = (0..COUNT)
            .map(|i| {
                MessageFrame::new(
                    MessageId::new_debug(i as u32),
                    vec![],
                    OnDiskMessage {
                        id: (1_000_000 + i) as u64,
                        seq: i as u64,
                        data: vec![42u8; 1024],
                    },
                )
            })
            .collect();

        store
            .append_many_sorted(msgs.iter(), |_, _| Ok(()), true)
            .unwrap();

        println!("Init: {:?}", init.elapsed());
        let mut count = 0;
        let mut sum = 0;
        let start = Instant::now();
        for msg in store
            .query_greater(MessageId::new_debug(1_999_900))
            .unwrap()
        {
            sum += msg.message.data[0] as u64;
            count += 1;
        }
        println!("Time to iterate: {:?}", start.elapsed());
        println!("Count: {count} (sum: {sum})");
    }
    #[test]
    fn test_create_disk_store_in_memory() {
        let mut store = OnDiskMessageStore::new(
            &mut InMemoryDisk::default(),
            &Target::CreateNewOrOverwrite("test/test_create_disk_store.bin".into()),
            0,
            10000,
            false,
        )
        .unwrap();

        const COUNT: usize = 1;

        let init = Instant::now();
        let msgs: Vec<_> = (0..COUNT)
            .map(|i| {
                MessageFrame::new(
                    MessageId::new_debug(i as u32),
                    vec![],
                    OnDiskMessage {
                        id: (1_000_000 + i) as u64,
                        seq: i as u64,
                        data: vec![42u8; 4],
                    },
                )
            })
            .collect();

        store
            .append_many_sorted(msgs.iter(), |_, _| Ok(()), true)
            .unwrap();

        println!("Init: {:?}", init.elapsed());
        let mut count = 0;
        let mut sum = 0;
        let start = Instant::now();
        for msg in store
            .query_greater(MessageId::new_debug(1_000_000))
            .unwrap()
        {
            sum += msg.message.data[0] as u64;
            count += 1;
        }
        println!("Time to iterate: {:?}", start.elapsed());
        println!("Count: {count} (sum: {sum})");
    }

    #[test]
    fn test_file_offset() {
        fn roundtrip(off: FileOffset) {
            let roundtripped = FileOffset::from_bits(off.bits());
            assert_eq!(off, roundtripped);
        }

        roundtrip(FileOffset::new(0, U1::ONE));
        roundtrip(FileOffset::new(1, U1::ZERO));
        roundtrip(FileOffset::new(2, U1::ONE));
        roundtrip(FileOffset::new(2, U1::ZERO));
        roundtrip(FileOffset::deleted());
        roundtrip(FileOffset::new(u64::MAX / 2 - 1, U1::ONE));
        roundtrip(FileOffset::new(u64::MAX / 2 - 1, U1::ZERO));
    }

    #[test]
    fn test_dedup_slice() {
        use super::dedup_slice;
        fn dedup(input: &[u32]) -> Vec<u32> {
            let mut temp = input.to_vec();
            let new_len = dedup_slice(&mut temp);
            temp.truncate(new_len);
            temp
        }

        assert_eq!(dedup(&[]), vec![0u32; 0]);
        assert_eq!(dedup(&[1]), &[1]);
        assert_eq!(dedup(&[1, 2]), &[1, 2]);
        assert_eq!(dedup(&[1, 1]), &[1]);
        assert_eq!(dedup(&[1, 1, 1]), &[1]);
        assert_eq!(dedup(&[1, 2, 2]), &[1, 2]);
        assert_eq!(dedup(&[1, 2, 2, 3]), &[1, 2, 3]);
        assert_eq!(dedup(&[1, 2, 2, 3, 3, 3]), &[1, 2, 3]);

        assert_eq!(dedup(&[1, 2, 2, 3, 3, 3, 5]), &[1, 2, 3, 5]);
        assert_eq!(dedup(&[1, 1, 1, 2, 2, 3, 3, 3, 5]), &[1, 2, 3, 5]);
    }

    #[test]
    fn add_and_delete_many_fuzz_all() {
        #[cfg(debug_assertions)]
        const COUNT: u32 = 3000;
        #[cfg(not(debug_assertions))]
        const COUNT: u32 = 20000;

        for seed in 0..COUNT {
            println!("===== add_and_delete_many_fuzz seed: {seed} =======");
            add_and_delete_fuzz_seed(seed);
        }
    }
    #[test]
    fn add_and_delete_many_fuzz345() {
        {
            let seed = 345;
            println!("===== add_and_delete_many_fuzz seed: {seed} =======");
            add_and_delete_fuzz_seed(seed);
        }
    }
    fn add_and_delete_fuzz_seed(seed: u32) {
        use rand::SeedableRng;
        let mut rng = SmallRng::seed_from_u64(seed as u64);

        let target = Target::CreateNewOrOverwrite("test/test_create_disk_store.bin".into());
        let mut store =
            OnDiskMessageStore::new(&mut InMemoryDisk::default(), &target, 0, 10000, false)
                .unwrap();

        const COUNT: u32 = 20;

        let mut head_tracker =
            UpdateHeadTracker::new(&mut InMemoryDisk::default(), &target).unwrap();

        let mut total_created_messages = (0..COUNT).collect::<Vec<_>>();

        let mut facit: BTreeSet<MessageId> = BTreeSet::new();

        for _ in 0..20 {
            let mut to_insert = Vec::new();
            let big = rng.gen_bool(0.5);
            let mut actually_inserted = IndexSet::new();
            total_created_messages.retain(|x| {
                if rng.gen_range(0..if big { 2 } else { 6 }) == 0 {
                    let new = MessageId::new_debug(*x);
                    to_insert.push(MessageFrame::new(
                        new,
                        vec![],
                        OnDiskMessage {
                            id: *x as u64,
                            seq: *x as u64,
                            data: vec![42u8; 4],
                        },
                    ));
                    if facit.insert(new) {
                        actually_inserted.insert(new);
                    }
                    if rng.gen_range(0..3) == 0 {
                        return true;
                    }
                    false
                } else {
                    true
                }
            });

            let mut was_inserted = IndexSet::new();

            store
                .append_many_sorted(
                    to_insert.iter(),
                    |inserted, _| {
                        was_inserted.insert(inserted);
                        Ok(())
                    },
                    true,
                )
                .unwrap();
            assert_eq!(actually_inserted, was_inserted);

            let mut to_delete = Vec::new();
            let all_msgs = store.get_all_message_ids().unwrap();
            let facit_msgs = facit.iter().copied().collect::<Vec<_>>();
            if facit_msgs != all_msgs {
                println!("Facit: {facit_msgs:#?}");
                println!("Actual: {all_msgs:#?}");
            }
            assert_eq!(
                all_msgs, facit_msgs,
                "msgs in db must be correct after append_many_sorted"
            );

            for msg in all_msgs {
                if rng.gen_range(0..3) == 0 {
                    to_delete.push(msg);
                    facit.remove(&msg);
                }
            }

            store
                .delete_many(to_delete.into_iter(), &mut head_tracker, true)
                .unwrap();

            let all_msgs = store.get_all_message_ids().unwrap();
            let facit_msgs = facit.iter().copied().collect::<Vec<_>>();
            if facit_msgs != all_msgs {
                println!("Facit: {facit_msgs:#?}");
                println!("Actual: {all_msgs:#?}");
            }
            assert_eq!(all_msgs, facit_msgs);
        }
    }
}
