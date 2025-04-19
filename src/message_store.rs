use crate::cutoff::{Acceptability, CutOffConfig, CutOffHashPos, CutOffTime, CutoffHash};
use crate::disk_abstraction::Disk;
use crate::disk_access::FileAccessor;
use crate::sequence_nr::SequenceNr;
use crate::sha2_helper::{sha2, sha2_message};
use crate::update_head_tracker::UpdateHeadTracker;
use crate::{Message, MessageHeader, MessageId, MessagePayload, NoatunTime, Target};
use anyhow::{anyhow, bail, Context, Result};
use bytemuck::{Pod, Zeroable};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem::offset_of;
use tracing::{info, trace, warn};

#[derive(Debug, Clone, Copy, Pod, Zeroable, PartialEq, Eq)]
#[repr(transparent)]
pub struct FileOffset(u64);

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

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
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
        // TODO: Optimize this!
        let l = self.len()?;
        let mut result = Vec::with_capacity(l);
        for i in 0..l {
            result.push(self.get(i)?);
        }

        Ok(result)
    }
    pub fn contains(&mut self, id: MessageId) -> Result<bool> {
        // TODO: Verify correctness, and Optimize this? Or remove it?
        let l = self.len()?;
        for i in 0..l {
            if self.get(i)? == id {
                return Ok(true);
            }
        }

        Ok(false)
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
        self.handle
            .seek(SeekFrom::Start(self.len_offset as u64))?;
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

    pub fn retain(&mut self, mut f: impl FnMut(MessageId) -> bool) -> Result<()> where T: Write
    {
        let mut i = 0;
        let mut len = self.len()?;
        while i < len {
            let item = self.get(i)?;
            if !f(item) {
                if i + 1 != len {
                    let last = self.get(len-1)?;
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
        Ok(self.handle.read_pod()?)
    }
    pub fn set(&mut self, index: usize, val: MessageId) -> Result<()>
    where
        T: Write,
    {
        debug_assert!(index < self.len()?);
        let target = self.start_offset + size_of::<MessageId>() * index;
        self.handle.seek(SeekFrom::Start(target as u64))?;
        Ok(self.handle.write_pod(&val)?)
    }

    // Unify with all(), remove one of them? Probably the other, I think this is better?
    pub fn to_vec(&mut self) -> Result<Vec<MessageId>> {
        let mut temp = Vec::new();
        let count = self.len()?;
        self.handle
            .seek(SeekFrom::Start(self.start_offset as u64))?;
        for _i in 0..count {
            let msg: MessageId = self.handle.read_pod()?;
            temp.push(msg);
        }
        Ok(temp)
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


const MAGIC: [u8;8] = [b'N', 152, 202, 45, 103, 197, 68, b'N'];

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct FileHeaderEntry {
    header_checksum: [u8; HASH_SIZE],
    magic: [u8;8],
    sha2: [u8; HASH_SIZE],
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

impl FileHeaderEntry {
    pub fn is_deleted(&self) -> bool {
        self.sha2 == [0u8; 16]
    }
    pub fn set_deleted(&mut self) {
        self.sha2 = [0u8; 16];
    }

    pub fn total_size_except_hash(&self) -> usize {
        self.total_size() - HASH_SIZE
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
    fn read_pod<T: Pod>(&mut self) -> Result<T>;
}
pub trait WritePod {
    fn write_pod<T: Pod>(&mut self, pod: &T) -> Result<()>;
}
impl<R: Read> ReadPod for R {
    fn read_pod<T: Pod>(&mut self) -> Result<T> {
        let mut zeroed = T::zeroed();
        let bytes = bytemuck::bytes_of_mut(&mut zeroed);
        self.read_exact(bytes)?;
        Ok(zeroed)
    }
}
impl<W: Write> WritePod for W {
    fn write_pod<T: Pod>(&mut self, pod: &T) -> Result<()> {
        let bytes = bytemuck::bytes_of(pod);
        self.write_all(bytes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Pod, Zeroable, PartialEq, Eq)]
#[repr(transparent)]
struct U1(u8);

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
    fn bits(self) -> u64 {
        self.0
    }
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
    pub(crate) fn deleted() -> Self {
        Self(u64::MAX)
    }
    pub(crate) fn is_deleted(self) -> bool {
        self.0 == u64::MAX
    }
    fn offset(self) -> Option<u64> {
        (self.0 != u64::MAX).then_some(self.0 >> 1)
    }

    // TODO: Consider changing u64 almost everywhere here to usize.
    // We absolutely _don't_ support files larger than usize (because we're memory-mapping, and
    // on 32-bit systems virtual memory is still smaller than usize range).
    fn file_and_offset(self) -> Option<(U1, u64)> {
        (self.0 != u64::MAX).then(|| {
            (
                if self.0 & 1 == 1 { U1::ONE } else { U1::ZERO },
                self.0 >> 1,
            )
        })
    }
    fn file(self) -> Option<U1> {
        (self.0 != u64::MAX).then(|| if self.0 & 1 == 1 { U1::ONE } else { U1::ZERO })
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct IndexEntry {
    pub(crate) message: MessageId,
    /// Offset into the logical file-area, or deletion-marker
    pub(crate) file_offset: FileOffset,
    /// This is the size with header, parents and payload.
    pub(crate) file_total_size: u64,
}

const DEFAULT_MAX_COUNT: usize = 1024;
const DEFAULT_MAX_SIZE_BYTES: usize = DEFAULT_MAX_COUNT * std::mem::size_of::<IndexEntry>();

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

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct DataFileEntry {
    /// The size of this file, in bytes.
    nominal_size: u64,
    /// The number of bytes actually used.
    /// If this becomes small, compaction may be in order
    /// This figure includes the file header (and any padding we might want to add later).
    /// This means that an unfragmented file has `bytes_used` == `nominal_size`
    bytes_used: u64,

    /// All file data before this offset has been moved
    compaction_pointer: u64,
}

#[derive(Debug)]
struct DataFileInfo {
    file: FileAccessor,
    file_number: U1,
}

const STATUS_OK: u32 = 1;
/// Not ok
const STATUS_NOK: u32 = 0;

#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
struct StoreHeader {
    entries: u32,
    padding: u32,
    // Compact file=1 + old file being compacted=0
    data_files: [DataFileEntry; 2],
    cutoff: CutOffHashPos,
}

pub(crate) struct OnDiskMessageStore<M> {
    target: Target,
    index_mmap: FileAccessor,
    data_files: [DataFileInfo; 2],
    active_file: U1,
    phantom: PhantomData<M>,
    loaded_existing: bool,
    filesystem_sync_disabled: bool,
    // MessageId's that aren't parents to any other message
    //update_heads: FileAccessor,
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
fn dedup_slice<T:PartialEq+Copy>(slice: &mut [T]) -> usize {
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
    pub fn get_upstream_of(
        &self,
        message_ids: impl DoubleEndedIterator<Item = (MessageId, /*query count*/ usize)>,
    ) -> Result<impl Iterator<Item = (MessageHeader, usize /*query count*/)> + '_>
    where
        M: MessagePayload,
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
        //println!("Breadth: {:?}, index: {}", breadth, index);

        Ok(message_index[0..index]
            .iter()
            .rev()
            .map(move |x| {
                //println!("Inspecting {:?}", x);
                if let Some((original_count, remaining_count)) = breadth.remove(&x.message) {
                    //println!("Remaining count {:?}, orig: {}", remaining_count, original_count);
                    if x.file_offset.is_deleted() {
                        //println!("Deleted! ({})", x.message);
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
        let header: &StoreHeader = bytemuck::from_bytes(&slice[0..size_of::<StoreHeader>()]);
        let file_capacity =
            (xlen.saturating_sub(size_of::<StoreHeader>())) / size_of::<IndexEntry>();
        let used = header.entries as usize;

        if file_capacity.saturating_sub(used) >= extra {
            return Ok(());
        }

        if file_capacity + extra >= u32::MAX as usize {
            return Err(anyhow!("Database max-size reached (~2^32 entries)"));
        }

        let base_capacity = file_capacity + extra;
        // Need to realloc
        let mut new_entries = base_capacity.saturating_mul(2);
        if new_entries > (u32::MAX / 2 + u32::MAX / 4) as usize {
            new_entries = u32::MAX as usize;
        }
        let new_file_size = (new_entries) * (size_of::<IndexEntry>()) + (size_of::<StoreHeader>());

        map.grow(new_file_size)
            .with_context(|| format!("resizing index file to {}", new_file_size))?;

        Ok(())
    }

    #[inline]
    fn header(map: &mut FileAccessor) -> Result<&mut StoreHeader> {
        Self::provide_index_map(map, 0)?;

        let slice: &mut [u8] = map.map_mut();
        let header: &mut StoreHeader =
            bytemuck::from_bytes_mut(&mut slice[0..size_of::<StoreHeader>()]);
        Ok(header)
    }

    pub fn contains_index(&self, index: usize) -> Result<bool> {
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
        // TODO: Create a fast-path for the case where we do have enough space, so we can inline
        // this method, and only call into something like `Self::provide_index_map` when needed
        Self::provide_index_map(map, extra)?;
        let slice: &mut [u8] = map.map_mut();
        let (store_header_bytes, index_bytes) = slice.split_at_mut(size_of::<StoreHeader>());
        let header: &mut StoreHeader = bytemuck::from_bytes_mut(store_header_bytes);
        let rest = bytemuck::cast_slice_mut(index_bytes);
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
        let header: &StoreHeader = bytemuck::from_bytes(store_header_bytes);
        let rest = bytemuck::cast_slice(index_bytes);
        Ok((header, &rest[0..header.entries as usize]))
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
        M: MessagePayload,
    {
        info!("Start recovery procedure");
        self.index_mmap.truncate(0)?;

        let magic_finder = memchr::memmem::Finder::new(&MAGIC);

        self.index_mmap.write_zeroes(size_of::<StoreHeader>())?;
        let pending_index_header: &mut StoreHeader = unsafe { &mut *(self.index_mmap.map_mut_ptr() as *mut StoreHeader) };
        self.index_mmap.seek(SeekFrom::Start(size_of::<StoreHeader>() as u64))?;

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

                fn read_header_and_parents<M:MessagePayload>(file_info: &mut FileAccessor) -> Result<(FileHeaderEntry, Vec<MessageId>, usize)> {
                    let file_offset = file_info.stream_position()?;
                    //println!("Reading message at {} / {}", file_offset, file_info.used_space());
                    let header = file_info.read_pod::<FileHeaderEntry>()?;
                    //println!("Recovery encountered message: {:x?} at {}", header.header_checksum, file_offset);
                    //println!("Header: {:x?}", header);
                    if header.magic != MAGIC {
                        bail!("Bad magic");
                    }
                    let tot_size = header.total_size();

                    if header.is_deleted() {
                        if tot_size == 0 {
                            bail!("Corrupt file"); //TODO: More robust recovery!
                        }
                        file_info
                            .seek(SeekFrom::Start(file_offset + tot_size as u64))?;
                        bail!("Header is deleted");
                    }

                    let mut parvec = EmbVecAccessor::new_parents(&mut *file_info, file_offset);
                    let parents = parvec.all()?;

                    let payload_start = file_offset as usize + header.offset_of_payload();
                    let checksum_start = file_offset  as usize + header.start_of_data_covered_by_header_checksum();
                    let header_checksum = file_info.with_bytes_at(checksum_start, payload_start-checksum_start, |bytes|{
                        sha2(bytes)
                    })?;
                    if header.header_checksum != header_checksum {
                        bail!("Header checksum mismatch: {:?} {:?}", header.header_checksum, header_checksum);
                    }

                    file_info.seek(SeekFrom::Start(
                        file_offset + header.offset_of_payload() as u64,
                    ))?;

                    file_info
                        .with_bytes(header.payload_size as usize, |payload_bytes| {
                            let actual_sha2 = sha2_message(header.message_id, payload_bytes);
                            if actual_sha2 != header.sha2 {
                                return Err(anyhow!("data corrupted on disk - checksum mismatch"));
                            }
                            Ok(())
                        })??;

                    file_info
                        .seek(SeekFrom::Start(file_offset + tot_size as u64))?;
                    Ok((header, parents, tot_size))
                }


                let (header, parents, msg_size_bytes) = match read_header_and_parents::<M>(&mut file_info.file) {
                    Ok((header, parents, msg_size_bytes)) => {
                        debug_assert!(file_info.file.stream_position()? >= largest_used_offset);
                        largest_used_offset = file_info.file.stream_position()?;
                        (header, parents, msg_size_bytes)
                    },
                    Err(err) => {
                        if file_offset < prev_used_space  as u64 {
                            warn!("Error reading message: {} @ offset {}", err, file_offset);
                        } else {
                            trace!("Message not recovered: {} @ offset {}", err, file_offset);
                        }
                        //println!("Error reading message: {:?} @ offset {}, seek pos: {}", err, file_offset,file_info.file.stream_position()?);;
                        let magic_search_start = file_offset as usize + offset_of!(FileHeaderEntry, magic) + size_of_val(&MAGIC);
                        let new_offset = file_info.file.with_all_bytes(|bytes|{
                            //println!("Starting search at {}", magic_search_start);
                            magic_finder.find(&bytes[magic_search_start..]).map(|x| x + magic_search_start - offset_of!(FileHeaderEntry, magic)).unwrap_or(file_length as usize)
                        })?;
                        //println!("Next offsed to try: {} of {}", new_offset, file_length);
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


        let (index_header, mut index) =
            Self::header_and_index_mut(&mut self.index_mmap)?;
        index.sort();
        index_header.entries = dedup_slice(index) as u32;
        index = &mut index[0..index_header.entries as usize];

        for item in index.iter() {
            debug_assert_eq!(item.file_offset.is_deleted(), false);
            if let Some((file, offset)) = item.file_offset.file_and_offset() {
                let mut file = &mut self.data_files[file.index()].file;
                let mut parent_accessor = EmbVecAccessor::new_parents(&mut file, offset);

                parent_accessor.retain(|cand|{
                    index.binary_search_by_key(&cand, |x|x.message).is_ok()
                })?;

                let mut child_accesor = EmbVecAccessor::new_children(&mut file, offset)?;
                child_accesor.retain(|cand|{
                    index.binary_search_by_key(&cand, |x|x.message).is_ok()
                })?;
            }
        }


        Self::recover_cutoff_state(now, index_header, index, cutoff_config)?;
        Ok(())
    }

    /*fn get_cutoff_state(&mut self) -> Result<&mut CutOffHashPos> {
        let hdr_start = self.index_mmap.map_mut_ptr();

        let (hdr, entries) = Self::header_and_index_mut(&mut self.index_mmap)?;

    }*/

    fn recover_cutoff_state(
        time_now: NoatunTime,
        index_header: &mut StoreHeader,
        index: &[IndexEntry],
        config: &CutOffConfig,
    ) -> Result<()> {
        let cutoff_time = config.nominal_cutoff(CutOffTime::from_noatun_time(time_now));
        index_header.cutoff.before_time = cutoff_time;
        index_header.cutoff.hash = CutoffHash::ZERO;
        let cutoff_time_noatun: NoatunTime = cutoff_time.into();
        for item in index.iter() {
            if item.message.timestamp() >= cutoff_time_noatun {
                break;
            }
            index_header.cutoff.apply(item.message, "recovery");
        }

        Ok(())
    }

    pub fn contains_message(&self, start: MessageId) -> Result<bool> {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;
        match message_index.binary_search_by_key(&start, |x| x.message) {
            Ok(index) => Ok(!message_index[index].file_offset.is_deleted()),
            Err(_err) => Ok(false),
        }
    }
    /// Get all messages with id start or greater
    fn query_greater(&self, start: MessageId) -> Result<impl Iterator<Item = &IndexEntry>> {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;

        let (Ok(index) | Err(index)) = message_index.binary_search_by_key(&start, |x| x.message);

        Ok(message_index[index..]
            .iter()
            .filter(|x| !x.file_offset.is_deleted()))
    }

    pub fn next_index(&self) -> Result<usize> {
        let (header, _message_index) = self.header_and_index().context("opening index file")?;
        Ok(header.entries as usize)
    }

    /// Get all messages with index `index` or greater. Note, the index series has holes,
    /// so the indices of the returned items don't need to be contiguous, or even start at 'index'
    pub fn query_by_index(
        &self,
        index: usize,
    ) -> Result<impl Iterator<Item = (usize /*seqnr/index*/, Message<M>)> + '_>
    where
        M: MessagePayload,
    {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;

        let data_files = &self.data_files;
        Ok(message_index[index..]
            .iter()
            .enumerate()
            .filter(|(_sub_index, x)| !x.file_offset.is_deleted())
            .map(move |(sub_index, x)| (index + sub_index, Self::read_msg(x, data_files, None)))
            .filter_map(|(index, x)| {
                match x {
                    Ok(x) => Some((index, x)),
                    Err(x) => {
                        warn!("Failed to load message #{}: {:?}", index, x);
                        //TODO: Log
                        None
                    }
                }
            }))
    }

    /// Return the position where a message with the given ID could be inserted, or None if the message already exists.
    pub fn get_insert_point(&self, msg: MessageId) -> Result<Option<usize>> {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;

        if let Err(index) = message_index.binary_search_by_key(&msg, |x| x.message) {
            Ok(Some(index))
        } else {
            // Already exists
            Ok(None)
        }
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

        //TODO: We could probably have a way to create a very cheap read-only clone
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

        //file.seek(SeekFrom::Start(offset + header.size_before_payload() as u64))?;

        Ok(MessageHeader {
            id: header.message_id,
            parents,
        })
    }
    fn read_msg(
        entry: &IndexEntry,
        data_files: &[DataFileInfo; 2],
        children: Option<&mut Vec<MessageId>>,
    ) -> Result<Message<M>>
    where
        M: MessagePayload,
    {
        let Some((file, offset)) = entry.file_offset.file_and_offset() else {
            return Err(anyhow!(
                "Message not found in store (appears deleted): {:?}",
                entry.message
            ));
        };

        //TODO: We could probably have a way to create a very cheap read-only clone
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

        file.seek(SeekFrom::Start(
            offset + header.offset_of_payload() as u64,
        ))?;

        let msg_payload = file.with_bytes(header.payload_size as usize, |bytes| {
            if sha2_message(header.message_id, bytes) != header.sha2 {
                return Err(anyhow!("Data corruption detected!"));
            }
            M::deserialize(bytes)
        })??;

        debug_assert_eq!(header.message_id, entry.message);
        Ok(Message {
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
        let header: &mut FileHeaderEntry = file.access_pod_mut(offset as usize)?;

        header.set_deleted();
        entry.file_offset.set_deleted();

        Ok(())
    }

    /// Return the given message, or None if it does not exist.
    /// If the call itself fails, returns Err.
    pub fn read_message(&self, id: MessageId) -> Result<Option<Message<M>>>
    where
        M: MessagePayload,
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
    ) -> Result<Option<(Message<M>, Vec<MessageId>)>>
    where
        M: MessagePayload,
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
    /// Return the index of the given message, if it exists, otherwise None.
    /// If the call itself fails, returns Err.
    pub fn get_index_of(&mut self, id: MessageId) -> Result<Option<usize>> {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;

        match message_index.binary_search_by_key(&id, |x| x.message) {
            Ok(index) => {
                if message_index[index].file_offset.is_deleted() {
                    Ok(None)
                } else {
                    Ok(Some(index))
                }
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
            return Ok(message_index.len()); //TODO: This is already done by 'binary_search_by_key'? Remove!
        }
        let (Ok(i) | Err(i)) = message_index.binary_search_by_key(&id, |x| x.message);
        Ok(i)
    }
    /// Return the greatest index such that all messages with a smaller index occur before
    /// time. All returned messages will be < time, and all messages before < time will have
    /// a smaller index. If there are no messages, this returns 0.
    /// If all messages are < time, this returns an index "one after the end".
    /// The returned index may point at a removed message.
    pub fn get_index_after(&self, time: NoatunTime) -> Result<SequenceNr> {
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
    pub fn get_all_messages(&self) -> Result<Vec<Message<M>>>
    where
        M: MessagePayload,
    {
        let (_header, message_index) = self.header_and_index().context("opening index file")?;
        let data_files = &self.data_files;
        message_index
            .iter()
            .filter(|x| !x.file_offset.is_deleted())
            .map(|x| Self::read_msg(x, data_files, None))
            .collect()
    }
    pub fn get_all_messages_with_children(&self) -> Result<Vec<(Message<M>, Vec<MessageId>)>>
    where
        M: MessagePayload,
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
    // TODO: Consider if there's a risk of inefficiency when two severely desynced instances meet.
    // They might have completely different messages, even before the cutoff time.
    // When they start bringing each other up-to-date, their before-cutoff timestamp will change.
    // Will this cause them to restart the sync-process? Make sure it doesn't.
    pub fn mark_deleted_by_index(
        &mut self,
        delete_index: SequenceNr,
        head_tracker: &mut UpdateHeadTracker,
    ) -> Result<bool>
    where
        M: MessagePayload,
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
            //TODO: Trace log
            return Ok(false);
        };
        if entry.file_offset.is_deleted() {
            return Ok(false);
        }

        if let Some((file, _offset)) = entry.file_offset.file_and_offset() {
            header.cutoff.report_delete(entry.message);

            let id = entry.message;
            header.data_files[file.index()].bytes_used -= entry.file_total_size;

            // TODO: Make sure recovery procedure can handle duplicate messages
            Self::delete_msg_from_file(entry, &mut self.data_files)?;

            head_tracker.remove_update_head(entry.message)?;

            let parents = msg.parents;
            for parent in &parents {
                self.add_remove_parents_and_children(*parent, &[], None, &children, Some(id))?;
            }

            for child in &children {
                self.add_remove_parents_and_children(*child, &[], Some(id), &[], None)?;
            }
            Ok(true)
        } else {
            warn!("Message was already deleted");
            Ok(false)
        }

    }

    pub fn set_cutoff_hash(&mut self, cutoff: CutOffHashPos) -> Result<()> {
        let (header, _message_index) =
            Self::header_and_index_mut(&mut self.index_mmap).context("Reading index file")?;
        header.cutoff = cutoff; //TODO: Should this really be in the message store?
        Ok(())
    }

    /// Return the given message, or None if it does not exist.
    /// If the call itself fails, returns Err.
    pub fn read_message_by_index(&mut self, index: usize) -> Result<Option<Message<M>>>
    where
        M: MessagePayload,
    {
        let (_header, message_index) =
            Self::header_and_index_mut(&mut self.index_mmap).context("Reading index file")?;

        let data_files = &mut self.data_files;
        match message_index.get(index) {
            Some(entry) => {
                if entry.file_offset.is_deleted() {
                    return Ok(None);
                }
                let msg = Self::read_msg(entry, data_files, None)?;
                Ok(Some(msg))
            }
            None => Err(anyhow!("Invalid message index")),
        }
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
            // TODO: Trace log?
            info!("Message to mark as transmitted no longer existed");
            return Ok(false);
        };
        let index_entry = &message_index[index];

        if let Some((file, offset)) = index_entry.file_offset.file_and_offset() {
            let file_header: &mut FileHeaderEntry = self.data_files[file.index()]
                .file
                .access_pod_mut(offset as usize)?;
            file_header.has_been_transmitted = 1;
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
    pub fn append_many_sorted(
        &mut self,
        messages: impl ExactSizeIterator<Item = Message<M>>,
        message_inserted: impl FnMut(MessageId, /*parents*/ &[MessageId]) -> Result<()>,
        local: bool,
    ) -> Result<Option<usize>>
    where
        M: MessagePayload + Debug,
    {
        self.do_append_many_sorted(messages, message_inserted, local)
    }

    /// Marks the db as dirty
    /// Find all the given messages, and mark their entries in their data files as unused.
    /// Use heuristics to determine if files should be compacted.
    /// Compact files (moving to higher file if suitable).
    /// Delete from the index.
    /// Mark the db as clean.
    /// Messages that are not found are simply ignored. The operation does not stop or fail.
    pub fn delete_many(
        &mut self,
        messages: impl Iterator<Item = MessageId>,
        update_head_tracker: &mut UpdateHeadTracker,
    ) -> Result<()>
    where
        M: MessagePayload,
    {
        self.do_delete_many(messages, update_head_tracker)
    }

    fn do_delete_many(
        &mut self,
        messages: impl Iterator<Item = MessageId>,
        update_head_tracker: &mut UpdateHeadTracker,
    ) -> Result<()>
    where
        M: MessagePayload,
    {
        for message in messages {
            let (_header, message_index) =
                Self::header_and_index_mut(&mut self.index_mmap).context("Reading index file")?;

            let Ok(index) = message_index.binary_search_by_key(&message, |x| x.message) else {
                continue;
            };
            if message_index[index].file_offset.is_deleted() {
                return Ok(());
            }
            self.mark_deleted_by_index(SequenceNr::from_index(index), update_head_tracker)?;
            /*let message_entry = &mut message_index[index];

            if let Some((file, offset)) = message_entry.file_offset.file_and_offset() {
                let store_file_entry = &mut self.data_files[file.index()];
                message_entry.file_offset = FileOffset::deleted();
                debug_assert!(message_entry.file_offset.is_deleted());
                let index_entry = &mut header.data_files[file.index()];

                store_file_entry
                    .file
                    .seek(SeekFrom::Start(offset + offset_of!(FileHeaderEntry, sha2) as u64 ))
                    .context("seeking file")?;
                store_file_entry.file.write_zeroes(HASH_SIZE)?;
                store_file_entry.file.flush()?;
                index_entry.bytes_used -= message_entry.file_total_size;
                            }
             */
        }
        Ok(())
    }

    /// Make sure the given number of slots are available.
    fn provide_index_map_for(&mut self, count: usize) -> Result<()> {
        Self::provide_index_map(&mut self.index_mmap, count)?;
        Ok(())
    }

    // If remaining_child_assignments is None, don't rewrite parents by adding new children to them
    fn do_write_message(
        files: &mut [DataFileInfo; 2],
        file_index: U1,
        msg: &Message<M>,
        children: &[MessageId],
        local: bool,
        remaining_child_assignments: Option<&mut RemainingChildAssignments>,
    ) -> Result<MessageWriteReport>
    where
        M: MessagePayload,
    {
        for parent in msg.header.parents.iter() {
            if *parent >= msg.id() {
                bail!("parent must be temporally before child")
            }
        }

        let file = &mut files[file_index.index()].file;
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
            header_checksum: [0;HASH_SIZE],
            sha2: sha2_hash,
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


        let file = &mut files[file_index.index()].file;
        file.seek(SeekFrom::Start(end_pos))?;

        Ok(MessageWriteReport {
            start_pos,
            total_size: msg_total_size as u64,
        })
    }
    fn do_header_checksum(file: &mut FileAccessor, start_pos: u64) -> Result<()> {
        let payload_pos = {
            let header: &FileHeaderEntry = file.access_pod(start_pos as usize)?;
            header.offset_of_payload()
        };

        let start_of_data_for_header_checksum = start_pos as usize + offset_of!(FileHeaderEntry, payload_size);
        let size_of_data_for_header_checksum = payload_pos - offset_of!(FileHeaderEntry, payload_size);
        let header_checksum = file.with_bytes_at(start_of_data_for_header_checksum, size_of_data_for_header_checksum, |bytes|{
            sha2(bytes)
        })?;

        {
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
        M: MessagePayload,
    {
        //dbg!(id,new_parents,new_children, removed_parent, removed_child);
        let (_header, search_index) = Self::header_and_index_mut(&mut self.index_mmap)?;
        let Ok(index) = search_index.binary_search_by_key(&id, |x| x.message) else {
            return Ok(());
        };
        if let Some((file, offset)) = search_index[index].file_offset.file_and_offset() {
            let file = &mut self.data_files[file.index()].file;
            //let _header: &FileHeaderEntry = file.access_pod(offset as usize)?;
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

    fn add_remove_parents_and_children(
        &mut self,
        id: MessageId,
        new_parents: &[MessageId],
        removed_parent: Option<MessageId>,
        new_children: &[MessageId],
        removed_child: Option<MessageId>,
    ) -> Result<()>
    where
        M: MessagePayload,
    {
        //dbg!(id,new_parents,new_children, removed_parent, removed_child);
        let (_header, search_index) = Self::header_and_index_mut(&mut self.index_mmap)?;
        let Ok(index) = search_index.binary_search_by_key(&id, |x| x.message) else {
            info!("message not found, can't addremove parents/children {:?}", id);
            return Ok(());
        };
        if let Some((file, offset)) = search_index[index].file_offset.file_and_offset() {
            let file = &mut self.data_files[file.index()].file;
            let header: &FileHeaderEntry = file.access_pod(offset as usize)?;
            if header.num_parents.free() < new_parents.len()
                || header.num_children.free() < new_children.len()
            {
                self.slow_add_parents_and_children(id, new_parents, new_children)?;
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

    fn slow_add_parents_and_children(
        &mut self,
        id: MessageId,
        new_parents: &[MessageId],
        new_children: &[MessageId],
    ) -> Result<()>
    where
        M: MessagePayload,
    {
        //dbg!(new_parents, new_children);
        // TODO: Maybe add fast apth, where caller can supply index
        let Some((mut msg, mut children)) = self.read_message_and_children(id)? else {
            return Ok(());
        };

        // TODO: Maybe create an optimized header_and_index_mut that has a fast path
        let (_header, search_index) = Self::header_and_index_mut(&mut self.index_mmap)?;
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

        // TODO: Preserve 'local' flag in this case!!
        let write_report = Self::do_write_message(
            &mut self.data_files,
            self.active_file,
            &msg,
            &children,
            false,
            None,
        )?;

        // TODO: This can be optimized - we've already looked this up when we did `read_message` above.
        if let Ok(index) = search_index.binary_search_by_key(&msg.header.id, |x| x.message) {
            let index_entry = &mut search_index[index];
            debug_assert!(!index_entry.file_offset.is_deleted());
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
        M: MessagePayload,
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

    fn do_append_many_sorted(
        &mut self,
        messages: impl ExactSizeIterator<Item = Message<M>>,
        mut message_inserted: impl FnMut(MessageId, &[MessageId] /*parents*/) -> Result<()>,
        local: bool,
    ) -> Result<Option<usize>>
    where
        M: MessagePayload + Debug,
    {

        let mut prev = None;
        // TODO: Remove this perhaps, below?
        let mut messages = messages.inspect(|test| {
            trace!("Inspecting for insert: {:?}", test);
            if prev.is_none() {
                prev = Some(test.header.id);
            } else {
                #[cfg(debug_assertions)]
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

        #[cfg(debug_assertions)]
        {
            let mut new_cutoff = index_header.cutoff;
            new_cutoff.hash = CutoffHash::default();
            for msg in mmap_index[0..index_header.entries as usize]
                .iter()
                .filter(|x| !x.file_offset.is_deleted())
            {
                new_cutoff.apply(msg.message, "debug");
            }
            assert_eq!(new_cutoff, index_header.cutoff);
        }

        //Self::check_duplicates(&mmap_index[0..index_header.entries as usize]);


        let mut last_msg_id = None;
        let mut carry_buffer: VecDeque<IndexEntry> = VecDeque::with_capacity(len);
        //let input_stream_pos = 0;

        let first_input_message: Message<M> = messages.next().unwrap();

        let initial_index_entries = index_header.entries as usize;
        let (Ok(first_index) | Err(first_index)) = mmap_index[0..index_header.entries as usize]
            .binary_search_by_key(&first_input_message.id(), |x| x.message);
        let mut cur_index = first_index;
        let mut index_we_must_rewind_db_to = None;

        //let mut parents: Vec<MessageId> = vec![];
        //println!("Start of loop. cur_index ={}, first input message: {:?}", cur_index, first_input_message.id());

        let mut cur_input_message = Some(first_input_message);

        let mut remaining_child_assignments = RemainingChildAssignments::default();

        loop {
            /*if let Some(cur_input_message) = &cur_input_message {
                println!("Start of loop, cur input msg: {:?}", cur_input_message.header.id);
            }*/
            //info!("Loop start: cur_input_message: {:?}", cur_input_message);
            if carry_buffer.is_empty() && cur_input_message.is_none() {
                //info!("Insert loop done");
                break;
            }

            //let info = &mut self.data_files[self.active_file.index()];

            //dbg!(&cur_input_message);
            let cur_index_entry = &mut mmap_index[cur_index];
            let present_id = if cur_index < index_header.entries as usize {
                (!cur_index_entry.file_offset.is_deleted()).then(|| cur_index_entry.message)
            } else {
                None
            };

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
                (Cases::NextFromPresent, present_id),
                (
                    Cases::NextFromCarry,
                    carry_buffer.front().map(|x| x.message),
                ),
                (
                    Cases::NextFromInput,
                    cur_input_message.as_ref().map(|x| x.id()),
                ),
            ];
            let (now_case, _now_message_id) = cases
                .iter()
                .filter_map(|(case, val)| val.map(|x| (*case, x)))
                .min_by_key(|x| x.1)
                .expect("There must always be some case present");

            match now_case {
                Cases::NextFromPresent => {
                    let input_message_id = cur_input_message.as_ref().map(|x| x.id());
                    if present_id == input_message_id {
                        //info!("Duplicate detected");
                        cur_input_message = messages.next();
                        cur_index += 1;
                        //eprintln!("Duplicate id detected");
                        continue;
                    }
                    // We get here if a previous item was a duplicate. There's no copying to be done
                    cur_index += 1;
                    warn!("A previous item was a duplicate");
                    continue;
                    /*dbg!(&cur_input_message, &present_id,&carry_buffer,&cases,&now_case, &now_message_id);
                    unreachable!("This shouldn't, logically, happen");*/
                }
                Cases::NextFromCarry => {
                    let temp = carry_buffer.pop_front().unwrap();

                    debug_assert!(carry_buffer.iter().is_sorted_by_key(|x| x.message));

                    if cur_index < initial_index_entries {
                        let (Ok(insert_at) | Err(insert_at)) = carry_buffer
                            .binary_search_by_key(&cur_index_entry.message, |x| x.message);

                        carry_buffer.insert(insert_at, *cur_index_entry);
                        debug_assert!(carry_buffer.iter().is_sorted_by_key(|x| x.message));
                    }
                    let input_message_id = cur_input_message.as_ref().map(|x| x.id());
                    if Some(temp.message) == input_message_id {
                        cur_input_message = messages.next();
                        //println!("Duplicate id detected in 2nd way");
                    }

                    *cur_index_entry = temp;
                    cur_index += 1;
                }
                Cases::NextFromInput => {
                    let msg = cur_input_message.take().unwrap();

                    info!(
                        "Inserting message {:?} at index {}",
                        msg.header.id, cur_index
                    );
                    index_header.cutoff.report_add(msg.header.id);

                    message_inserted(msg.id(), &msg.header.parents)?;
                    if index_we_must_rewind_db_to.is_none() {
                        index_we_must_rewind_db_to =
                            Some(Self::find_valid_index_at_or_before(cur_index, mmap_index));
                    }
                    //Self::register_heads_and_tails(&mut self.update_heads, &parents, &msg.id())?;
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
                        &msg,
                        &[],
                        local,
                        Some(&mut remaining_child_assignments),
                    )?;

                    let cur_index_entry = &mut mmap_index[cur_index];
                    // Append to the index (compacting while doing so)
                    if cur_index < index_header.entries as usize
                        && !cur_index_entry.file_offset.is_deleted()
                    {
                        //println!("Pushing {:?} to carry, to make space for {:?}", cur_index_entry.message, msg.header.id);
                        carry_buffer.push_back(*cur_index_entry);
                        debug_assert!(carry_buffer.iter().is_sorted_by_key(|x| x.message));
                    }
                    *cur_index_entry = IndexEntry {
                        message: msg.id(),
                        file_offset: FileOffset::new(start_pos, self.active_file),
                        file_total_size: total_size,
                    };
                    cur_index += 1;
                    let full_msg_size = total_size;

                    index_header.data_files[self.active_file.index()].bytes_used += full_msg_size;
                    index_header.data_files[self.active_file.index()].nominal_size += full_msg_size;

                    cur_input_message = messages.next();

                }
            }
        }

        let info = &mut self.data_files[self.active_file.index()];
        let final_file_position = info.file.stream_position()?;
        if !self.filesystem_sync_disabled {
            info.file
                .flush_range(
                    initial_file_position as usize,
                    (final_file_position - initial_file_position) as usize,
                )
                .context("flush file to disk")?;

        }
        //let data_file = &mut index_header.data_files[self.active_file.index()];

        index_header.entries = index_header
            .entries
            .max(cur_index.try_into().expect("max message count exceeded")); //TODO: Fail better when hitting hard size limit

        //Self::check_duplicates(&mmap_index[0..index_header.entries as usize]);
        debug_assert!(mmap_index[0..index_header.entries as usize].is_sorted_by_key(|x| x.message));

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
            //println!("KNOWN GOOD");
        }

        self.handle_remaining_child_assignments(remaining_child_assignments)?;

        //self.compact()?;

        trace!(
            "Finished do_append_many_sorted, now with {} elements",
            cur_index
        );

        Ok(index_we_must_rewind_db_to)
    }
    fn check_duplicates(mmap_index: &[IndexEntry]) {
        for (index, window) in mmap_index.windows(2).enumerate() {
            #[allow(clippy::nonminimal_bool)]
            if !(window[0].message < window[1].message) {
                panic!(
                    "Failed condition: {:?} < {:?} at {}",
                    window[0].message, window[1].message, index
                );
            }
        }
    }

    /// Compact all files to the given maximum degree of fragmentation.
    /// NOTE! Never call this method with a value smaller than 25. This can lead
    /// to abysmal performance. If disk space permits, consider allowing up to 50% fragmentation.
    fn compact_to_target(&mut self, tolerated_fragmentation_percent: u32) -> Result<()>
    where
        M: MessagePayload,
    {
        self.do_compact(|wasted_space, total_space| {
            let frag_perc = (100 * wasted_space) / total_space;
            frag_perc > tolerated_fragmentation_percent as u64
        })
    }

    /// Compact all files
    pub(crate) fn compact(&mut self) -> Result<()>
    where
        M: MessagePayload,
    {
        self.compact_to_target(25)
    }

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

    fn do_compact(
        &mut self,
        mut need_compact: impl FnMut(/*wasted bytes:*/ u64, /*total bytes: */ u64) -> bool,
    ) -> Result<()> {
        let completed_one_compaction = self.do_compact_impl(&mut need_compact)?;
        if completed_one_compaction {
            self.do_compact_impl(need_compact)?;
        }

        Ok(())
    }
    /// Returns true if 'active' has been swapped
    fn do_compact_impl(
        &mut self,
        mut need_compact: impl FnMut(/*wasted bytes:*/ u64, /*total bytes:*/ u64) -> bool,
    ) -> Result<bool> {
        let (header, index) = Self::header_and_index_mut(&mut self.index_mmap)?;

        let active_file = self.active_file;
        let (src_file_info, dst_file_info) = Self::get_src_dst(&mut self.data_files, active_file);
        let (src_file_entry, dst_file_entry) =
            Self::get_src_dst(&mut header.data_files, active_file);

        //let initial_compacted_bytes = src_file_entry.compaction_pointer;
        loop {
            let src_wasted = src_file_entry.nominal_size - src_file_entry.bytes_used;
            let dst_wasted = dst_file_entry.nominal_size - dst_file_entry.bytes_used;
            let src_tot = src_file_entry.nominal_size;
            let dst_tot = dst_file_entry.nominal_size;


            let src_position = src_file_entry.compaction_pointer;
            if src_file_entry.compaction_pointer >= src_file_entry.nominal_size {

                if !self.filesystem_sync_disabled {
                    dst_file_info.file.flush_all()?;
                }

                src_file_entry.nominal_size = 0;
                src_file_entry.bytes_used = 0;
                src_file_entry.compaction_pointer = 0;
                if !self.filesystem_sync_disabled {
                    src_file_info.file.flush_all()?;
                }
                src_file_info.file.seek_to(0)?;
                src_file_info.file.set_used_space(0);

                // Don't actually swap if current active isn't even fragmented at all,
                // or if target is reached
                if dst_file_entry.bytes_used != dst_file_entry.nominal_size
                    && need_compact(src_wasted + dst_wasted, src_tot + dst_tot)
                {
                    self.active_file.swap();
                    return Ok(true);
                }
                return Ok(false);
            }

            if !need_compact(src_wasted + dst_wasted, src_tot + dst_tot) {
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
            dst_file_entry.bytes_used += full_size;
            dst_file_entry.nominal_size += full_size;
        }
    }

    pub fn new<D: Disk>(
        d: &mut D,
        target: &Target,
        max_file_size: usize,
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
            let file_name = format!("data{}", file_number);
            let data_file = d
                .open_file(target, &file_name, 0, max_file_size)
                .context("Opening data-file")?;

            if data_file.on_disk_size() > 0 {
                db_existed = true;
            }


            data_file
                .try_lock_exclusive()
                .with_context(||format!("While obtaining file lock on data file: {:?}", target.path().join(&file_name)))?;

            Ok(DataFileInfo {
                file: data_file,
                file_number: U1::from_index(file_number),
            })
        });

        //TODO: Use array `try_map` once stabilized
        let data_files: [DataFileInfo; 2] = data_files
            .into_iter()
            .collect::<Result<Vec<DataFileInfo>>>()?
            .try_into()
            .unwrap_or_else(|_| unreachable!());

        //let overwrite = target.overwrite(); //TODO: use?

        let mut index_file = d
            .open_file(target, "index", size_of::<StoreHeader>(), max_file_size)
            .context("Opening index-file")?;
        index_file
            .try_lock_exclusive()
            .context("While obtaining lock on index-file")?;

        //let len = index_file.used_space();

        let index = Self::header(&mut index_file)?;

        // The active file is the one with the least percentage of wasted bytes
        let active = index
            .data_files
            .iter()
            .enumerate()
            .min_by_key(|(_index, data_file)| {
                (100 * ((data_file.nominal_size - data_file.bytes_used) as u128))
                    / (data_file.nominal_size.max(1) as u128)
            })
            .unwrap()
            .0;

        //let update_heads = d.open_file(target, "update_heads", 0, 128 * 1024 * 1024)?;

        let this = Self {
            target: target.clone(),
            index_mmap: index_file,
            data_files,
            active_file: U1::from_index(active),
            phantom: Default::default(),
            loaded_existing: db_existed,
            //update_heads,
            filesystem_sync_disabled: false,
        };

        Ok(this)
    }

    pub(crate) fn disable_filesystem_sync(&mut self) {
        self.filesystem_sync_disabled = true;
    }

    pub fn count_messages(&self) -> usize {
        let Ok((_header, index)) = self.header_and_index() else {
            return 0;
        };

        index.iter().filter(|x|!x.file_offset.is_deleted()).count()
    }

    pub fn current_cutoff_hash(&self) -> Result<CutOffHashPos> {
        //TODO: Name? Current is superfluous?
        let (header, _index) = self.header_and_index()?;
        Ok(header.cutoff)
    }
    pub fn current_cutoff_time(&self) -> Result<NoatunTime> {
        //TODO: Name? Current is superfluous?
        let (header, _index) = self.header_and_index()?;
        Ok(header.cutoff.before_time.to_noatun_time())
    }
    /// Warning! The returned slice may contain deleteds messages!
    pub(crate) fn get_messages_slice(&self) -> Result<&[IndexEntry]> {
        let (header, index) = self.header_and_index()?;
        Ok(&index[0..header.entries as usize])
    }
    pub(crate) fn set_cutoff_time(&mut self, time: CutOffTime) -> Result<()> {
        let (header, _index) = Self::header_and_index_mut(&mut self.index_mmap)?;
        header.cutoff.before_time = time;
        Ok(())
    }

    pub(crate) fn get_messages_at_or_after(
        &self,
        message: MessageId,
        count: usize,
    ) -> Result<Vec<MessageId>> {
        let (_header, search_index) = self.header_and_index()?;
        let (Ok(index) | Err(index)) = search_index.binary_search_by_key(&message, |x| x.message);
        let mut result = Vec::with_capacity(count);
        for message in search_index[index..].iter().take(count) {
            if !message.file_offset.is_deleted() {
                result.push(message.message);
            }
        }
        Ok(result)
    }

    pub fn is_acceptable_cutoff_hash(
        &self,
        hash: CutOffHashPos,
        cutoff_config: &CutOffConfig,
    ) -> Result<Acceptability> {
        let (header, _index) = self.header_and_index()?;
        Ok(header.cutoff.is_acceptable_cutoff_hash(hash, cutoff_config))
    }
}

#[cfg(test)]
mod tests {
    use crate::disk_abstraction::{InMemoryDisk, StandardDisk};
    use crate::message_store::{FileOffset, OnDiskMessageStore, U1};
    use crate::{DummyUnitObject, Message, MessageId, MessagePayload, NoatunTime, Target};
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use std::io::{Cursor, Read, Write};

    use crate::cutoff::CutOffConfig;
    use crate::update_head_tracker::UpdateHeadTracker;
    use std::pin::Pin;
    use std::time::Instant;

    #[derive(Debug)]
    struct OnDiskMessage {
        id: u64,
        seq: u64,
        data: Vec<u8>,
    }

    impl MessagePayload for OnDiskMessage {
        type Root = DummyUnitObject;

        fn apply(&self, _time: NoatunTime, _root: Pin<&mut Self::Root>) {
            unimplemented!()
        }

        fn deserialize(buf: &[u8]) -> anyhow::Result<Self>
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

        fn serialize<W: Write>(&self, mut writer: W) -> anyhow::Result<()> {
            writer.write_u64::<LittleEndian>(self.id)?;
            writer.write_u64::<LittleEndian>(self.seq)?;
            writer.write_u64::<LittleEndian>(self.data.len() as u64)?;
            writer.write_all(&self.data)?;
            Ok(())
        }
    }

    #[test]
    fn add_and_recover_messages() {
        let mut store = OnDiskMessageStore::new(
            &mut StandardDisk,
            &Target::CreateNewOrOverwrite("test/add_and_recover_messages.bin".into()),
            10000,
        )
        .unwrap();

        const COUNT: usize = 5; //1_000_000usize;

        //let init = Instant::now();
        let msgs = (0..COUNT).map(|i| {
            Message::new(
                MessageId::new_debug(i as u32),
                vec![],
                OnDiskMessage {
                    id: i as u64,
                    seq: i as u64,
                    data: vec![42u8; 4],
                },
            )
        });

        store
            .append_many_sorted(msgs.into_iter(), |_, _| Ok(()), true)
            .unwrap();

        {
            let _header =
                OnDiskMessageStore::<OnDiskMessage>::header(&mut store.index_mmap).unwrap();
            //header.dirty_status = STATUS_NOK;
        }
        store
            .recover(|_, _| Ok(()), NoatunTime::ZERO, &CutOffConfig::default())
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
            5_000_000_000,
        )
        .unwrap();

        //let init = Instant::now();
        let msg = Message::new(
            MessageId::new_debug(0 as u32),
            vec![],
            OnDiskMessage {
                id: 0 as u64,
                seq: 0 as u64,
                data: vec![42u8; 4],
            },
        );

        //let prev = Instant::now();
        store
            .append_many_sorted(std::iter::once(msg), |_, _| Ok(()), true)
            .unwrap();

        //let prev = Instant::now();
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
            5_000_000_000,
        )
        .unwrap();

        #[cfg(debug_assertions)]
        const COUNT: usize = 10usize;
        #[cfg(not(debug_assertions))]
        const COUNT: usize = 1000usize;

        //let init = Instant::now();
        let msgs = (0..COUNT).map(|i| {
            Message::new(
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
        });

        //let prev = Instant::now();
        store
            .append_many_sorted(msgs.into_iter(), |_, _| Ok(()), true)
            .unwrap();

        let mut all_children_of_0 = vec![];
        for i in 1..COUNT {
            all_children_of_0.push(MessageId::new_debug(i as u32))
        }
        assert_eq!(
            store.get_children_of(MessageId::new_debug(0)).unwrap(),
            all_children_of_0
        );

        //let prev = Instant::now();
        let msg = store
            .read_message(MessageId::new_debug(2))
            .unwrap()
            .unwrap();
        assert_eq!(msg.payload.id, 2);
        assert_eq!(msg.payload.data, [42, 42, 42, 42]);
    }

    #[test]
    fn add_and_read_messages_twice() {
        let mut store = OnDiskMessageStore::new(
            &mut StandardDisk,
            &Target::CreateNewOrOverwrite("test/add_and_read_messages_twice3.bin".into()),
            10000,
        )
        .unwrap();

        const COUNT: usize = 5; //1_000_000usize;

        //let init = Instant::now();
        let msgs = (0..COUNT).map(|i| {
            Message::new(
                MessageId::new_debug(2 * i as u32),
                vec![],
                OnDiskMessage {
                    id: 2 * i as u64,
                    seq: 2 * i as u64,
                    data: vec![42u8; 4],
                },
            )
        });

        store
            .append_many_sorted(msgs.into_iter(), |_, _| Ok(()), true)
            .unwrap();
        let msgs = (0..COUNT).map(|i| {
            Message::new(
                MessageId::new_debug(2 * i as u32 + 1),
                vec![],
                OnDiskMessage {
                    id: 2 * i as u64 + 1,
                    seq: 2 * i as u64 + 1,
                    data: vec![43u8; 4],
                },
            )
        });

        store
            .append_many_sorted(msgs.into_iter(), |_, _| Ok(()), true)
            .unwrap();

        let msg = store
            .read_message(MessageId::new_debug(2))
            .unwrap()
            .unwrap();
        assert_eq!(msg.payload.id, 2);
        assert_eq!(msg.payload.data, [42, 42, 42, 42]);
    }
    #[test]
    fn add_and_delete_messages() {
        let target = Target::CreateNewOrOverwrite("test/add_and_delete_messages4.bin".into());
        let mut store = OnDiskMessageStore::new(&mut StandardDisk, &target, 10000).unwrap();

        let mut head_tracker = UpdateHeadTracker::new(&mut StandardDisk, &target).unwrap();
        const COUNT: usize = 3;

        //let init = Instant::now();
        let msgs = (0..COUNT).map(|i| {
            Message::new(
                MessageId::new_debug(i as u32),
                vec![],
                OnDiskMessage {
                    id: i as u64,
                    seq: i as u64,
                    data: vec![42u8; 4],
                },
            )
        });

        store
            .append_many_sorted(msgs.into_iter(), |_, _| Ok(()), true)
            .unwrap();

        store
            .delete_many(
                [
                    MessageId::new_debug(0),
                    MessageId::new_debug(1),
                    MessageId::new_debug(2),
                ]
                .into_iter(),
                &mut head_tracker,
            )
            .unwrap();

        assert_eq!(
            store
                .query_greater(MessageId::new_debug(0))
                .unwrap()
                .count(),
            0
        );

        store.compact_to_target(0).unwrap();
    }

    #[test]
    fn test_create_disk_store() {
        let mut store = OnDiskMessageStore::new(
            &mut StandardDisk,
            &Target::CreateNewOrOverwrite("test/test_create_disk_store.bin".into()),
            10000,
        )
        .unwrap();

        const COUNT: usize = 1_000; //1_000_000usize;

        let init = Instant::now();
        let msgs = (0..COUNT).map(|i| {
            Message::new(
                MessageId::new_debug(i as u32),
                vec![],
                OnDiskMessage {
                    id: (1_000_000 + i) as u64,
                    seq: i as u64,
                    data: vec![42u8; 1024],
                },
            )
        });

        store
            .append_many_sorted(msgs.into_iter(), |_, _| Ok(()), true)
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
        println!("Count: {} (sum: {})", count, sum);
    }
    #[test]
    fn test_create_disk_store_in_memory() {
        let mut store = OnDiskMessageStore::new(
            &mut InMemoryDisk::default(),
            &Target::CreateNewOrOverwrite("test/test_create_disk_store.bin".into()),
            10000,
        )
        .unwrap();

        const COUNT: usize = 1; //1_000_000usize;

        let init = Instant::now();
        let msgs = (0..COUNT).map(|i| {
            Message::new(
                MessageId::new_debug(i as u32),
                vec![],
                OnDiskMessage {
                    id: (1_000_000 + i) as u64,
                    seq: i as u64,
                    data: vec![42u8; 4],
                },
            )
        });

        store
            .append_many_sorted(msgs.into_iter(), |_, _| Ok(()), true)
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
        println!("Count: {} (sum: {})", count, sum);
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
            return temp;
        }

        assert_eq!(dedup(&[]), &[]);
        assert_eq!(dedup(&[1]), &[1]);
        assert_eq!(dedup(&[1,2]), &[1,2]);
        assert_eq!(dedup(&[1,1]), &[1]);
        assert_eq!(dedup(&[1,1,1]), &[1]);
        assert_eq!(dedup(&[1,2,2]), &[1,2]);
        assert_eq!(dedup(&[1,2,2,3]), &[1,2,3]);
        assert_eq!(dedup(&[1,2,2,3,3,3]), &[1,2,3]);

        assert_eq!(dedup(&[1,2,2,3,3,3,5]), &[1,2,3,5]);
        assert_eq!(dedup(&[1,1,1,2,2,3,3,3,5]), &[1,2,3,5]);

    }
}
