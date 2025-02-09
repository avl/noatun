use crate::cutoff::{Acceptability, CutOffConfig, CutOffHashPos, CutOffState, CutOffTime, CutoffHash};
use crate::disk_abstraction::Disk;
use crate::disk_access::FileAccessor;
use crate::sequence_nr::SequenceNr;
use crate::sha2_helper::sha2;
use crate::{Database, Message, MessageHeader, MessageId, MessagePayload, NoatunTime, Target};
use anyhow::{anyhow, bail, Context, Result};
use bytemuck::{Pod, Zeroable};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use fs2::FileExt;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem::{offset_of, MaybeUninit};
use std::ops::Index;
use std::path::Path;

#[derive(Debug, Clone, Copy, Pod, Zeroable, PartialEq, Eq)]
#[repr(transparent)]
pub struct FileOffset(u64);

/// Size of header for each individual message in store
const MSG_HEADER_SIZE: usize = size_of::<FileHeaderEntry>();

const HASH_SIZE: usize = 16;

struct EmbVecAccessor<T> {
    handle: T,
    len_offset: usize,
    capacity_offset: usize,
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
    pub fn new_children(mut handle: T, offset: u64) -> Result<Self> {
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
            .seek(SeekFrom::Start((self.len_offset) as u64))?;
        Ok(self
            .handle
            .write_u16::<LittleEndian>(val.try_into().unwrap())?)
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
                self.set_len(count - 1);
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
            self.set_len(self_len + 1);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct FileHeaderEntry {
    sha2: [u8; HASH_SIZE],
    payload_size: u64,
    message_id: MessageId,
    has_been_transmitted: u8,
    padding1: u8,
    padding2: u16,
    /// Size of message payload (without this header), nor parents
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
    pub fn size_before_payload(&self) -> usize {
        size_of::<FileHeaderEntry>()
            + (self.num_parents.num_allocated as usize + self.num_children.num_allocated as usize)
                * size_of::<MessageId>()
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
        ((self.0 as usize + 1) % 2)
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
    fn is_deleted(self) -> bool {
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

    /// MessageId's that aren't parents to any other message
    update_heads: FileAccessor,
}

struct MessageWriteReport {
    start_pos: u64,
    total_size: u64,
}
#[derive(Default)]
struct RemainingChildAssignments {
    parent2child: HashMap<MessageId, Vec<MessageId>>,
}

//compile_error!("When a message is deleted, make sure to delete it from the parent-list of all its children!")
impl<M> OnDiskMessageStore<M> {
    // TODO: This doesn't need to return full Message-instances. It could just return
    // message-id + parents
    pub fn get_upstream_of(
        &self,
        message_ids: impl DoubleEndedIterator<Item = (MessageId, /*query count*/ usize)>,
    ) -> Result<impl Iterator<Item = (MessageHeader, usize /*query count*/)> + '_>
    where
        M: MessagePayload,
    {
        let (header, message_index) = self.header_and_index().context("opening index file")?;
        let data_files = &self.data_files;

        let mut index = 0;

        let mut breadth = HashMap::new();
        #[cfg(debug_assertions)]
        let mut prev = None;

        let mut first = true;
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
                            //TODO: Log
                            eprintln!("Failed to parse message: {:?}", err);
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

        let slice = map.map_mut();
        let header: &mut StoreHeader =
            bytemuck::from_bytes_mut(&mut slice[0..size_of::<StoreHeader>()]);
        // We're currently updating, so status should be 'NOK'. But we haven't failed,
        // so as soon as the transaction is complete, this will be set to OK again.

        Ok(())
    }

    pub fn count_messages(&self) -> Result<usize> {
        Ok(self.header_and_index()?.0.entries as usize)
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
        let (header, index_entries) = self.header_and_index()?;

        match index_entries.get(index) {
            Some(found) => Ok(!found.file_offset.is_deleted()),
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
        cutoff_config: &CutOffConfig
    ) -> Result<()>
    where
        M: MessagePayload,
    {
        self.index_mmap.truncate(0)?;

        let mut max_count_messages = 0usize;
        for file in self.data_files.iter_mut() {
            let length = file.file.seek(SeekFrom::End(0))?;
            file.file.seek(SeekFrom::Start(0))?;

            while file.file.stream_position()? < length {
                let header = file
                    .file
                    .read_pod::<FileHeaderEntry>()
                    .context("reading file header during recovery")?;
                // TODO: Probably have an option to salvage what can be salvaged,
                // rather than outright failing if we can't parse everything.
                file.file.seek(SeekFrom::Current(
                    (header.total_size() - size_of::<FileHeaderEntry>()) as i64,
                ))?;
                max_count_messages += 1;
            }
        }

        self.update_heads.fast_truncate(0);

        let (index_header, index) =
            Self::header_and_index_mut_uninit(&mut self.index_mmap, max_count_messages)?;
        assert_eq!(index_header.entries, 0);
        let mut index_ptr = 0usize;

        for (file_info, file_entry) in self
            .data_files
            .iter_mut()
            .zip(index_header.data_files.iter_mut())
        {
            file_info.file.seek(SeekFrom::End(0))?;
            let file_length = file_info.file.stream_position()?;
            file_entry.nominal_size = file_length;
            file_entry.compaction_pointer = 0;
            file_info.file.seek(SeekFrom::Start(0))?;

            loop {
                let file_offset = file_info.file.stream_position()?;
                //println!("In main loop {}", file_offset);
                if file_offset == file_length {
                    break;
                }
                let header = file_info.file.read_pod::<FileHeaderEntry>()?;
                let tot_size = header.total_size();

                let mut parvec = EmbVecAccessor::new_parents(&mut file_info.file, file_offset);
                let parents = parvec.all()?;

                file_info.file.seek(SeekFrom::Start(
                    file_offset + header.size_before_payload() as u64,
                ))?;

                let msg: Result<M> = file_info
                    .file
                    .with_bytes(header.payload_size as usize, |bytes| M::deserialize(bytes))?;
                let msg = match msg {
                    Ok(msg) => msg,
                    Err(err) => {
                        eprintln!("Message could not be recovered: {:?}", err);
                        continue;
                    }
                };

                file_info.file.with_bytes_at(
                    file_offset as usize + HASH_SIZE,
                    tot_size - HASH_SIZE,
                    |bytes| {
                        if sha2(bytes) != header.sha2 {
                            return Err(anyhow!("data corrupted on disk - checksum mismatch"));
                        }
                        Ok(())
                    },
                )?;

                file_info
                    .file
                    .seek(SeekFrom::Start(file_offset + tot_size as u64))?;

                report_inserted(header.message_id, &parents)?;

                file_entry.bytes_used += tot_size as u64;
                index[index_ptr] = IndexEntry {
                    message: header.message_id,
                    file_offset: FileOffset::new(file_offset, file_info.file_number),
                    file_total_size: tot_size as u64,
                };
                index_ptr += 1;
                index_header.entries += 1;
            }
        }

        index[0..index_header.entries as usize].sort();

        Self::recover_cutoff_state(now, index_header, index, cutoff_config)?;
        Ok(())
    }

    fn recover_cutoff_state(time_now: NoatunTime, index_header: &mut StoreHeader, index: &[IndexEntry], config: &CutOffConfig) -> Result<()> {
        let cutoff_time = config.nominal_cutoff(CutOffTime::from_noatun_time(time_now));
        index_header.cutoff.before_time = cutoff_time;
        index_header.cutoff.hash = CutoffHash::ZERO;
        let cutoff_time_noatun : NoatunTime = cutoff_time.into();
        for item in index.iter() {
            if item.message.timestamp() >= cutoff_time_noatun {
                break;
            }
            index_header.cutoff.apply(item.message, "recovery");
        }

        Ok(())
    }

    pub fn contains_message(&self, start: MessageId) -> Result<bool> {
        let (header, message_index) = self.header_and_index().context("opening index file")?;
        match message_index.binary_search_by_key(&start, |x| x.message) {
            Ok(index) => Ok(!message_index[index].file_offset.is_deleted()),
            Err(err) => Ok(false),
        }
    }
    /// Get all messages with id start or greater
    fn query_greater(&self, start: MessageId) -> Result<impl Iterator<Item = &IndexEntry>> {
        let (header, message_index) = self.header_and_index().context("opening index file")?;

        let (Ok(index) | Err(index)) = message_index.binary_search_by_key(&start, |x| x.message);

        Ok(message_index[index..]
            .iter()
            .filter(|x| !x.file_offset.is_deleted()))
    }

    pub fn next_index(&self) -> Result<usize> {
        let (header, message_index) = self.header_and_index().context("opening index file")?;
        Ok(header.entries as usize)
    }

    /// Get all messages with index `index` or greater
    pub fn query_by_index(
        &self,
        index: usize,
    ) -> Result<impl Iterator<Item = (usize /*seqnr/index*/, Message<M>)> + '_>
    where
        M: MessagePayload,
    {
        let (header, message_index) = self.header_and_index().context("opening index file")?;

        let mut data_files = &self.data_files;
        Ok(message_index[index..]
            .iter()
            .enumerate()
            .filter(|(_sub_index, x)| !x.file_offset.is_deleted())
            .map(move |(sub_index, x)| (index + sub_index, Self::read_msg(x, data_files, None)))
            .filter_map(|(index, x)| {
                match x {
                    Ok(x) => Some((index, x)),
                    Err(x) => {
                        eprintln!("Failed to load message #{}: {:?}", index, x);
                        //TODO: Log
                        None
                    }
                }
            }))
    }

    /// Return the position where a message with the given ID could be inserted, or None if the message already exists.
    pub fn get_insert_point(&self, msg: MessageId) -> Result<Option<usize>> {
        let (header, message_index) = self.header_and_index().context("opening index file")?;

        if let (Err(index)) = message_index.binary_search_by_key(&msg, |x| x.message) {
            Ok(Some(index))
        } else {
            // Already exists
            Ok(None)
        }
    }

    pub(crate) fn may_have_been_transmitted(&self, msg: SequenceNr) -> Result<bool> {
        let (header, message_index) = self.header_and_index().context("opening index file")?;

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

        file.seek(SeekFrom::Start(offset + HASH_SIZE as u64))?;
        file.with_bytes(header.total_size_except_hash(), |bytes| {
            if sha2(bytes) != header.sha2 {
                return Err("Data corruption detected!");
            }
            Ok(())
        })?;

        file.seek(SeekFrom::Start(
            offset + header.size_before_payload() as u64,
        ))?;

        let mut msg_payload =
            file.with_bytes(header.payload_size as usize, |bytes| M::deserialize(bytes))??;

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
        let mut file = &mut data_files[file.index()].file;
        let mut header: &mut FileHeaderEntry = file.access_pod_mut(offset as usize)?;

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
        let (header, message_index) = self.header_and_index().context("Reading index file")?;

        let data_files = &self.data_files;
        match message_index.binary_search_by_key(&id, |x| x.message) {
            Ok(index) => {
                let entry = &message_index[index];
                let msg = Self::read_msg(entry, data_files, None)?;
                Ok(Some(msg))
            }
            Err(index) => Ok(None),
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
        let (header, message_index) = self.header_and_index().context("Reading index file")?;

        let data_files = &self.data_files;
        match message_index.binary_search_by_key(&id, |x| x.message) {
            Ok(index) => {
                let entry = &message_index[index];
                let mut children = vec![];
                let msg = Self::read_msg(entry, data_files, Some(&mut children))?;
                Ok(Some((msg, children)))
            }
            Err(index) => Ok(None),
        }
    }
    /// Return the given message, or None if it does not exist.
    pub fn read_message_header_and_children_by_index(
        &self,
        index: SequenceNr,
    ) -> Result<Option<(MessageHeader, Vec<MessageId>)>> {
        let (header, message_index) = self.header_and_index().context("Reading index file")?;

        let data_files = &self.data_files;
        let entry = &message_index[index.index()];
        let mut children = vec![];
        let msg = Self::read_msg_header(entry, data_files, Some(&mut children))?;
        Ok(Some((msg, children)))
    }
    /// Return the given message, or None if it does not exist.
    pub fn read_message_header_and_children(
        &self,
        id: MessageId,
    ) -> Result<Option<(MessageHeader, Vec<MessageId>)>> {
        let (header, message_index) = self.header_and_index().context("Reading index file")?;

        let data_files = &self.data_files;
        match message_index.binary_search_by_key(&id, |x| x.message) {
            Ok(index) => {
                let entry = &message_index[index];
                let mut children = vec![];
                let msg = Self::read_msg_header(entry, data_files, Some(&mut children))?;
                Ok(Some((msg, children)))
            }
            Err(index) => Ok(None),
        }
    }
    /// Return the index of the given message, if it exists, otherwise None.
    /// If the call itself fails, returns Err.
    pub fn get_index_of(&mut self, id: MessageId) -> Result<Option<usize>> {
        let (header, message_index) = self.header_and_index().context("opening index file")?;

        match message_index.binary_search_by_key(&id, |x| x.message) {
            Ok(index) => Ok(Some(index)),
            Err(index) => Ok(None),
        }
    }

    /// Return message with the given id.
    /// If no such message exist, return the index of the next message with a larger id.
    /// If the given id is larger than the largest MessageId, return the number of messages.
    pub fn get_insertion_point(&self, id: MessageId) -> Result<usize> {
        let (header, message_index) = self.header_and_index().context("opening index file")?;
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
    pub fn get_index_after(&self, time: NoatunTime) -> Result<SequenceNr> {
        let (header, message_index) = self.header_and_index().context("opening index file")?;
        let needle = MessageId::from_parts_raw(time.0, [0;10])?;
        match message_index.binary_search_by_key(&needle, |x| x.message) {
            Ok(needle_index) => {
                Ok(SequenceNr::from_index(needle_index))
            }
            Err(index) => {
                Ok(SequenceNr::from_index(index))
            }
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
    // TODO: Consider if there's a risk of inefficiency when two severely desynced instances meet.
    // They might have completely different messages, even before the cutoff time.
    // When they start bringing each other up-to-date, their before-cutoff timestamp will change.
    // Will this cause them to restart the sync-process? Make sure it doesn't.
    pub fn mark_deleted_by_index(&mut self, delete_index: SequenceNr) -> Result<()>
    where
        M: MessagePayload,
    {
        let Some((msg, children)) = self.read_message_header_and_children_by_index(delete_index)?
        else {
            eprintln!("Message was already deleted");
            return Ok(());
        };

        let (header, message_index) =
            Self::header_and_index_mut(&mut self.index_mmap).context("Reading index file")?;

        let Some(entry) = message_index.get_mut(delete_index.index()) else {
            //TODO: Trace log
            return Ok(());
        };

        if let Some((file, offset)) = entry.file_offset.file_and_offset() {
            header.cutoff.report_delete(entry.message);

            let id = entry.message;
            header.data_files[file.index()].bytes_used -= entry.file_total_size;

            // TODO: Make sure recovery procedure can handle duplicate messages
            Self::delete_msg_from_file(entry, &mut self.data_files)?;

            let parents = msg.parents;
            for parent in &parents {
                self.add_remove_parents_and_children(*parent, &[], None, &children, Some(id))?;
            }

            for child in &children {
                self.add_remove_parents_and_children(*child, &parents, Some(id), &[], None)?;
            }
        } else {
            eprintln!("Message was already deleted");
        }

        Ok(())
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
        let (header, message_index) =
            Self::header_and_index_mut(&mut self.index_mmap).context("Reading index file")?;

        let data_files = &mut self.data_files;
        match message_index.get(index) {
            Some(entry) => {
                let msg = Self::read_msg(entry, data_files, None)?;
                Ok(Some(msg))
            }
            None => Err(anyhow!("Invalid message index")),
        }
    }

    pub fn mark_transmitted(&mut self, message_id: MessageId) -> Result<()> {
        let (header, message_index) =
            Self::header_and_index_mut(&mut self.index_mmap).context("Reading index file")?;
        let Ok(index) = message_index.binary_search_by_key(&message_id, |x| x.message) else {
            // TODO: Trace log?
            return Ok(());
        };
        let index_entry = &message_index[index];

        if let Some((file, offset)) = index_entry.file_offset.file_and_offset() {
            let file_header: &mut FileHeaderEntry = self.data_files[file.index()]
                .file
                .access_pod_mut(offset as usize)?;
            file_header.has_been_transmitted = 1;
        } else {
            // Deleted
        }
        Ok(())
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
    /// Returns the index of the first insertion point, or None if all messages already existed.
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
    pub fn delete_many(&mut self, messages: impl Iterator<Item = MessageId>) -> Result<()>
    where
        M: MessagePayload,
    {
        self.do_delete_many(messages)
    }

    fn do_delete_many(&mut self, messages: impl Iterator<Item = MessageId>) -> Result<()>
    where
        M: MessagePayload,
    {
        for message in messages {
            let (header, message_index) =
                Self::header_and_index_mut(&mut self.index_mmap).context("Reading index file")?;

            let Ok(index) = message_index.binary_search_by_key(&message, |x| x.message) else {
                continue;
            };
            self.mark_deleted_by_index(SequenceNr::from_index(index))?;
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
        Self::provide_index_map(&mut self.index_mmap, count);
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
        let start_pos = file.seek(SeekFrom::End(0))?;

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
        msg.payload.serialize(&mut *file);

        let end_pos = file.stream_position()?;
        let payload_size = end_pos - pre_payload_pos;
        let size_with_padding =
            (end_pos - start_pos).next_multiple_of(align_of::<FileHeaderEntry>() as u64);
        let padding = size_with_padding - (end_pos - start_pos);
        file.write_zeroes(padding as usize)?;

        let msg_total_size = size_with_padding;

        let sha2 = file.with_bytes_at(
            start_pos as usize + HASH_SIZE,
            msg_total_size as usize - HASH_SIZE,
            sha2,
        )?;
        // Write data store header
        file.seek(SeekFrom::Start(start_pos))?;
        let header = FileHeaderEntry {
            message_id: msg.id(),
            sha2,
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

        let file = &mut files[file_index.index()].file;
        file.seek(SeekFrom::Start(end_pos))?;

        Ok(MessageWriteReport {
            start_pos,
            total_size: msg_total_size as u64,
        })
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
        let (header, search_index) = Self::header_and_index_mut(&mut self.index_mmap)?;
        let Ok(index) = search_index.binary_search_by_key(&id, |x| x.message) else {
            eprintln!("Message did not exist");
            return Ok(());
        };
        if let Some((file, offset)) = search_index[index].file_offset.file_and_offset() {
            let file = &mut self.data_files[file.index()].file;
            let header: &FileHeaderEntry = file.access_pod(offset as usize)?;
            if header.num_parents.free() < new_parents.len()
                || header.num_children.free() < new_children.len()
            {
                println!("Taking slow path");
                self.slow_add_parents_and_children(id, new_parents, new_children);
                return Ok(());
            }
            let mut parents = EmbVecAccessor::new_parents(&mut *file, offset);
            if let Some(removed_parent) = removed_parent {
                parents.remove(removed_parent);
            }
            parents.extend(new_parents.iter());

            let mut children = EmbVecAccessor::new_children(&mut *file, offset)?;
            if let Some(removed_child) = removed_child {
                children.remove(removed_child);
            }
            children.extend(new_children.iter());
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
            eprintln!("Message not found");
            return Ok(());
        };

        // TODO: Maybe create an optimized header_and_index_mut that has a fast path
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

    fn do_append_many_sorted(
        &mut self,
        mut messages: impl ExactSizeIterator<Item = Message<M>>,
        mut message_inserted: impl FnMut(MessageId, &[MessageId] /*parents*/) -> Result<()>,
        local: bool,
    ) -> Result<Option<usize>>
    where
        M: MessagePayload + Debug,
    {
        let len = messages.len();

        if len == 0 {
            return Ok(None);
        }
        let info = &mut self.data_files[self.active_file.index()];
        let initial_file_position = info.file.stream_position()?;

        let (index_header, index) = Self::header_and_index_mut_uninit(&mut self.index_mmap, len)?;
        /*println!("Inserting into:");
        for i in index.iter() {
            println!("Index: {}", i.message);
        }*/

        let mut last_msg_id = None;
        let mut carry_buffer: VecDeque<IndexEntry> = VecDeque::with_capacity(len);
        let mut input_stream_pos = 0;

        let first_input_message: Message<M> = (messages.next().unwrap());

        let initial_index_entries = index_header.entries as usize;
        let (Ok(first_index) | Err(first_index)) = index[0..index_header.entries as usize]
            .binary_search_by_key(&first_input_message.id(), |x| x.message);
        let mut cur_index = first_index;
        let mut first_index_actually_inserted = None;

        let mut parents: Vec<MessageId> = vec![];
            //println!("Start of loop. cur_index ={}, first input message: {:?}", cur_index, first_input_message.id());

        let mut cur_input_message = Some(first_input_message);
        let mut actual_inserted_entries = 0;

        let mut remaining_child_assignments = RemainingChildAssignments::default();

        loop {
            /*if let Some(cur_input_message) = &cur_input_message {
                println!("Start of loop, cur input msg: {:?}", cur_input_message.header.id);
            }*/
            if carry_buffer.is_empty() && cur_input_message.is_none() {
                break;
            }

            let info = &mut self.data_files[self.active_file.index()];

            //dbg!(&cur_input_message, &minne);
            let cur_index_entry = &mut index[cur_index];
            let present_id = if cur_index < index_header.entries as usize {
                Some(cur_index_entry.message)
            } else {
                None
            };

            #[derive(Clone, Copy, Debug)]
            #[allow(clippy::enum_variant_names)]
            enum Cases {
                /// The next value that should be written to the output is the one that is already
                /// present. I.e, a no-op. If multiple match, this matches first.
                NextFromCurrent,
                /// The next that should be written is the current input message
                NextFromInput,
                /// The next should be from 'carry' - i.e, we're compacting
                NextFromCarry,
            }

            let cases = [
                (Cases::NextFromCurrent, present_id),
                (
                    Cases::NextFromInput,
                    cur_input_message.as_ref().map(|x| x.id()),
                ),
                (
                    Cases::NextFromCarry,
                    carry_buffer.front().map(|x| x.message),
                ),
            ];
            let (now_case, now_message_id) = cases
                .iter()
                .filter_map(|(case, val)| val.map(|x| (*case, x)))
                .min_by_key(|x| x.1)
                .expect("There must always be some case present");

            match now_case {
                Cases::NextFromCurrent => {
                    if present_id == cur_input_message.as_ref().map(|x| x.id()) {
                        cur_input_message = messages.next();
                        cur_index += 1;
                        eprintln!("Duplicate id detected");
                        continue;
                    }
                    // We get here if a previous item was a duplicate. There's no copying to be done
                    cur_index += 1;
                    continue;
                    /*dbg!(&cur_input_message, &present_id,&carry_buffer,&cases,&now_case, &now_message_id);
                    unreachable!("This shouldn't, logically, happen");*/
                }
                Cases::NextFromInput => {
                    let msg = cur_input_message.take().unwrap();

                    index_header.cutoff.report_add(msg.header.id);
                    message_inserted(msg.id(), &msg.header.parents)?;
                    if first_index_actually_inserted.is_none() {
                        first_index_actually_inserted = Some(cur_index);
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

                    let cur_index_entry = &mut index[cur_index];
                    // Append to the index (compacting while doing so)
                    if cur_index < index_header.entries as usize {
                        carry_buffer.push_back(*cur_index_entry);
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
                    actual_inserted_entries += 1;
                }
                Cases::NextFromCarry => {
                    let temp = carry_buffer.pop_front().unwrap();
                    if cur_index < initial_index_entries {
                        carry_buffer.push_front(*cur_index_entry);
                    }
                    *cur_index_entry = temp;
                    cur_index += 1;
                }
            }
        }

        let info = &mut self.data_files[self.active_file.index()];
        let final_file_position = info.file.stream_position()?;
        info.file
            .flush_range(
                initial_file_position as usize,
                (final_file_position - initial_file_position) as usize,
            )
            .context("flush file to disk")?;

        let data_file = &mut index_header.data_files[self.active_file.index()];

        index_header.entries += actual_inserted_entries as u32;

        self.handle_remaining_child_assignments(remaining_child_assignments);

        Ok(first_index_actually_inserted)
    }

    /// Compact all files to the given maximum degree of fragmentation.
    /// NOTE! Never call this method with a value smaller than 25. This can lead
    /// to abysmal performance. If disk space permits, consider allowing up to 50% fragmentation.
    fn compact_to_target(&mut self, tolerated_fragmentation_percent: u32) -> Result<()>
    where
        M: MessagePayload,
    {
        self.do_compact(|wasted_space, total_space| {
            (100 * wasted_space) / total_space > tolerated_fragmentation_percent as u64
        })
    }

    /// Compact all files
    fn compact(&mut self) -> Result<()>
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
        mut done: impl FnMut(/*wasted bytes:*/ u64, /*total bytes: */ u64) -> bool,
    ) -> Result<()> {
        let completed_one_compaction = self.do_compact_impl(&mut done)?;
        if completed_one_compaction {
            self.do_compact_impl(done)?;
        }

        Ok(())
    }
    /// Returns true if 'active' has been swapped
    fn do_compact_impl(
        &mut self,
        mut done: impl FnMut(/*wasted bytes:*/ u64, /*total bytes:*/ u64) -> bool,
    ) -> Result<bool> {
        let (header, index) = Self::header_and_index_mut(&mut self.index_mmap)?;

        let active_file = self.active_file;
        let (src_file_info, dst_file_info) = Self::get_src_dst(&mut self.data_files, active_file);
        let (src_file_entry, dst_file_entry) =
            Self::get_src_dst(&mut header.data_files, active_file);

        let mut initial_compacted_bytes = src_file_entry.compaction_pointer;
        loop {
            let mut src_wasted = src_file_entry.nominal_size - src_file_entry.bytes_used;
            let mut dst_wasted = dst_file_entry.nominal_size - dst_file_entry.bytes_used;
            let mut src_tot = src_file_entry.nominal_size;
            let mut dst_tot = dst_file_entry.nominal_size;

            let src_position = src_file_entry.compaction_pointer;
            if src_file_entry.compaction_pointer >= src_file_entry.nominal_size {
                dst_file_info.file.flush_all()?;

                src_file_entry.nominal_size = 0;
                src_file_entry.bytes_used = 0;
                src_file_entry.compaction_pointer = 0;
                src_file_info.file.flush_all()?;

                // Don't actually swap if current active isn't even fragmented at all,
                // or if target is reached
                if dst_file_entry.bytes_used != dst_file_entry.nominal_size
                    && !done(src_wasted + dst_wasted, src_tot + dst_tot)
                {
                    self.active_file.swap();
                    return Ok(true);
                }
                return Ok(false);
            }

            if done(src_wasted + dst_wasted, src_tot + dst_tot) {
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

            src_file_info
                .file
                .copy_to(full_size as usize, &mut dst_file_info.file)?;
            dst_file_entry.bytes_used += full_size;
            dst_file_entry.nominal_size += full_size;
        }
    }

    pub fn new<D: Disk>(
        mut d: &mut D,
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

        let data_files: [Result<DataFileInfo>; 2] = [0, 1].map(|file_number| {
            let mut data_file = d
                .open_file(target, &format!("data{}", file_number), 0, max_file_size)
                .context("Opening data-file")?;

            data_file
                .try_lock_exclusive()
                .context("While obtaining file lock on data file")?;

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

        let mut overwrite = target.overwrite();

        let mut index_file = d
            .open_file(target, "index", size_of::<StoreHeader>(), max_file_size)
            .context("Opening index-file")?;
        index_file
            .try_lock_exclusive()
            .context("While obtaining lock on index-file")?;

        let len = index_file.used_space();

        let index = Self::header(&mut index_file)?;

        // The active file is the one with the least percentage of wasted bytes
        let active = index
            .data_files
            .iter()
            .enumerate()
            .min_by_key(|(index, data_file)| {
                (100 * ((data_file.nominal_size - data_file.bytes_used) as u128))
                    / (data_file.nominal_size.max(1) as u128)
            })
            .unwrap()
            .0;
        let mut update_heads = d.open_file(target, "update_heads", 0, 128 * 1024 * 1024)?;

        let mut this = Self {
            target: target.clone(),
            index_mmap: index_file,
            data_files,
            active_file: U1::from_index(active),
            phantom: Default::default(),
            update_heads,
        };

        Ok(this)
    }


    pub fn current_cutoff_hash(&self) -> Result<CutOffHashPos> { //TODO: Name? Current is superfluous?
        let (header, _index) = self.header_and_index()?;
        Ok(header.cutoff)
    }

    pub(crate) fn get_messages_slice(&self) -> Result<&[IndexEntry]> {
        let (header, index) = self.header_and_index()?;
        Ok(&index[0..header.entries as usize])
    }

    pub(crate) fn get_messages_at_or_after(
        &self,
        message: MessageId,
        count: usize,
    ) -> Result<Vec<MessageId>> {
        let (header, search_index) = self.header_and_index()?;
        let (Ok(index) | Err(index)) = search_index.binary_search_by_key(&message, |x| x.message);
        let mut result = Vec::with_capacity(count);
        for message in search_index[index..].iter().take(count) {
            result.push(message.message);
        }
        Ok(result)
    }

    pub fn is_acceptable_cutoff_hash(&self, now: NoatunTime, hash: CutOffHashPos, cutoff_config: &CutOffConfig) -> Result<Acceptability> {
        let (header, _index) = self.header_and_index()?;
        Ok(header.cutoff.is_acceptable_cutoff_hash(now, hash, cutoff_config))
    }
}

#[cfg(test)]
mod tests {
    use crate::disk_abstraction::{InMemoryDisk, StandardDisk};
    use crate::message_store::{FileOffset, OnDiskMessageStore, STATUS_NOK, STATUS_OK, U1};
    use crate::{
        DatabaseContextData, DummyUnitObject, Message, MessageId, MessagePayload, NoatunTime,
        Target,
    };
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use std::io::{Cursor, Read, Write};
    use std::path::Path;
    use std::pin::Pin;
    use std::time::Instant;
    use crate::cutoff::CutOffConfig;

    #[derive(Debug)]
    struct OnDiskMessage {
        id: u64,
        seq: u64,
        data: Vec<u8>,
    }

    impl MessagePayload for OnDiskMessage {
        type Root = DummyUnitObject;

        fn apply(&self, time: NoatunTime, root: Pin<&mut Self::Root>) {
            unimplemented!()
        }

        fn deserialize(buf: &[u8]) -> anyhow::Result<Self>
        where
            Self: Sized,
        {
            let mut cursor = Cursor::new(buf);
            let id = cursor.read_u64::<LittleEndian>()?;
            let seq = cursor.read_u64::<LittleEndian>()?;
            let data_len = cursor.read_u64::<LittleEndian>()?;
            let mut data = vec![0; 4];
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

        let init = Instant::now();
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
            let header =
                OnDiskMessageStore::<OnDiskMessage>::header(&mut store.index_mmap).unwrap();
            //header.dirty_status = STATUS_NOK;
        }
        store.recover(|_, _| Ok(()), NoatunTime::ZERO, &CutOffConfig::default()).unwrap();

        let msg = store
            .read_message(MessageId::new_debug(2))
            .expect("io should work")
            .expect("a message should be found");
        assert_eq!(msg.payload.id, 2);
    }

    #[test]
    fn add_and_read_messages() {
        let mut store = OnDiskMessageStore::new(
            &mut StandardDisk,
            &Target::CreateNewOrOverwrite("test/add_and_read_messages.bin".into()),
            5_000_000_000,
        )
        .unwrap();

        #[cfg(debug_assertions)]
        const COUNT: usize = 10usize;
        #[cfg(not(debug_assertions))]
        const COUNT: usize = 1000usize;

        let init = Instant::now();
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

        let prev = Instant::now();
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

        let prev = Instant::now();
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
            &Target::CreateNewOrOverwrite("test/add_and_read_messages_twice.bin".into()),
            10000,
        )
        .unwrap();

        const COUNT: usize = 5; //1_000_000usize;

        let init = Instant::now();
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
        let mut store = OnDiskMessageStore::new(
            &mut StandardDisk,
            &Target::CreateNewOrOverwrite("test/add_and_delete_messages.bin".into()),
            10000,
        )
        .unwrap();

        const COUNT: usize = 3;

        let init = Instant::now();
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
}
