use crate::{Message, MessageId, Target, sha2};
use anyhow::{Context, Result, anyhow, bail};
use buf_read_write::BufStream;
use bytemuck::{Pod, Zeroable};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem::{offset_of, MaybeUninit};
use std::path::{Path};
use fs2::FileExt;
use crate::disk_abstraction::Disk;
use crate::disk_abstraction::DiskMmap;
use crate::disk_abstraction::DiskFile;

#[derive(Debug, Clone, Copy, Pod, Zeroable, PartialEq, Eq)]
#[repr(transparent)]
struct FileOffset(u64);

const BUFSTREAM_CAPACITY: usize = 1024*256;

/// Size of header for each individual message in store
const MSG_HEADER_SIZE: usize = size_of::<FileHeaderEntry>();

#[derive(Debug,Clone,Copy,Pod,Zeroable)]
#[repr(C)]
struct FileHeaderEntry {
    message_id: MessageId,
    sha2: [u8; 16],
    /// Size of message payload (without this header)
    msg_size: u64,
}

impl FileHeaderEntry {
    pub fn is_deleted(&self) -> bool {
        self. sha2 == [0u8; 16]
    }
}

pub trait ReadPod {
    const N:usize;
    fn read_pod<T:Pod>(&mut self) -> Result<T>;
}

impl<R:Read> ReadPod for R {
    const N:usize = size_of::<R>();
    fn read_pod<T: Pod>(&mut self) -> Result<T> {
        let mut zeroed = T::zeroed();
        let bytes = bytemuck::bytes_of_mut(&mut zeroed);
        self.read_exact(bytes)?;
        Ok(zeroed)
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
        if self.0==1 {
            1
        } else {
            0
        }
    }
    fn swap(&mut self) {
        *self = Self::from_index(self.other());
    }
    fn other(self) -> usize {
        ((self.0 as usize+1)%2)
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
    fn from_bits(bits:u64) -> FileOffset {
        Self(bits)
    }

    fn new(offset: u64, file: U1) -> Self {
        assert!(offset < u64::MAX/2); //Note, must <, not <=, to avoid magic value u64::MAX
        Self((offset<<1) | file.index() as u64)
    }
    fn deleted() -> Self {
        Self(u64::MAX)
    }
    fn is_deleted(self) -> bool {
        self.0 == u64::MAX
    }
    fn offset(self) -> Option<u64> {
        (self.0 != u64::MAX).then(|| self.0>>1)
    }
    fn file_and_offset(self) -> Option<(U1,u64)> {
        (self.0 != u64::MAX).then(||
                                      (
                                          if self.0 &1 == 1 {
                                              U1::ONE
                                          } else {
                                              U1::ZERO
                                          },
                                          self.0>>1
                                          )
        )
    }
    fn file(self) -> Option<U1> {
        (self.0 != u64::MAX).then(||
            if self.0 &1 == 1 {
                U1::ONE
            } else {
                U1::ZERO
            }
        )
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
struct IndexEntry {
    message: MessageId,
    /// Offset into the logical file-area, or deletion-marker
    file_offset: FileOffset,
    /// This is the size without the header - payload only
    file_size: u64,
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
    /// The size of this file.
    ///
    nominal_size: u64,
    /// The number of bytes actually used.
    /// If this becomes small, compaction may be in order
    /// This figure includes the file header (and any padding we might want to add later).
    /// This means that an unfragmented file has `bytes_used` == `nominal_size`
    bytes_used: u64,

    /// All files beyond this offset have been removed
    compaction_pointer: u64,
}

#[derive(Debug)]
struct DataFileInfo<F:Seek+Write+Read> {
    file: BufStream<F>,
    file_number: U1,
}

#[repr(C, u8)]
enum WriteLogKind {
    CompactFile(u32),
}

struct WriteLogEntry {
    kind: WriteLogKind,
    checksum: u64,
}

const STATUS_OK: u32 = 1;
/// Not ok
const STATUS_NOK: u32 = 0;

#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
struct StoreHeader {
    dirty_status: u32,
    entries: u32,
    // Compact file=1 + old file being compacted=0
    data_files: [DataFileEntry; 2],
}





struct OnDiskMessageStore<M, D:Disk> {
    target: Target,
    index_file: D::File,
    index_mmap: Option<D::Mmap>,
    data_files: [DataFileInfo<D::File>;2],
    active_file: U1,
    phantom: PhantomData<M>,
    d: D,
}

impl<D:DiskFile> DataFileInfo<D> {}

impl<M, D:Disk> OnDiskMessageStore<M, D> {

    fn provide_index_map<'a>(
        d: &mut D,
        map: &'a mut Option<D::Mmap>,
        file: &mut D::File,
        extra: usize,
    ) -> Result<&'a mut D::Mmap> {
        let cur_available = map.as_mut().map(|x| {
            let xlen = x.len();
            let slice: &mut [u8] = x.map_mut();
            let header: &StoreHeader = bytemuck::from_bytes(&slice[0..size_of::<StoreHeader>()]);
            let file_capacity =
                (xlen.saturating_sub(size_of::<StoreHeader>())) / size_of::<IndexEntry>();
            let used = header.entries;
            (file_capacity, used as usize, x)
        });


        match cur_available {
            Some((file_capacity, used, _map)) if file_capacity.saturating_sub(used) >= extra => {
                // Can't return _map here only because of borrow checker limitations in this case
                return Ok(map.as_mut().unwrap());
            }
            Some((file_capacity, _, _)) if file_capacity >= u32::MAX as usize => {
                return Err(anyhow!("Database max-size reached (~2^32 entries)"));
            }
            t => {
                let base_capacity = t.map(|x| (x.0+extra)).unwrap_or(extra.max(1024));
                *map = None;
                // Need to realloc
                let mut new_entries = base_capacity.saturating_mul(2);
                if new_entries > (u32::MAX / 2 + u32::MAX / 4) as usize {
                    new_entries = u32::MAX as usize;
                }
                let new_file_size = (new_entries as u64) * (size_of::<IndexEntry>() as u64)
                    + (size_of::<StoreHeader>() as u64);

                file.set_len(new_file_size).with_context(||format!("resizing index file to {}", new_file_size))?;

                // Make sure to sync metadata.
                file.sync_all().with_context(||format!("syncing index file to {}", new_file_size))?;

                let mut mmap = unsafe { d.mmap(file)? };
                let slice = mmap.map_mut();
                let header: &mut StoreHeader = bytemuck::from_bytes_mut(&mut slice[0..size_of::<StoreHeader>()]);
                // We're currently updating, so status should be 'NOK'. But we haven't failed,
                // so as soon as the transaction is complete, this will be set to OK again.
                header.dirty_status = STATUS_NOK;

                Ok(map.insert(mmap))
            }
        }
    }

    #[inline]
    fn header<'a>(d: &mut D, map: &'a mut Option<D::Mmap>, file: &mut D::File) -> Result<&'a mut StoreHeader> {
        let mmap = Self::provide_index_map(d, map, file, 0)?;

        let slice: &mut [u8] = mmap.map_mut();
        let header: &mut StoreHeader = bytemuck::from_bytes_mut(&mut slice[0..size_of::<StoreHeader>()]);
        Ok(header)
    }

    /// NOTE! This returns also unallocated message-entries, for writing
    #[inline]
    fn header_and_index_mut_uninit<'a>(
        d: &mut D,
        map: &'a mut Option<D::Mmap>,
        file: &mut D::File,
        extra: usize
    ) -> Result<(&'a mut StoreHeader, &'a mut [IndexEntry])> {
        let mmap = Self::provide_index_map(d, map, file, extra)?;
        let slice: &mut [u8] = mmap.map_mut();
        let (store_header_bytes, index_bytes) = slice.split_at_mut(size_of::<StoreHeader>());
        let header: &mut StoreHeader = bytemuck::from_bytes_mut(store_header_bytes);
        let rest = bytemuck::cast_slice_mut(index_bytes);
        Ok((header, rest))
    }
    #[inline]
    fn header_and_index_mut<'a>(
        d: &mut D,
        map: &'a mut Option<D::Mmap>,
        file: &mut D::File,
    ) -> Result<(&'a mut StoreHeader, &'a mut [IndexEntry])> {
        let (store,index) = Self::header_and_index_mut_uninit(d, map, file, 0)?;

        let entries = store.entries as usize;
        Ok((store, &mut index[..entries]))
    }
    /// NOTE! This does not return unwritten message entries. This is because
    /// since it doesn't support writing, there's no point in returning unwritten entries.
    #[inline]
    fn header_and_index(
        &self,
    ) -> Result<(&StoreHeader, &[IndexEntry])> {
        let Some(mmap) = &self.index_mmap else {
            return Err(anyhow!("index is corrupt/not found"));
        };
        let slice: &[u8] = mmap.map();
        let (store_header_bytes, index_bytes) = slice.split_at(size_of::<StoreHeader>());
        let header: &StoreHeader = bytemuck::from_bytes(store_header_bytes);
        let rest = bytemuck::cast_slice(index_bytes);
        Ok((header, &rest[0..header.entries as usize]))
    }

    fn with_transaction(&mut self, mut f: impl FnOnce(&mut Self) -> Result<()>) -> Result<()> where M:Message {

        if self.update_status(STATUS_NOK)? != STATUS_OK {
            // Recovery needed
            self.recover()?;
        }
        //TODO: Make sure we actually inspect the status when reading db!
        let ret = f(self);
        self.update_status(STATUS_OK)
            .expect("failed to mark db as OK after transaction complete");
        ret
    }

    fn end_transaction(&mut self) -> Result<()> {
        self.update_status(STATUS_OK)?;
        Ok(())
    }

    /// Recover from index corruption.
    /// You should never need to call this explicitly, it will be called automatically
    /// whenever a transaction is attempted and database is in a corrupted state
    pub fn recover(&mut self) -> Result<()> where M:Message {
        self.index_mmap = None;
        self.index_file.set_len(0)?;

        let mut max_count_messages = 0usize;
        for file in self.data_files.iter_mut() {
            let length = file.file.seek(SeekFrom::End(0))?;
            file.file.seek(SeekFrom::Start(0))?;

            while file.file.stream_position()? < length {
                let header = file.file.read_pod::<FileHeaderEntry>().context("reading file header during recovery")?;
                file.file.seek(SeekFrom::Current(header.msg_size as i64))?;
                max_count_messages += 1;
            }
        }

        let (index_header, index) = Self::header_and_index_mut_uninit(&mut self.d, &mut self.index_mmap, &mut self.index_file, max_count_messages)?;
        assert_eq!(index_header.entries, 0);
        let mut index_ptr = 0usize;
        for (file_info, file_entry) in self.data_files.iter_mut().zip(index_header.data_files.iter_mut()) {

            file_info.file.seek(SeekFrom::End(0))?;
            let file_length =  file_info.file.stream_position()?;
            file_entry.nominal_size = file_length;
            file_entry.compaction_pointer = 0;
            file_info.file.seek(SeekFrom::Start(0))?;

            loop {
                let file_offset = file_info.file.stream_position()?;
                dbg!(file_offset, file_length);
                if file_offset == file_length {
                    break;
                }
                let header = file_info.file.read_pod::<FileHeaderEntry>()?;

                let msg: Result<M> = file_info.file.with_bytes(header.msg_size as usize, |bytes|{
                    if sha2(bytes) != header.sha2 {
                        return Err(anyhow!("data corrupted on disk - checksum mismatch"));
                    }
                    M::deserialize(bytes)
                })?;
                let msg = match msg {
                    Ok(msg) => msg,
                    Err(err) => {
                        eprintln!("Message could not be recovered: {:?}", err);
                        continue;
                    }
                };
                file_entry.bytes_used += header.msg_size + MSG_HEADER_SIZE as u64;
                index[index_ptr] = IndexEntry {
                    message: header.message_id,
                    file_offset: FileOffset::new(file_offset, file_info.file_number),
                    file_size: header.msg_size,
                };
                index_ptr += 1;
                index_header.entries += 1;
            }
        }

        index[0..index_header.entries as usize].sort();

        self.update_status(STATUS_OK)?;

        Ok(())
    }


    fn update_status(&mut self, status: u32) -> Result<u32> {
        let header = Self::header(&mut self.d, &mut self.index_mmap, &mut self.index_file)?;
        let prev = header.dirty_status;
        if prev != status {
            header.dirty_status = status;
            self.index_mmap.as_mut().unwrap().flush_range(
                offset_of!(StoreHeader, dirty_status),
                std::mem::size_of::<u32>(),
            )?;
        }
        Ok(prev)
    }

    pub fn query(&self, start: MessageId) -> Result<impl Iterator<Item=&IndexEntry>> {
        let (header, message_index) = self.header_and_index().context("opening index file")?;

        let (Ok(index)|Err(index)) = message_index.binary_search_by_key(&start, |x|x.message);

        Ok(message_index[index..].iter().filter(|x|!x.file_offset.is_deleted()))
    }


    /// Return the given message, or None if it does not exist.
    /// If the call itself fails, returns Err.
    pub fn read_message(&mut self, id: MessageId) -> Result<Option<M>> where M: Message {
        let (header, message_index) = self.header_and_index().context("opening index file")?;

        match message_index.binary_search_by_key(&id, |x|x.message) {
            Ok(index) => {
                let entry = &message_index[index];
                let Some((file, offset)) = entry.file_offset.file_and_offset() else {
                    return Err(anyhow!("Message not found in store"));
                };
                let mut file = &mut self.data_files[file.index()].file;
                file.seek(SeekFrom::Start(offset)).context("Seeking to message data")?;
                let header: FileHeaderEntry = file.read_pod()?;

                let msg = file.with_bytes(header.msg_size as usize, |bytes|{
                    if sha2(bytes) != header.sha2 {
                        return Err(anyhow!("Message has been corrupted on disk"));
                    }
                    M::deserialize(bytes)
                })??;

                Ok(Some(msg))
            }
            Err(index) => {
                Ok(None)
            }
        }
    }


    /// Add the given messages.
    /// Note: You should call 'begin_transaction' before this, to mark db as dirty.
    ///
    /// This will determine the position they need to be added to, and will then
    /// merge with the index. Marks the db clean.
    pub fn append_many_sorted(&mut self, messages: impl ExactSizeIterator<Item=M>) -> Result<()> where M:Message+Debug{
        self.with_transaction(|tself| tself.do_append_many_sorted(messages))
    }

    /// Marks the db as dirty
    /// Find all the given messages, and mark their entries in their data files as unused.
    /// Use heuristics to determine if files should be compacted.
    /// Compact files (moving to higher file if suitable).
    /// Delete from the index.
    /// Mark the db as clean.
    /// Messages that are not found are simply ignored. The operation does not stop or fail.
    pub fn delete_many(&mut self, messages: impl Iterator<Item=MessageId>) -> Result<()> where M:Message {
        self.with_transaction(|tself|
            tself.do_delete_many(messages)
        )
    }

    fn do_delete_many(&mut self, messages: impl Iterator<Item=MessageId>) -> Result<()> {
        let (header, message_index) = Self::header_and_index_mut(&mut self.d, &mut self.index_mmap, &mut self.index_file)
            .context("Reading index file")?;

        for message in messages {

            let Ok(index) = message_index.binary_search_by_key(&message, |x|x.message) else {
                continue;
            };
            let message_entry = &mut message_index[index];

            if let Some((file, offset)) = message_entry.file_offset.file_and_offset() {
                let store_file_entry = &mut self.data_files[file.index()];
                message_entry.file_offset = FileOffset::deleted();
                debug_assert!(message_entry.file_offset.is_deleted());
                let index_entry = &mut header.data_files[file.index()];

                store_file_entry.file.seek(SeekFrom::Start(offset+8)).context("seeking file")?;
                let size = store_file_entry.file.read_u64::<LittleEndian>()?;
                store_file_entry.file.write_zeroes(16)?;
                store_file_entry.file.flush()?;
                index_entry.bytes_used -= message_entry.file_size + MSG_HEADER_SIZE as u64;
            }
        }
        Ok(())
    }

    /// Make sure the given number of slots are available.
    fn provide_index_map_for(&mut self, count: usize) -> Result<()> {
        Self::provide_index_map(&mut self.d, &mut self.index_mmap, &mut self.index_file, count);
        Ok(())
    }

    fn do_append_many_sorted(&mut self, mut messages: impl ExactSizeIterator<Item=M>) -> Result<()> where M: Message+Debug{
        let len = messages.len();

        if len == 0 {
            return Ok(());
        }
        let info = &mut self.data_files[self.active_file.index()];

        let (index_header, index) = Self::header_and_index_mut_uninit(&mut self.d, &mut self.index_mmap, &mut self.index_file, len)?;



        let mut size_delta = 0usize;
        let mut last_msg_id = None;
        let mut minne: VecDeque<IndexEntry> = VecDeque::with_capacity(len);
        let mut input_stream_pos = 0;

        let first_input_message: M = (messages.next().unwrap());
        let (Ok(first_index)|Err(first_index)) = index[0..index_header.entries as usize].binary_search_by_key(&first_input_message.id(), |x|x.message);
        let mut cur_index = first_index;

        let mut cur_input_message = Some(first_input_message);

        loop {

            if minne.is_empty() && cur_input_message.is_none() {
                break;
            }
            //dbg!(&cur_input_message, &minne);
            let cur_index_entry = &mut index[cur_index];
            let present_id = if cur_index < index_header.entries as usize {Some(cur_index_entry.message)} else {None};


            #[derive(Clone,Copy,Debug)]
            enum Cases {
                NextFromCurrent,
                NextFromInput,
                NextFromMinne,
            }

            let cases = [
                (Cases::NextFromCurrent, present_id),
                (Cases::NextFromInput, cur_input_message.as_ref().map(|x|x.id())),
                (Cases::NextFromMinne, minne.front().map(|x|x.message))
            ];
            let (now_case, now_message_id) = cases.iter().filter_map(|(case,val)|val.map(|x|(*case,x))).min_by_key(|x|x.1)
                .expect("There must always be some case present");

            println!("now case: {:?}, id: {:?}", now_case , now_message_id);

            match now_case {
                Cases::NextFromCurrent => {
                    unreachable!("This shouldn't, logically, happen");
                }
                Cases::NextFromInput => {
                    let msg = cur_input_message.take().unwrap();

                    let start_pos = info.file.stream_position()?;

                    if let Some(last_msg_id) = last_msg_id {
                        if last_msg_id >= msg.id() {
                            bail!("MessageId was not applied in correct order.");
                        }
                    }
                    last_msg_id = Some(msg.id());


                    // Serialize the message and write to data store
                    info.file.write_zeroes(MSG_HEADER_SIZE)?;
                    msg.serialize(&mut info.file);

                    let end_pos = info.file.stream_position()?;
                    size_delta += (end_pos - start_pos) as usize;
                    let msg_size = end_pos - start_pos - MSG_HEADER_SIZE as u64;
                    info.file.seek(SeekFrom::Start(start_pos+ MSG_HEADER_SIZE as u64))?;

                    let sha2 = info
                        .file
                        .with_bytes(msg_size as usize, |bytes| {
                            let temp = sha2(bytes);
                            temp
                        })?;
                    // Write data store header
                    info.file.seek(SeekFrom::Start(start_pos))?;
                    info.file.write_all(
                        bytemuck::bytes_of(
                            &FileHeaderEntry {
                                message_id: msg.id(),
                                sha2,
                                msg_size,
                            }
                        )
                    )?;
                    info.file.seek(SeekFrom::Start(end_pos))?;



                    // Append to the index (compacting while doing so)
                    if cur_index < index_header.entries as usize {
                        minne.push_back(*cur_index_entry);
                    }
                    *cur_index_entry = IndexEntry {
                        message: msg.id(),
                        file_offset: FileOffset::new(start_pos, self.active_file),
                        file_size: msg_size,
                    };
                    cur_index += 1;
                    let full_msg_size = msg_size + MSG_HEADER_SIZE as u64;

                    index_header.data_files[self.active_file.index()].bytes_used += full_msg_size;
                    index_header.data_files[self.active_file.index()].nominal_size += full_msg_size;

                    // This is just a little perf improvement, that
                    // reduces the risk of messages being split into more than one buffer,
                    // thus causing unnecessary seeks and writes.
                    info.file.flush_if(BUFSTREAM_CAPACITY/2, false);


                    cur_input_message = messages.next();
                }
                Cases::NextFromMinne => {
                    *cur_index_entry = minne.pop_front().unwrap();
                    println!("Assigning new item from input to pos {}", cur_index);
                    cur_index += 1;
                }
            }
        }
        info.file.inner_unchecked_mut().sync_all()?;
        let data_file = &mut index_header.data_files[self.active_file.index()];
        data_file.bytes_used += size_delta as u64;
        index_header.entries += len as u32;


        Ok(())
    }


    /// Compact all files to the given maximum degree of fragmentation.
    /// NOTE! Never call this method with a value smaller than 25. This can lead
    /// to abysmal performance. If disk space permits, consider allowing up to 50% fragmentation.
    fn compact_to_target(&mut self, tolerated_fragmentation_percent: u32) -> Result<()> where M:Message{
        self.with_transaction(|tself|
            tself.do_compact(
                |wasted_space, total_space|
                    (100*wasted_space)/total_space > tolerated_fragmentation_percent as u64
            ))
    }

    /// Compact all files
    fn compact(&mut self) -> Result<()> where M:Message{
        self.compact_to_target(25)
    }

    fn get_src_dst<T>(entries: &mut [T;2], active: U1) -> (&mut T, &mut T) {
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

    fn do_compact(&mut self, mut done: impl FnMut(/*wasted bytes:*/u64, /*total bytes: */ u64) -> bool) -> Result<()> {
        let completed_one_compaction = self.do_compact_impl(&mut done)?;
        if completed_one_compaction {
            self.do_compact_impl(done)?;
        }

        Ok(())
    }
    /// Returns true if 'active' has been swapped
    fn do_compact_impl(&mut self, mut done: impl FnMut(/*wasted bytes:*/u64, /*total bytes:*/u64) -> bool) -> Result<bool> {
        let (header, index) = Self::header_and_index_mut(&mut self.d, &mut self.index_mmap, &mut self.index_file)?;

        let active_file = self.active_file;
        let (src_file_info, dst_file_info) = Self::get_src_dst(&mut self.data_files, active_file);
        let (src_file_entry, dst_file_entry) = Self::get_src_dst(&mut header.data_files, active_file);


        let mut initial_compacted_bytes = src_file_entry.compaction_pointer;
        loop {


            let mut src_wasted = src_file_entry.nominal_size - src_file_entry.bytes_used;
            let mut dst_wasted = dst_file_entry.nominal_size - dst_file_entry.bytes_used;
            let mut src_tot = src_file_entry.nominal_size;
            let mut dst_tot = dst_file_entry.nominal_size;

            let src_position = src_file_entry.compaction_pointer;
            if src_file_entry.compaction_pointer >= src_file_entry.nominal_size {
                dst_file_info.file.inner_unchecked_mut().sync_all()?;
                src_file_entry.nominal_size=0;
                src_file_entry.bytes_used=0;
                src_file_entry.compaction_pointer=0;
                src_file_info.file.flush()?;
                src_file_info.file.inner_unchecked_mut().set_len(0)?;

                // Don't actually swap if current active isn't even fragmented at all,
                // or if target is reached
                if dst_file_entry.bytes_used != dst_file_entry.nominal_size
                    && !done(src_wasted + dst_wasted, src_tot+dst_tot) {
                    self.active_file.swap();
                    return Ok(true);
                }
                return Ok(false);
            }

            if done(src_wasted + dst_wasted, src_tot+dst_tot) {
                return Ok(false);
            }

            src_file_info.file.seek(SeekFrom::Start(src_position))?;
            let header = src_file_info.file.read_pod::<FileHeaderEntry>()?;
            let full_size = header.msg_size + MSG_HEADER_SIZE as u64;

            let new_offset = dst_file_entry.nominal_size;
            dst_file_info.file.seek(SeekFrom::Start(dst_file_entry.nominal_size))?;

            if header.is_deleted() {
                src_file_entry.compaction_pointer += full_size;
                continue;
            }


            if let Ok(msg_in_index) = index.binary_search_by_key(&header.message_id, |x|x.message) {
                let index_entry = &mut index[msg_in_index];
                if index_entry.file_offset.is_deleted() {
                    src_file_entry.compaction_pointer += full_size;
                    continue;
                }
                index_entry.file_offset = FileOffset::new(new_offset, active_file);
            }

            src_file_info.file.copy_to(full_size as usize, &mut dst_file_info.file)?;
            dst_file_entry.bytes_used += full_size;
            dst_file_entry.nominal_size += full_size;

        }
    }

    pub fn new(mut d: D, target: Target) -> Result<OnDiskMessageStore<M, D>> {
        const {
            if size_of::<usize>() < size_of::<u32>() {
                panic!(
                    "This platform has a usize that is smaller than 4 bytes. This is not supported"
                );
            }
        }



        let data_files: [Result<DataFileInfo<D::File>>;2] = [0,1].map(|file_number|{
            let data_file_path = target
                .path()
                .join(format!("data{}.bin", file_number));

            compile_error!("USe disk-abstraction!");
            let mut data_file = d.open_file( &data_file_path, target.create(), target.overwrite()).context("Opening data-file")?;
            /*OpenOptions::new()
                .read(true)
                .write(true)
                .create(target.create())
                .truncate(target.overwrite())*/
                //.open(&data_file_path).context("Opening data-file")?;


            data_file.try_lock_exclusive().context("While obtaining file lock on data file")?;

            Ok(DataFileInfo {
                file: BufStream::with_capacity(data_file, BUFSTREAM_CAPACITY)?,
                file_number: U1::from_index(file_number),
            })
        });

        //TODO: Use array `try_map` once stabilized
        let data_files: [DataFileInfo<D::File>;2] = data_files.into_iter().collect::<Result<Vec<DataFileInfo<D::File>>>>()?.try_into().unwrap_or_else(|_|unreachable!());


        let index_file_path = target.path().join("index.bin");
        let mut overwrite = target.overwrite();

        let mut index_file = d.open_file(&index_file_path, target.create(), overwrite).context("Opening index-file")?;
        /*let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(target.create())
            .truncate(overwrite)
            .open(&index_file_path)
            .context("Opening index-file")
            ?;*/

        index_file.try_lock_exclusive().context("While obtaining lock on index-file")?;


        let len = index_file.len().context("reading index file length");

        let mut index_mmap = None;
        let index = Self::header(&mut d, &mut index_mmap, &mut index_file)?;
        if overwrite {
            // We've overwritten (truncated) the existing file, so it's definitely not corrupt
            index.dirty_status = STATUS_OK;
        }

        // The active file is the one with the least percentage of wasted bytes
        let active = index.data_files.iter().enumerate().min_by_key(|(index,data_file)|{
            let wasted_bytes_percent = (100*((data_file.nominal_size - data_file.bytes_used) as u128))/(data_file.nominal_size.max(1) as u128);
            wasted_bytes_percent
        }).unwrap().0;

        let mut this = Self {
            d,
            target: target.clone(),
            index_file,
            index_mmap,
            data_files,
            active_file: U1::from_index(active),
            phantom: Default::default(),
        };

        Ok(this)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};
    use std::path::Path;
    use std::time::Instant;
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use crate::on_disk_message_store::{FileOffset, OnDiskMessageStore, STATUS_NOK, STATUS_OK, U1};
    use crate::{DatabaseContext, DummyUnitObject, Message, MessageId, Target};
    use crate::disk_abstraction::{InMemoryDisk, StandardDisk};

    #[derive(Debug)]
    struct OnDiskMessage {
        id: u64,
        seq: u64,
        data: Vec<u8>,
    }

    impl Message for OnDiskMessage {
        type Root = DummyUnitObject;

        fn id(&self) -> MessageId {
            assert!(self.id < u32::MAX as u64);
            MessageId::new_debug(self.id as u32)
        }

        fn parents(&self) -> impl ExactSizeIterator<Item=MessageId> {
            std::iter::empty()
        }

        fn apply(&self, context: &mut DatabaseContext, root: &mut Self::Root) {
            todo!()
        }

        fn deserialize(buf: &[u8]) -> anyhow::Result<Self>
        where
            Self: Sized
        {
            let mut cursor = Cursor::new(buf);
            let id = cursor.read_u64::<LittleEndian>()?;
            let seq = cursor.read_u64::<LittleEndian>()?;
            let data_len = cursor.read_u64::<LittleEndian>()?;
            let mut data = Vec::new();
            data.resize(4,0);
            cursor.read_exact(&mut data)?;
            Ok(OnDiskMessage {
                id,
                seq,
                data,
            })
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
            StandardDisk,
            Target::CreateNewOrOverwrite("add_and_recover_messages.bin".into())).unwrap();

        const COUNT: usize = 5; //1_000_000usize;

        let init = Instant::now();
        let msgs = (0..COUNT).map(|i| OnDiskMessage {
            id: i as u64,
            seq: i as u64,
            data: vec![42u8; 4],
        });

        store.append_many_sorted(msgs.into_iter()).unwrap();

        {
            let header = OnDiskMessageStore::<OnDiskMessage, StandardDisk>::header(&mut store.d, &mut store.index_mmap, &mut store.index_file).unwrap();
            header.dirty_status = STATUS_NOK;
        }
        store.recover().unwrap();

        let msg = store.read_message(MessageId::new_debug(2)).expect("io should work").expect("a message should be found");
        assert_eq!(msg.id, 2);

    }


    #[test]
    fn add_and_read_messages() {
        let mut store = OnDiskMessageStore::new(
            StandardDisk,Target::CreateNewOrOverwrite("add_and_read_messages.bin".into())).unwrap();

        const COUNT: usize = 5; //1_000_000usize;

        let init = Instant::now();
        let msgs = (0..COUNT).map(|i| OnDiskMessage {
            id: i as u64,
            seq: i as u64,
            data: vec![42u8; 4],
        });

        store.append_many_sorted(msgs.into_iter()).unwrap();

        let msg = store.read_message(MessageId::new_debug(2)).unwrap().unwrap();
        assert_eq!(msg.id, 2);
        assert_eq!(msg.data, [42,42,42,42]);
    }
    #[test]
    fn add_and_read_messages_twice() {
        let mut store = OnDiskMessageStore::new(
            StandardDisk,Target::CreateNewOrOverwrite("add_and_read_messages_twice.bin".into())).unwrap();

        const COUNT: usize = 5; //1_000_000usize;

        let init = Instant::now();
        let msgs = (0..COUNT).map(|i| OnDiskMessage {
            id: 2*i as u64,
            seq: 2*i as u64,
            data: vec![42u8; 4],
        });

        store.append_many_sorted(msgs.into_iter()).unwrap();
        let msgs = (0..COUNT).map(|i| OnDiskMessage {
            id: 2*i as u64+1,
            seq: 2*i as u64+1,
            data: vec![43u8; 4],
        });

        store.append_many_sorted(msgs.into_iter()).unwrap();

        let msg = store.read_message(MessageId::new_debug(2)).unwrap().unwrap();
        assert_eq!(msg.id, 2);
        assert_eq!(msg.data, [42,42,42,42]);
    }
    #[test]
    fn add_and_delete_messages() {
        let mut store = OnDiskMessageStore::new(
            StandardDisk,Target::CreateNewOrOverwrite("add_and_delete_messages.bin".into())).unwrap();

        const COUNT: usize = 3;

        let init = Instant::now();
        let msgs = (0..COUNT).map(|i| OnDiskMessage {
            id: i as u64,
            seq: i as u64,
            data: vec![42u8; 4],
        });

        store.append_many_sorted(msgs.into_iter()).unwrap();

        store.delete_many([
            MessageId::new_debug(0),
            MessageId::new_debug(1),
            MessageId::new_debug(2),
        ].into_iter()).unwrap();

        assert_eq!(
            store.query(MessageId::new_debug(0)).unwrap().count(),
            0);

        store.compact_to_target(0).unwrap();

    }



    #[test]
    fn test_create_disk_store() {

        let mut store = OnDiskMessageStore::new(
            StandardDisk,Target::CreateNewOrOverwrite("test_create_disk_store.bin".into())).unwrap();

        const COUNT:usize = 1_000;//1_000_000usize;

        let init = Instant::now();
        let msgs = (0..COUNT).map(|i|OnDiskMessage {
            id: (1000_000 + i) as u64,
            seq: i as u64,
            data: vec![42u8; 1024],
        });

        store.append_many_sorted(msgs.into_iter()).unwrap();

        println!("Init: {:?}", init.elapsed());
        let mut count = 0;
        let mut sum = 0;
        let start = Instant::now();
        for msg in store.query(MessageId::new_debug(1999_900)).unwrap() {
            sum += msg.message.data[0] as u64;
            count += 1;
        }
        println!("Instrumentation: {:?}", store.data_files[0].file.instrumentation());
        println!("Time to iterate: {:?}", start.elapsed());
        println!("Count: {} (sum: {})", count, sum);

    }
    #[test]
    fn test_create_disk_store_in_memory() {

        let mut store = OnDiskMessageStore::new(
            InMemoryDisk::default(),Target::CreateNewOrOverwrite("test_create_disk_store.bin".into())).unwrap();

        const COUNT:usize = 1_000;//1_000_000usize;

        let init = Instant::now();
        let msgs = (0..COUNT).map(|i|OnDiskMessage {
            id: (1000_000 + i) as u64,
            seq: i as u64,
            data: vec![42u8; 1024],
        });

        store.append_many_sorted(msgs.into_iter()).unwrap();

        println!("Init: {:?}", init.elapsed());
        let mut count = 0;
        let mut sum = 0;
        let start = Instant::now();
        for msg in store.query(MessageId::new_debug(1999_900)).unwrap() {
            sum += msg.message.data[0] as u64;
            count += 1;
        }
        println!("Instrumentation: {:?}", store.data_files[0].file.instrumentation());
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
        roundtrip(FileOffset::new(u64::MAX/2-1, U1::ONE));
        roundtrip(FileOffset::new(u64::MAX/2-1, U1::ZERO));
    }


/*
    #[test]
    fn test_create_sqlite_disk_store() {
        if std::path::Path::exists(Path::new("test.bin")) {
            std::fs::remove_file("test.bin").unwrap();
        }
        let mut conn = Connection::open("test.bin").unwrap();

        conn.execute(
            "CREATE TABLE message (
            id    INTEGER PRIMARY KEY,
            seq INTEGER,
            data  BLOB
        );
        ",
            (), // empty list of parameters.
        )
        .unwrap();

        /*
                conn.execute(
                    "
            CREATE INDEX message_idx1 ON message (id);
            ",
                    (), // empty list of parameters.
                ).unwrap();
        */

        let trans = conn.transaction().unwrap();
        let bef_insert = Instant::now();
        for i in 0..1000_000 {
            let me = OnDiskMessage {
                id: 1000_000 + i,
                seq: i,
                data: vec![42u8; 1024],
            };
            trans
                .execute(
                    "INSERT INTO message (id, seq, data) VALUES (?1, ?2, ?3)",
                    (&me.id, i, me.data),
                )
                .unwrap();
        }
        println!("Inserting 1 million: {:?}", bef_insert.elapsed());
        trans.commit().unwrap();

        let bef_query = Instant::now();


        let mut stmt = conn
            .prepare(
                "
                SELECT seq FROM message
                 where id >= ?1",
            )
            .unwrap();
        let person_iter = stmt
            .query_map([1_000_000 + 999_900], |row| {
                let count: usize = row.get(0)?;

                Ok(())
            })
            .unwrap();

        let mut idsum = 0;
        for person in person_iter {
            //println!("got: {:?}", person.unwrap());
            idsum += 1;
        }
        println!("Query {:?} (ids: {})", bef_query.elapsed(), idsum);
    }
 */
}
