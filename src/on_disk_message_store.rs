use crate::{Message, MessageId, Target, sha2};
use anyhow::{Context, Result, anyhow};
use buf_read_write::BufStream;
use bytemuck::{Pod, Zeroable};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use memmap2::{Mmap, MmapMut};
use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem::offset_of;
use std::path::Path;

#[derive(Debug, Clone, Copy, Pod, Zeroable, PartialEq, Eq)]
#[repr(transparent)]
struct FileOffset(u64);

const BUFSTREAM_CAPACITY: usize = 1024*256;


#[derive(Debug, Clone, Copy, Pod, Zeroable, PartialEq, Eq)]
#[repr(transparent)]
struct U1(u8);
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
struct MessageEntry {
    message: MessageId,
    /// Offset into the logical file-area, or deletion-marker
    file_offset: FileOffset,
    file_size: u64,
}

const DEFAULT_MAX_COUNT: usize = 1024;
const DEFAULT_MAX_SIZE_BYTES: usize = DEFAULT_MAX_COUNT * std::mem::size_of::<MessageEntry>();

impl PartialEq for MessageEntry {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
    }
}
impl Eq for MessageEntry {}
impl PartialOrd for MessageEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MessageEntry {
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
    bytes_used: u64,
}

struct DataFileInfo {
    file: BufStream<File>,
    //mapping: MmapMut,
    /// Where in the db data stream this file starts
    /// Note: Adding data is always possible at the end of the last file.
    /// Moving stuff to files higher in the chain is easy, just update
    /// the [`DataFileEntry::size`] field.
    /// When compacting, the main index needs to be rewritten.
    offset: u64,
    /// The length of this file.
    length: u64,
    /// The number of bytes that are actually used
    bytes_used: u64,
    file_number: U1,
    active: bool,

    /// How far into this file we've completed compaction/reaping.
    /// When the whole file has been copied to the compact file, it is removed, and a new
    /// compact file created.
    reaping_progress: u64,
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

struct OnDiskMessageStore<M> {
    target: Target,
    index_file: File,
    index_mmap: Option<MmapMut>,
    data_files: Vec<DataFileInfo>,
    phantom: PhantomData<M>,
}

impl DataFileInfo {}

impl<M> OnDiskMessageStore<M> {
    fn provide_entry_for_writing<'a>(
        data_files: &'a mut Vec<DataFileInfo>,
        target: &Target,
    ) -> Result<&'a mut DataFileInfo> {
        if data_files.is_empty() {
            std::fs::create_dir_all(&target.path).context("create database directory")?;

            let new_file_number = 0;
            let data_file_path = target
                .path
                .join(format!("data{}.bin", new_file_number));

            let data_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(target.overwrite)
                .open(&data_file_path).context("Opening data-file")?;

            data_files.push(DataFileInfo {
                file: BufStream::with_capacity(data_file, BUFSTREAM_CAPACITY)?,
                //mapping: (),
                offset: 0,
                length: 0,
                bytes_used: 0,
                file_number: U1::ZERO,
                active: true,
                reaping_progress: 0,
            });
        }
        Ok(data_files.last_mut().unwrap())
    }

    fn provide_index_map<'a>(
        map: &'a mut Option<MmapMut>,
        file: &File,
        extra: usize,
    ) -> Result<&'a mut MmapMut> {
        let cur_available = map.as_mut().map(|x| {
            let xlen = x.len();
            let slice: &mut [u8] = x;
            let header: &StoreHeader = bytemuck::from_bytes_mut(&mut slice[0..size_of::<StoreHeader>()]);
            let file_capacity =
                (xlen.saturating_sub(size_of::<StoreHeader>())) / size_of::<MessageEntry>();
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
                let new_file_size = (new_entries as u64) * (size_of::<MessageEntry>() as u64)
                    + (size_of::<StoreHeader>() as u64);

                file.set_len(new_file_size).with_context(||format!("resizing index file to {}", new_file_size))?;

                // Make sure to sync metadata.
                file.sync_all().with_context(||format!("syncing index file to {}", new_file_size))?;

                let mmap = unsafe { MmapMut::map_mut(file)? };
                let slice = &mmap[..];
                assert!((slice.as_ptr() as usize) % 256 == 0);

                Ok(map.insert(mmap))
            }
        }
    }

    #[inline]
    fn header<'a>(map: &'a mut Option<MmapMut>, file: &File) -> Result<&'a mut StoreHeader> {
        let mmap = Self::provide_index_map(map, file, 0)?;

        let slice: &mut [u8] = mmap;
        let header: &mut StoreHeader = bytemuck::from_bytes_mut(&mut slice[0..size_of::<StoreHeader>()]);
        Ok(header)
    }

    /// NOTE! This returns also unallocated message-entries, for writing
    #[inline]
    fn header_and_index_mut<'a>(
        map: &'a mut Option<MmapMut>,
        file: &File,
    ) -> Result<(&'a mut StoreHeader, &'a mut [MessageEntry])> {
        let mmap = Self::provide_index_map(map, file, 0)?;
        let slice: &mut [u8] = &mut *mmap;
        let (store_header_bytes, index_bytes) = slice.split_at_mut(size_of::<StoreHeader>());
        let header: &mut StoreHeader = bytemuck::from_bytes_mut(store_header_bytes);
        let rest = bytemuck::cast_slice_mut(index_bytes);
        Ok((header, rest))
    }

    /// NOTE! This does not return unwritten message entries. This is because
    /// since it doesn't support writing, there's no point in returning unwritten entries.
    #[inline]
    fn header_and_index(
        &self,
    ) -> Result<(&StoreHeader, &[MessageEntry])> {
        let Some(mmap) = &self.index_mmap else {
            return Err(anyhow!("index is corrupt/not found"));
        };
        let slice: &[u8] = mmap;
        let (store_header_bytes, index_bytes) = slice.split_at(size_of::<StoreHeader>());
        let header: &StoreHeader = bytemuck::from_bytes(store_header_bytes);
        let rest = bytemuck::cast_slice(index_bytes);
        Ok((header, &rest[0..header.entries as usize]))
    }

    fn with_transaction(&mut self, mut f: impl FnOnce(&mut Self) -> Result<()>) -> Result<()> {
        self.update_status(STATUS_NOK)?;
        //TODO: Make sure we actually inspect the status when reading db!
        let ret = f(self);
        self.update_status(STATUS_OK)
            .expect("failed to mark db as OK after transaction complete");
        ret
    }

    fn end_transaction(&mut self) -> Result<()> {
        self.update_status(STATUS_OK)
    }

    fn update_status(&mut self, status: u32) -> Result<()> {
        let header = Self::header(&mut self.index_mmap, &self.index_file)?;
        header.dirty_status = status;
        self.index_mmap.as_mut().unwrap().flush_range(
            offset_of!(StoreHeader, dirty_status),
            std::mem::size_of::<u32>(),
        )?;
        Ok(())
    }

    pub fn query(&self, start: MessageId) -> Result<impl Iterator<Item=&MessageEntry>> {
        let (header, message_index) = self.header_and_index().context("opening index file")?;

        let (Ok(index)|Err(index)) = message_index.binary_search_by_key(&start, |x|x.message);

        println!("Binary search: {}", index);

        Ok(message_index[index..].iter())
    }


    /// Return the given message, or None if it does not exist.
    /// If the call itself fails, returns Err.
    pub fn read_message(&mut self, id: MessageId) -> Result<Option<M>> where M: Message {
        let (header, message_index) = self.header_and_index().context("opening index file")?;

        match message_index.binary_search_by_key(&id, |x|x.message) {
            Ok(index) => {
                let entry = &message_index[index];
                println!("Entry: {:?}", entry);
                let Some((file, offset)) = entry.file_offset.file_and_offset() else {
                    return Err(anyhow!("Message not found in store"));
                };
                let mut file = &mut self.data_files[file.index()].file;
                file.seek(SeekFrom::Start(offset + 8)).context("Seeking to message data")?;
                let size = file.read_u64::<LittleEndian>().context("reading size")?;

                let mut nominal_sha2_bytes = [0u8;16];
                file.read_exact(&mut nominal_sha2_bytes)?;

                let msg = file.with_bytes(size as usize, |bytes|{
                    println!("Message bytes: {:?}, sha2: {:?}", bytes, sha2(bytes));
                    if sha2(bytes) != nominal_sha2_bytes {
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
    pub fn append_many(&mut self, messages: impl ExactSizeIterator<Item=M>) -> Result<()> where M:Message{
        self.with_transaction(|tself| tself.do_append_many(messages))
    }

    /// Marks the db as dirty
    /// Find all the given messages, and mark their entries in their data files as unused.
    /// Use heuristics to determine if files should be compacted.
    /// Compact files (moving to higher file if suitable).
    /// Delete from the index.
    /// Mark the db as clean.
    /// Messages that are not found are simply ignored. The operation does not stop or fail.
    pub fn delete_many(&mut self, messages: impl Iterator<Item=MessageId>) -> Result<()> {
        self.with_transaction(|tself|
            tself.do_delete_many(messages)
        )
    }

    fn do_delete_many(&mut self, messages: impl Iterator<Item=MessageId>) -> Result<()> {
        let (header, message_index) = Self::header_and_index_mut(&mut self.index_mmap, &self.index_file)
            .context("Reading index file")?;

        for message in messages {

            let Ok(index) = message_index.binary_search_by_key(&message, |x|x.message) else {
                continue;
            };
            let message_entry = &mut message_index[index];

            if let Some((file, offset)) = message_entry.file_offset.file_and_offset() {
                let store_file_entry = &mut self.data_files[file.index()];
                message_entry.file_offset = FileOffset::deleted();
                let index_entry = &mut header.data_files[file.index()];

                store_file_entry.file.seek(SeekFrom::Start(offset+8)).context("seeking file")?;
                let size = store_file_entry.file.read_u64::<LittleEndian>()?;
                store_file_entry.file.write_zeroes(16)?;
                store_file_entry.file.flush()?;
                index_entry.bytes_used -= message_entry.file_size;
            }
        }
        Ok(())
    }

    /// Make sure the given number of slots are available.
    fn provide_index_map_for(&mut self, count: usize) -> Result<()> {
        Self::provide_index_map(&mut self.index_mmap, &self.index_file, count);
        Ok(())
    }

    fn do_append_many(&mut self, messages: impl ExactSizeIterator<Item=M>) -> Result<()> where M: Message{
        let len = messages.len();
        self.provide_index_map_for(len)?;

        let entry = Self::provide_entry_for_writing(&mut self.data_files, &self.target)?;

        let (index_header, index) = Self::header_and_index_mut(&mut self.index_mmap, &self.index_file)?;

        let mut index_entries = &mut index[index_header.entries as usize..];

        for msg in messages {
            let start_pos = entry.file.stream_position()?;

            const FILE_HEADER_SIZE : usize = 32;

            entry.file.write_zeroes(FILE_HEADER_SIZE)?;
            msg.serialize(&mut entry.file);

            let end_pos = entry.file.stream_position()?;
            println!("start pos: {} End-pos: {}", start_pos, end_pos);
            let msg_size = end_pos - start_pos - FILE_HEADER_SIZE as u64;
            entry.file.seek(SeekFrom::Start(start_pos+FILE_HEADER_SIZE as u64))?;

            let sha2 = entry
                .file
                .with_bytes(msg_size as usize, |bytes| {
                    let temp = sha2(bytes);
                    println!("Sha2: {:?}", temp);
                    temp
                }

                )?;

            let (cur_message_entry, rest) = index_entries.split_at_mut(1);
            cur_message_entry[0] = MessageEntry {
                message: msg.id(),
                file_offset: FileOffset::new(start_pos, entry.file_number),
                file_size: msg_size,
            };
            index_entries = rest;

            entry.file.seek(SeekFrom::Start(start_pos))?;
            entry.file.write_u64::<LittleEndian>(end_pos)?;
            entry.file.write_u64::<LittleEndian>(msg_size)?;
            assert_eq!(sha2.len(), 16);
            entry.file.write_all(&sha2)?;
            entry.file.seek(SeekFrom::Start(end_pos))?;

            // This is just a little perf improvement, that
            // reduces the risk of messages being split into more than one buffer,
            // thus causing unnecessary seeks and writes.
            entry.file.flush_if(BUFSTREAM_CAPACITY/2, false);
        }
        entry.file.sync_all()?;
        index_header.entries += len as u32;


        Ok(())
    }


    /// Compact all files with file_number and above.
    fn compact(&mut self, file_number: u8, message: M) {
        todo!("add message")
    }

    pub fn new(target: &Target) -> Result<OnDiskMessageStore<M>> {
        const {
            if size_of::<usize>() < size_of::<u32>() {
                panic!(
                    "This platform has a usize that is smaller than 4 bytes. This is not supported"
                );
            }
        }

        std::fs::create_dir_all(&target.path).context("create database directory")?;

        let index_file_path = target.path.join("index.bin");

        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(target.overwrite)
            .open(&index_file_path)
            .context("Opening index-file")
            ?;

        let len = index_file.metadata().context("reading input file")?.len();



        let mut this = Self {
            target: target.clone(),
            index_file,
            index_mmap: None,
            data_files: vec![],
            phantom: Default::default(),
        };

        Ok(this)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};
    use rusqlite::Connection;
    use std::path::Path;
    use std::time::Instant;
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use crate::on_disk_message_store::{FileOffset, OnDiskMessageStore, U1};
    use crate::{DatabaseContext, DummyUnitObject, Message, MessageId, Target};

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
    fn add_and_read_messages() {
        let mut store = OnDiskMessageStore::new(&Target {
            path: "test_disk_store.bin".into(),
            overwrite: true,
        }).unwrap();

        const COUNT: usize = 5; //1_000_000usize;

        let init = Instant::now();
        let msgs = (0..COUNT).map(|i| OnDiskMessage {
            id: i as u64,
            seq: i as u64,
            data: vec![42u8; 4],
        });

        store.append_many(msgs.into_iter()).unwrap();

        let msg = store.read_message(MessageId::new_debug(2)).unwrap().unwrap();
        assert_eq!(msg.id, 2);
    }

    #[test]
    fn add_and_delete_messages() {
        let mut store = OnDiskMessageStore::new(&Target {
            path: "test_disk_store.bin".into(),
            overwrite: true,
        }).unwrap();

        const COUNT: usize = 3;

        let init = Instant::now();
        let msgs = (0..COUNT).map(|i| OnDiskMessage {
            id: i as u64,
            seq: i as u64,
            data: vec![42u8; 4],
        });

        store.append_many(msgs.into_iter()).unwrap();

        let msg = store.delete_many([
            MessageId::new_debug(0),
            MessageId::new_debug(1),
            MessageId::new_debug(2),
        ].into_iter());
    }


    #[test]
    fn test_create_disk_store() {

        let mut store = OnDiskMessageStore::new(&Target {
            path: "test_disk_store.bin".into(),
            overwrite: true,
        }).unwrap();

        const COUNT:usize = 1_000_000;//1_000_000usize;

        let init = Instant::now();
        let msgs = (0..COUNT).map(|i|OnDiskMessage {
            id: (1000_000 + i) as u64,
            seq: i as u64,
            data: vec![42u8; 1024],
        });

        store.append_many(msgs.into_iter()).unwrap();

        println!("Init: {:?}", init.elapsed());
        let mut count = 0;
        let mut sum = 0;
        let start = Instant::now();
        for msg in store.query(MessageId::new_debug(1999_000)).unwrap() {
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


    #[ignore]
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
                    "INSERT INTO message (id, data) VALUES (?1, ?2)",
                    (&me.id, me.data),
                )
                .unwrap();
        }
        println!("Inserting 1 million: {:?}", bef_insert.elapsed());
        trans.commit().unwrap();

        let bef_query = Instant::now();

        /*      let mut stmt = conn.prepare("
                SELECT * FROM (
                SELECT ROW_NUMBER () OVER (
                    ORDER BY id
                ) rownum,id,data FROM message) t
                 where id >= ?1").unwrap();
        */

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
                println!("Seq: {}", count);
                Ok(())
                /* let rowno:usize = row.get(0)?;

                Ok((rowno-1,OnDiskMessage {
                    id: row.get(1)?,
                    data: row.get(2)?,
                }))*/
            })
            .unwrap();

        let mut idsum = 0;
        for person in person_iter {
            //println!("got: {:?}", person.unwrap());
            idsum += 1;
        }
        println!("Query {:?} (ids: {})", bef_query.elapsed(), idsum);
    }
}
