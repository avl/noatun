use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::path::Path;
use memmap2::{Mmap, MmapMut};
use crate::MessageId;
use anyhow::{Context, Result};
use bytemuck::{Pod, Zeroable};

#[repr(C)]
#[derive(Debug,Clone,Copy,Pod,Zeroable)]
struct MainHeader {
    message: MessageId,
    file_offset: u64, //MSB 16 bits are file number
}

const DEFAULT_MAX_COUNT: usize = 1000;
const DEFAULT_MAX_SIZE_BYTES: usize = DEFAULT_MAX_COUNT * std::mem::size_of::<MainHeader>();

impl PartialEq for MainHeader {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
    }
}
impl Eq for MainHeader {}
impl PartialOrd for MainHeader {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.partial_cmp(other)
    }
}
impl Ord for MainHeader {
    fn cmp(&self, other: &Self) -> Ordering {
        self.message.cmp(&other.message)
    }
}

struct DataFileEntry {
    /// The size of this file.
    ///
    nominal_size:u64,
    /// The number of bytes actually used.
    /// If this becomes small, compaction may be in order
    bytes_used: u64,
}

struct DataFileInfo {
    /// Where in the db data stream this file starts
    /// Note: Adding data is always possible at the end of the last file.
    /// Moving stuff to files higher in the chain is easy, just update
    /// the [`DataFileEntry::size`] field.
    /// When compacting, the main index needs to be rewritten.
    offset: u64,
    /// The length of this file.
    length: u64,
    file_number: u8,

}

#[repr(C,u8)]
enum WriteLogKind {
    CompactFile(u32),
}


struct WriteLogEntry {
    kind: WriteLogKind,
    checksum: u64,
}

#[repr(u32)]
enum Status {
    Clean,
    Dirty
}

#[repr(C)]
struct StoreHeader {
    dirty_status: Status,
    pad1: u32,
    data_files: [DataFileEntry; 16],
}


struct OnDiskMessageStore {
    mmap: MmapMut,
    status_file: File,
}

impl OnDiskMessageStore {


    /// Add the given messages.
    /// Marks the db as dirty.
    /// Then writes to the active data file.
    ///
    /// This will determine the position they need to be added to, and will then
    /// merge with the index.
    /// Marks the db clean.
    fn append_many<M>(&mut self, message: &mut [M]) {
        todo!("add message")
    }

    /// Marks the db as dirty
    /// Find all the given messages, and mark their entries in their data files as unused.
    /// Use heuristics to determine if files should be compacted.
    /// Compact files (moving to higher file if suitable).
    /// Delete from the index.
    /// Mark the db as clean.
    fn delete_many(&mut self, messages: &mut [MessageId]) {

    }

    /// Compact all files with file_number and above.
    fn compact<M>(&mut self, file_number: u8, message: M) {
        todo!("add message")
    }

    pub fn new(path: impl AsRef<Path>) -> Result<OnDiskMessageStore> {

        std::fs::create_dir_all(path.as_ref()).context("create database directory")?;

        let status_file_path = path.as_ref().join("status.bin");
        let index_file_path = path.as_ref().join("index.bin");

        let status_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&status_file_path)?;

        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&index_file_path)?;

        let len = index_file.metadata().context("reading input file")?.len();

        let min_size_bytes = (size_of::<StoreHeader>() + DEFAULT_MAX_SIZE_BYTES) as u64;
        if len < min_size_bytes {
            index_file.set_len(min_size_bytes);
        }

        let mmap = unsafe { Mmap::map(&index_file)?  };

        let mut this = Self {
            mmap: (),
            mmap: (),
        };


        Ok(this)
    }
}



#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Instant;
    use rusqlite::Connection;

    #[derive(Debug)]
    struct OnDiskMessage {
        id: u64,
        seq: u64,
        data: Vec<u8>,
    }

    #[test]
    fn test_create_disk_store() {
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
        ).unwrap();

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
                id: 1000_000+i,
                seq: i,
                data: vec![42u8;1024],
            };
            trans.execute(
                "INSERT INTO message (id, data) VALUES (?1, ?2)",
                (&me.id, me.data),
            ).unwrap();
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

        let mut stmt = conn.prepare("
                SELECT seq FROM message
                 where id >= ?1").unwrap();
        let person_iter = stmt.query_map([1_000_000+999_900], |row| {

            let count: usize = row.get(0)?;
            println!("Seq: {}", count);
            Ok(())
           /* let rowno:usize = row.get(0)?;

            Ok((rowno-1,OnDiskMessage {
                id: row.get(1)?,
                data: row.get(2)?,
            }))*/
        }).unwrap();

        let mut idsum = 0;
        for person in person_iter {
            //println!("got: {:?}", person.unwrap());
            idsum += 1;
        }
        println!("Query {:?} (ids: {})", bef_query.elapsed(), idsum);
    }
}