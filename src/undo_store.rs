use crate::disk_abstraction::Disk;
use crate::disk_access::FileAccessor;
use crate::sequence_nr::SequenceNr;
use crate::{bytes_of, read_unaligned, Target};
use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use std::arch::asm;
use std::io::{Seek, SeekFrom, Write};

#[derive(Debug)]
pub enum UndoLogEntry<'a> {
    /// Set the database pointer to the given value (and clear all bytes after)
    SetPointer(usize),
    /// Zero out the values in start..end. I.e, this is the undo action of overwriting zeroes.
    ZeroOut { start: usize, len: usize },
    // Restore memory at start, with the contents of the slice
    // It is okay for the slice to contain uninitialized values
    //RestoreUninit { start: usize, data: &'a [MaybeUninit<u8>] },
    /// Restore memory at start, with the contents of the slice.
    /// The slice cannot contain uninitialized values.
    /// Note, all bytes in 'data' must be initialized
    RestorePod { start: usize, data: &'a mut [u8] },
    /// This entry is emitted in the undo-log _prior_ to SequenceNr.
    /// I.e, if you rewind to this, the event that happened at SequenceNr will not
    /// be present in the database. I.e, this should be considered the 'next' SequenceNr
    /// In all nominal situations, the last entry in the undo-log is a SequenceNr.
    /// Rewinding to a specific SequenceNr leaves its UndoLogEntry::Rewind in the undo-log, but
    /// not the event.
    Rewind(SequenceNr),
}

pub struct UndoLog {
    store_mmap: FileAccessor,
}

impl UndoLog {
    pub(crate) fn clear(&mut self) -> Result<()> {
        self.store_mmap.truncate(0)
    }
}

pub enum HowToProceed {
    /// Leave the current entry in the undo log, and stop traversal.
    /// Used for Rewind-entries, which we should let remain.
    DontPopAndStop,
    /// Pop the current element from the undo log, and proceed
    PopAndContinue,
}

/// Magically initialize all padding bytes of T to some concrete, but unspecified value.
///
/// This routine ostensibly knows about which values are padding bytes in any type, and
/// specifically writes these bytes with arbitrary values.
///
/// In fact, it doesn't actually do anything. However, it uses inline-assembly, which is
/// guaranteed to be considered a "black box" by the compiler. This means the compiler
/// has to assume it _could_ have initialized all the padding bytes. Otherwise, it might
/// analyze the code and determine that some code path will read uninitialized memory.
/// Since this is known to be impossible, it could conclude that the code path it has
/// identified cannot occur, and might optimize it all away.
///
/// By calling this method, the optimizer can no longer see this. It must assume that
/// T could have been rewritten from scratch.
///
/// SAFETY:
/// The actual memory that T is stored in, must be in some reliable state. I.e, you
/// must make sure it hasn't been allocated using linux MADV_FREE-semantics, for example.
/// In principle, this guarantee can only be upheld if the caller has full control over the
/// memory, for example, having allocated it from a memory-mapped file with known options.
#[inline]
pub(crate) unsafe fn magic_initialize_ptr<T>(data_ptr: *mut T) {
    unsafe {
        asm!(
        "nop",
        "/* magic {0} */",
            in(reg) data_ptr
        )
    }
}

impl UndoLog {
    pub fn new<D: Disk>(
        disk: &mut D,
        target: &Target,
        min_size: usize,
        max_size: usize,
    ) -> Result<UndoLog> {
        let (file, _existed) = disk.open_file(target, "undo", min_size, max_size, "undo", "Undo log")?;

        Ok(UndoLog { store_mmap: file })
    }

    fn access_mut<R>(&mut self, f: impl FnOnce(&mut FileAccessor) -> R) -> R {
        let bytes = &mut self.store_mmap;
        f(bytes)
    }

    /// Calls the callback with the most recent entry in the undo-log, repeatedly.
    /// If no entry, return false. If closure returns 'Error', return false.
    /// Otherwise return true;
    pub(crate) fn rewind(&mut self, mut cb: impl FnMut(UndoLogEntry) -> HowToProceed) -> bool {
        self.access_mut(|mmap| {
            while let Some((new_len, item)) = Self::parse1(mmap.map_mut()) {
                match cb(item) {
                    HowToProceed::DontPopAndStop => {
                        return true;
                    }
                    HowToProceed::PopAndContinue => {
                        mmap.fast_truncate(new_len);
                    }
                }
            }
            panic!("Rewind failed - couldn't reach desired time!");
            //false
        })
    }
    fn parse1(data: &mut [u8]) -> Option<(usize /*new size*/, UndoLogEntry<'_>)> {
        if data.is_empty() {
            return None;
        }
        let mut offset = data.len() - 1;
        let last = data[offset];
        match last {
            1 => {
                // SetPointer
                assert!(offset >= 8);
                offset -= 8;
                let prev_ptr = LittleEndian::read_u64(&data[offset..offset + 8]);
                Some((offset, UndoLogEntry::SetPointer(prev_ptr as usize)))
            }
            2 => {
                assert!(offset >= 16);
                offset -= 8;
                let len = LittleEndian::read_u64(&data[offset..offset + 8]) as usize;
                offset -= 8;
                let start = LittleEndian::read_u64(&data[offset..offset + 8]) as usize;
                Some((offset, UndoLogEntry::ZeroOut { start, len }))
            }
            /*3 => {
                assert!(offset >= 16);
                offset -= 8;
                let len = LittleEndian::read_u64(&data[offset..offset + 8]) as usize;
                offset -= 8;
                let start = LittleEndian::read_u64(&data[offset..offset + 8]) as usize;
                assert!(offset >= len);
                offset -= len;
                let buf = &data[offset..offset + len];
                Some((offset, UndoLogEntry::RestorePod { start, data: uninit_slice(buf) }))
            }*/
            4 => {
                assert!(offset >= 16);
                offset -= 8;
                let len = LittleEndian::read_u64(&data[offset..offset + 8]) as usize;
                offset -= 8;
                let start = LittleEndian::read_u64(&data[offset..offset + 8]) as usize;
                assert!(offset >= len);
                offset -= len;
                let buf = &mut data[offset..offset + len];
                Some((offset, UndoLogEntry::RestorePod { start, data: buf }))
            }
            5 => {
                assert!(offset >= 4);
                offset -= 4;
                let time: SequenceNr = read_unaligned(&data[offset..offset + 4]);
                Some((offset, UndoLogEntry::Rewind(time)))
            }
            _ => panic!("Corrupt undo-store"),
        }
    }
    pub fn record(&mut self, entry: UndoLogEntry) {
        let store_ref = &mut self.store_mmap;
        let store = &mut *store_ref;
        store.seek(SeekFrom::End(0)).unwrap();
        match entry {
            UndoLogEntry::SetPointer(size) => {
                store
                    .write_u64::<LittleEndian>(size as u64)
                    .expect("Failed to write to undo store");
                store.write_u8(1).expect("Failed to write to undo store");
            }
            UndoLogEntry::ZeroOut { start, len } => {
                store
                    .write_u64::<LittleEndian>(start as u64)
                    .expect("Failed to write to undo store");
                store
                    .write_u64::<LittleEndian>(len as u64)
                    .expect("Failed to write to undo store");
                store.write_u8(2).expect("Failed to write to undo store");
            }
            /*UndoLogEntry::RestoreUninit { start, data } => {
                // SAFETY:
                // This is safe, since no other references to these 'u8' exist.
                // Everyone else has only at most a &Cell<..>.
                // This whole data structure is !Sync, so no other threads exist.
                let data: &[MaybeUninit<u8>] = unsafe { std::mem::transmute(data) };
                store
                    .write_uninit(data)
                    .expect("Failed to write to undo store");
                store
                    .write_u64::<LittleEndian>(start as u64)
                    .expect("Failed to write to undo store");
                store
                    .write_u64::<LittleEndian>(data.len() as u64)
                    .expect("Failed to write to undo store");
                store
                    .write_u8(3)
                    .expect("Failed to write to undo store");
            }*/
            UndoLogEntry::RestorePod { start, data } => {
                let data_len = data.len();

                let all_zero;
                #[cfg(not(miri))]
                {
                    unsafe { magic_initialize_ptr(data.as_mut_ptr()) };
                    all_zero = data.iter().copied().all(|x| x == 0);
                }
                #[cfg(miri)]
                {
                    all_zero = false;
                }

                if !all_zero {
                    // SAFETY:
                    // This is safe, since no other references to these 'u8' exist.
                    // Everyone else has only at most a &Cell<..>.
                    // This whole data structure is !Sync, so no other threads exist.
                    let data: &[u8] = unsafe { std::mem::transmute(data) };
                    store
                        .write_all(data)
                        .expect("Failed to write to undo store");
                }
                store
                    .write_u64::<LittleEndian>(start as u64)
                    .expect("Failed to write to undo store");
                store
                    .write_u64::<LittleEndian>(data_len as u64)
                    .expect("Failed to write to undo store");
                store
                    .write_u8(if all_zero { 2 } else { 4 })
                    .expect("Failed to write to undo store");
            }
            UndoLogEntry::Rewind(time) => {
                store
                    .write_all(bytes_of(&time))
                    .expect("Failed to write to undo store");
                store.write_u8(5).expect("Failed to write to undo store");
            }
        }
    }
}
