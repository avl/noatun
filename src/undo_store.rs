use crate::disk_abstraction::Disk;
use crate::disk_access::FileAccessor;
use crate::sequence_nr::SequenceNr;
use crate::{uninit_slice, Target};
use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use std::io::{Seek, SeekFrom, Write};
use std::mem::MaybeUninit;

#[derive(Debug)]
pub enum UndoLogEntry<'a> {
    /// Set the database pointer to the given value (and clear all bytes after)
    SetPointer(usize),
    /// Zero out the values in start..end. I.e, this is the undo action of overwriting zeroes.
    ZeroOut { start: usize, len: usize },
    /// Restore memory at start, with the contents of the slice
    /// It is okay for the slice to contain uninitialized values
    RestoreUninit { start: usize, data: &'a [MaybeUninit<u8>] },
    /// Restore memory at start, with the contents of the slice.
    /// The slice cannot contain uninitialized values.
    RestorePod { start: usize, data: &'a [u8] },
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
    Error,
}

impl UndoLog {
    pub fn new<D: Disk>(disk: &mut D, target: &Target, max_size: usize) -> Result<UndoLog> {
        let file = disk.open_file(target, "undo", 0, max_size)?;

        Ok(UndoLog { store_mmap: file })
    }

    fn access<R>(&self, f: impl FnOnce(&FileAccessor) -> R) -> R {
        let bytes = &self.store_mmap;
        f(bytes)
    }
    fn access_mut<R>(&mut self, f: impl FnOnce(&mut FileAccessor) -> R) -> R {
        let bytes = &mut self.store_mmap;
        f(bytes)
    }

    /// Calls the callback with the most recent entry in the undo-log, repeatedly.
    /// If no entry, return false. If closure returns 'Error', return false.
    /// Otherwise return true;
    pub fn rewind(&mut self, mut cb: impl FnMut(UndoLogEntry) -> HowToProceed) -> bool {
        self.access_mut(|mmap| {
            while let Some((new_len, item)) = Self::parse1(mmap.map()) {
                match cb(item) {
                    HowToProceed::Error => {
                        panic!("HowToProceed::Error");
                        //return false;
                    }
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
    fn parse1(data: &[u8]) -> Option<(usize /*new size*/, UndoLogEntry)> {
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
            3 => {
                assert!(offset >= 16);
                offset -= 8;
                let len = LittleEndian::read_u64(&data[offset..offset + 8]) as usize;
                offset -= 8;
                let start = LittleEndian::read_u64(&data[offset..offset + 8]) as usize;
                assert!(offset >= len);
                offset -= len;
                let buf = &data[offset..offset + len];
                Some((offset, UndoLogEntry::RestoreUninit { start, data: uninit_slice(buf) }))
            }
            4 => {
                assert!(offset >= 16);
                offset -= 8;
                let len = LittleEndian::read_u64(&data[offset..offset + 8]) as usize;
                offset -= 8;
                let start = LittleEndian::read_u64(&data[offset..offset + 8]) as usize;
                assert!(offset >= len);
                offset -= len;
                let buf = &data[offset..offset + len];
                Some((offset, UndoLogEntry::RestorePod { start, data: buf }))
            }
            5 => {
                assert!(offset >= 4);
                offset -= 4;
                let time: SequenceNr = bytemuck::pod_read_unaligned(&data[offset..offset + 4]);
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
            UndoLogEntry::RestoreUninit { start, data } => {
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
            }
            UndoLogEntry::RestorePod { start, data } => {
                let all_zero = data.iter().copied().all(|x| x == 0);
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
                    .write_u64::<LittleEndian>(data.len() as u64)
                    .expect("Failed to write to undo store");
                store
                    .write_u8(if all_zero { 2 } else { 4 })
                    .expect("Failed to write to undo store");
            }
            UndoLogEntry::Rewind(time) => {
                store
                    .write_all(bytemuck::bytes_of(&time))
                    .expect("Failed to write to undo store");
                store.write_u8(5).expect("Failed to write to undo store");
            }
        }
    }
}
