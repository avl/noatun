use crate::disk_abstraction::Disk;
use crate::growable_file_mapping::DiskMmapHandleNew;
use crate::{MessageId, SequenceNr, Target};
use anyhow::Result;
use bytemuck::{Pod, Zeroable};
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use std::backtrace::Backtrace;
use std::cell::RefCell;
use std::io::Write;

#[derive(Debug)]
pub enum UndoLogEntry<'a> {
    /// Set the database pointer to the given value (and clear all bytes after)
    SetPointer(usize),
    /// Zero out the values in start..end
    ZeroOut {
        start: usize,
        len: usize,
    },
    /// Restore memory at start, with the contents of the slice
    Restore {
        start: usize,
        data: &'a [u8],
    },
    Rewind(SequenceNr),
}

pub struct UndoLog {
    store_mmap: RefCell<DiskMmapHandleNew>,
}

impl UndoLog {
    pub(crate) fn clear(&self) -> Result<()> {
        self.store_mmap.borrow_mut().truncate(0)
    }
}

pub enum HowToProceed {
    PopAndStop,
    PopAndContinue,
    Error,
}

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct UndoLogHeader {
    len: usize,
}

impl UndoLog {
    pub fn new<D: Disk>(disk: &mut D, target: &Target) -> Result<UndoLog> {
        let mut file = disk.open_file(target, "undo")?;

        Ok(UndoLog {
            store_mmap: RefCell::new(file),
        })
    }

    fn access<R>(&self, f: impl FnOnce(&UndoLogHeader, &[u8]) -> R) -> R {
        let bytes = self.store_mmap.borrow();
        let (header, rest) = bytes.map().split_at(size_of::<UndoLogHeader>());
        let header: &UndoLogHeader = bytemuck::from_bytes(header);
        let len = header.len;
        f(header, &rest[0..len])
    }
    fn access_mut<R>(&mut self, f: impl FnOnce(&mut UndoLogHeader, &mut [u8]) -> R) -> R {
        let mut bytes = self.store_mmap.borrow_mut();
        let (header, rest) = bytes.map_mut().split_at_mut(size_of::<UndoLogHeader>());
        let header: &mut UndoLogHeader = bytemuck::from_bytes_mut(header);
        let len = header.len;
        f(header, rest)
    }

    /// Calls the callback with the most recent entry in the undo-log, repeatedly.
    /// If no entry, return false. If closure returns 'Error', return false.
    /// Otherwise return true;
    pub fn rewind(&mut self, mut cb: impl FnMut(UndoLogEntry) -> HowToProceed) -> bool {
        self.access_mut(|header, store| {
            let store = &store[0..header.len];
            while let Some((new_len, item)) = Self::parse1(store) {
                match cb(item) {
                    HowToProceed::Error => {
                        return false;
                    }
                    HowToProceed::PopAndStop => {
                        header.len = new_len;
                        return true;
                    }
                    HowToProceed::PopAndContinue => {
                        header.len = new_len;
                    }
                }
            }
            false
        })
    }
    fn parse1(data: &[u8]) -> Option<(usize /*new size*/, UndoLogEntry)> {
        if data.len() == 0 {
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
                return Some((offset, UndoLogEntry::SetPointer(prev_ptr as usize)));
            }
            2 => {
                assert!(offset >= 16);
                offset -= 8;
                let len = LittleEndian::read_u64(&data[offset..offset + 8]) as usize;
                offset -= 8;
                let start = LittleEndian::read_u64(&data[offset..offset + 8]) as usize;
                return Some((offset, UndoLogEntry::ZeroOut { start, len }));
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
                return Some((offset, UndoLogEntry::Restore { start, data: buf }));
            }
            4 => {
                assert!(offset >= 4);
                offset -= 4;
                let time: SequenceNr = bytemuck::pod_read_unaligned(&data[offset..offset + 4]);
                return Some((offset, UndoLogEntry::Rewind(time)));
            }
            _ => panic!("Corrupt undo-store"),
        }
    }
    pub fn record(&self, entry: UndoLogEntry) {
        let mut store_ref = self.store_mmap.borrow_mut();
        let store = &mut *store_ref;
        todo!()
        /*
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
            UndoLogEntry::Restore { start, data } => {
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
                    .write_u8(if all_zero { 2 } else { 3 })
                    .expect("Failed to write to undo store");
            }
            UndoLogEntry::Rewind(time) => {
                store
                    .write_all(bytemuck::bytes_of(&time))
                    .expect("Failed to write to undo store");
                store.write_u8(4).expect("Failed to write to undo store");
            }
        }
        */
    }
}
