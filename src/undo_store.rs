use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use std::backtrace::Backtrace;
use std::cell::RefCell;
use std::io::Write;
use crate::{MessageId, SequenceNr};

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
    store: Vec<u8>,
}

pub enum HowToProceed {
    PopAndStop,
    PopAndContinue,
    Error,
}

impl UndoLog {
    pub fn new() -> UndoLog {
        UndoLog { store: vec![] }
    }

    /// Calls the callback with the most recent entry in the undo-log, repeatedly.
    /// If no entry, return false. If closure returns 'Error', return false.
    /// Otherwise return true;
    pub fn rewind(&mut self, mut cb: impl FnMut(UndoLogEntry) -> HowToProceed) -> bool {
        while let Some((new_len, item)) = Self::parse1(&self.store) {
            match cb(item) {
                HowToProceed::Error => {
                    return false;
                }
                HowToProceed::PopAndStop => {
                    self.store.truncate(new_len);
                    return true;
                }
                HowToProceed::PopAndContinue => {
                    self.store.truncate(new_len);
                }
            }
        }
        false
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
                let time : SequenceNr = bytemuck::pod_read_unaligned(&data[offset..offset + 4]);
                return Some((offset, UndoLogEntry::Rewind(time)));
            }
            _ => panic!("Corrupt undo-store"),
        }
    }
    pub fn record(&mut self, entry: UndoLogEntry) {
        let store = &mut self.store;
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
    }
}
