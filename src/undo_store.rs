use std::cell::{Cell, RefCell};
use std::io::Write;
use byteorder::{LittleEndian, WriteBytesExt};

pub enum UndoLogEntry<'a> {
    /// Allocate the number of bytes
    SetPointer(usize),
    /// Zero out the values in start..end
    ZeroOut {
        start: usize,
        len: usize,
    },
    /// Restore memory at start, with the contents of the slice
    Restore {
        start: usize,
        data: &'a [Cell<u8>]
    }
}

pub struct UndoLog {
    store: RefCell<Vec<u8>>,
}


impl UndoLog {
    pub fn new() -> UndoLog {
        UndoLog {
            store: RefCell::new(vec![]),
        }
    }
    pub fn record(&self, entry: UndoLogEntry) {
        let mut store = self.store.borrow_mut();
        let mut store = &mut *store;
        match entry {
            UndoLogEntry::SetPointer(size) => {
                store.write_u8(1).expect("Failed to write to undo store");
                store.write_u64::<LittleEndian>(size as u64).expect("Failed to write to undo store");
            }
            UndoLogEntry::ZeroOut { start, len } => {
                store.write_u8(2).expect("Failed to write to undo store");
                store.write_u64::<LittleEndian>(start as u64).expect("Failed to write to undo store");
                store.write_u64::<LittleEndian>(len as u64).expect("Failed to write to undo store");
            }
            UndoLogEntry::Restore { start, data } => {
                store.write_u8(3).expect("Failed to write to undo store");
                store.write_u64::<LittleEndian>(start as u64).expect("Failed to write to undo store");
                store.write_u64::<LittleEndian>(data.len() as u64).expect("Failed to write to undo store");

                {
                    // SAFETY:
                    // This is safe, since no other references to these 'u8' exist.
                    // Everyone else has only at most a &Cell<..>.
                    // This whole data structure is !Sync, so no other threads exist.
                    let data : &[u8] = unsafe { std::mem::transmute(data) };
                    store.write_all(data).expect("Failed to write to undo store");
                }
            }
        }
    }
    pub(crate) fn record_ptr(&self, pointer: usize) {
        self.record(UndoLogEntry::SetPointer(pointer));
    }

}