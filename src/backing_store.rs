use std::cell::Cell;
use std::slice::SliceIndex;
use crate::buffer::DummyMemoryMappedBuffer;
use crate::undo_store::{UndoLog, UndoLogEntry};

pub trait BackingStore {
    fn allocate<const N:usize, const ALIGN: usize>(&self) -> &[Cell<u8>;N];
    fn access(&self, range: impl SliceIndex<[Cell<u8>], Output = [Cell<u8>]>) -> &[Cell<u8>];
    fn index_of<T>(&self, t:&T) -> usize;
    fn write(&self, index: usize, data: &[u8]);

}

struct DummyBackingStore {
    data_store: DummyMemoryMappedBuffer,
    undo_log: UndoLog
}

impl BackingStore for DummyBackingStore {
    fn allocate<const N: usize, const ALIGN: usize>(&self) -> &[Cell<u8>; N] {
        self.undo_log.record_ptr(self.data_store.pointer());
        self.data_store.allocate::<N,ALIGN>()

    }
    fn access(&self, range: impl SliceIndex<[Cell<u8>], Output = [Cell<u8>]>) -> &[Cell<u8>] {
        self.data_store.access(range)
    }

    fn write(&self, index: usize, data: &[u8]) {
        let prev =&self.data_store.access(index..index+data.len());
        if prev.iter().all(|x|x.get()==0) {
            self.undo_log.record(UndoLogEntry::ZeroOut{
                start: index,
                len: data.len(),
            });
        } else {
            self.undo_log.record(UndoLogEntry::Restore{
                start: index,
                data: prev,
            });
        }
        self.data_store.write(index, data);
    }



    fn index_of<T>(&self, t: &T) -> usize {
        self.index_of(t)
    }
}
