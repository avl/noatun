use crate::disk_abstraction::Disk;
use crate::disk_access::FileAccessor;
use crate::{DatabaseContext, Object, SequenceNr, Target, ThinPtr};
use anyhow::Result;
use bytemuck::{Pod, Zeroable};
use indexmap::IndexMap;
use memmap2::{Mmap, MmapMut};
use std::fs::{File, OpenOptions};
use std::iter;
use std::path::Path;
use crate::data_types::{DatabaseVec, RawDatabaseVec};
use crate::projection_store::message_dependency_tracker::linked_list_entry::DepTrackLinkedListEntry;


pub(crate) mod linked_list_entry {
    use bytemuck::{Pod, Zeroable};
    use crate::{SequenceNr, ThinPtr};

    #[derive(Debug, Clone, Copy, Pod, Zeroable, Default)]
    #[repr(C)]
    pub(crate) struct DepTrackLinkedListEntry {
        // use two values, so we only use 12 byte per entry, because of alignment
        // 0 means 'the end'
        pub next_lsb: u32,
        pub next_msb: u32,
        pub seq: SequenceNr,
    }

    impl DepTrackLinkedListEntry {
        pub(crate) fn set_next(&mut self, thin_ptr: ThinPtr) {
            let next = thin_ptr.0;
            self.next_lsb = next as u32;
            self.next_msb = (next >> 32) as u32;
        }
        pub(crate) fn get_next(&self) -> u64 {
            self.next_lsb as u64 | ((self.next_msb as u64) << 32)
        }
        pub(crate) fn seq(&self) -> SequenceNr {
            self.seq
        }
        pub(crate) fn set_seq(&mut self, new_seq: SequenceNr) {
            self.seq = new_seq;
        }
    }

}


#[cfg(test)]
mod tests {
    use crate::disk_abstraction::StandardDisk;
    use crate::projection_store::message_dependency_tracker::MessageDependencyTracker;
    use crate::projection_store::message_dependency_tracker::MmapMessageDependencyTracker;
    use crate::{SequenceNr, Target};
    use std::path::Path;
    use std::time::Instant;

    #[test]
    fn smoke_deptrack() {
        let mut tracker = MmapMessageDependencyTracker::new(
            &mut StandardDisk,
            &Target::CreateNewOrOverwrite("test/test_smoke_deptrack.bin".into()),
            10000,
        )
        .unwrap();
        tracker.record_dependency(SequenceNr::from_index(1), SequenceNr::from_index(2));

        let result: Vec<_> = tracker.read_dependency(SequenceNr::from_index(1)).collect();
        assert_eq!(result, vec![SequenceNr::from_index(2)]);
    }

    #[test]
    fn smoke_deptrack_many() {
        let mut tracker = MmapMessageDependencyTracker::new(
            &mut StandardDisk,
            &Target::CreateNewOrOverwrite("test/test_smoke_deptrack_many.bin".into()),
            10000,
        )
        .unwrap();
        let t = Instant::now();
        for i in 0..100_usize {
            tracker.record_dependency(SequenceNr::from_index(i.isqrt()), SequenceNr::from_index(i));
        }
        println!("Time: {:?}", t.elapsed());

        let result: Vec<_> = tracker
            .read_dependency(SequenceNr::from_index(8))
            .map(|x| x.index())
            .collect();
        assert_eq!(result, vec![
            80, 79, 78, 77, 76, 75, 74, 73, 72, 71, 70, 69, 68, 67, 66, 65, 64
        ]);
    }
}
