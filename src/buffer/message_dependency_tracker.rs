use crate::{SequenceNr, Target};
use anyhow::Result;
use bytemuck::{Pod, Zeroable};
use indexmap::IndexMap;
use memmap2::{Mmap, MmapMut};
use std::fs::{File, OpenOptions};
use std::iter;
use std::path::Path;
use crate::disk_abstraction::{Disk, DiskFile, DiskMmapHandle};

pub trait MessageDependencyTracker {
    fn new<S:Disk>(s: &mut S, path: &Target) -> Result<Self>
    where
        Self: Sized;
    fn record_dependency(&mut self, observee: SequenceNr, observer: SequenceNr);
    fn read_dependency(&mut self, observee: SequenceNr) -> impl Iterator<Item = SequenceNr>;
}

impl MessageDependencyTracker for IndexMap<SequenceNr, Vec<SequenceNr>> {
    fn new<S:Disk>(s: &mut S, name: &Target) -> Result<Self> {
        Ok(IndexMap::new())
    }

    fn record_dependency(&mut self, observee: SequenceNr, observer: SequenceNr) {
        self.entry(observee).or_default().push(observer);
    }

    fn read_dependency(&mut self, observee: SequenceNr) -> impl Iterator<Item = SequenceNr> {
        self.get(&observee)
            .into_iter()
            .flat_map(|x| x.iter().copied())
    }
}

// Mapping from observee to observers.
pub struct MmapMessageDependencyTracker {
    /// the sequencenr that has been observed
    keys: DiskMmapHandle,
    key_capacity: usize,
    /// the sequence nr that have observed each key. I.e, the messages that
    /// are dependent upon the key.
    vals: DiskMmapHandle,
    value_capacity: usize,
    key_file: Box<dyn DiskFile>,
    value_file: Box<dyn DiskFile>,
}

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct LinkedListEntry {
    next_lsb: u32,
    next_msb: u32,
    seq: SequenceNr,
}

impl LinkedListEntry {
    fn set_next(&mut self, next: u64) {
        self.next_lsb = next as u32;
        self.next_msb = (next >> 32) as u32;
    }
    fn get_next(&self) -> u64 {
        self.next_lsb as u64 | ((self.next_msb as u64) << 32)
    }
}

impl MessageDependencyTracker for MmapMessageDependencyTracker {
    fn new<S:Disk>(disk: &mut S, path: &Target) -> Result<Self> {
        std::fs::create_dir_all(&path.path());
        let key_path = path.path().join("dep_keys.bin");
        let value_path = path.path().join("dep_values.bin");
        /*let key_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(path.overwrite())
            .open(&key_path)?;
        let value_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(path.create())
            .truncate(path.overwrite())
            .open(&value_path)?;
        */

        let mut key_file = disk.open_file(&key_path, true, path.overwrite())?;
        let mut value_file = disk.open_file(&value_path, true, path.overwrite())?;

        const DEFAULT_KEY_CAPACITY: usize = 3;
        const DEFAULT_VALUE_CAPACITY: usize = 3;

        let key_size_bytes = Self::calc_needed_bytes_keys(DEFAULT_KEY_CAPACITY);
        let value_size_bytes = Self::calc_needed_bytes_vals(DEFAULT_VALUE_CAPACITY);

        //println!("Setting lens: {} {}", key_size_bytes, value_size_bytes);
        key_file.set_len(key_size_bytes as u64)?;
        value_file.set_len(value_size_bytes as u64)?;

        //let key_mmap = unsafe { MmapMut::map_mut(&key_file)? };
        //let value_mmap = unsafe { MmapMut::map_mut(&value_file)? };
        let key_mmap = key_file.mmap()?;
        let value_mmap = value_file.mmap()?;

        Ok(MmapMessageDependencyTracker {
            key_file: Box::new(key_file),
            value_file: Box::new(value_file),
            key_capacity: DEFAULT_KEY_CAPACITY,
            keys: key_mmap,
            value_capacity: DEFAULT_VALUE_CAPACITY,
            vals: value_mmap,
        })
    }

    fn record_dependency(&mut self, observee: SequenceNr, observer: SequenceNr) {
        assert!(observee.is_valid());
        assert!(observer.is_valid());

        //println!("Observee index {}, cap: {}", observee.index(), self.key_capacity);
        if observee.index() >= self.key_capacity {
            //println!("REALLOC!");
            self.reallocate_keys((observee.index() + 1) * 2);
        }

        //dbg!(Self::get_count(&self.vals) + 1, self.value_capacity);
        if Self::get_count(&self.vals) + 1 >= self.value_capacity {
            //println!("REALLOC!");
            self.reallocate_values((self.value_capacity + 1) * 2);
        }

        let (val_len, vals): (_, &mut [LinkedListEntry]) = Self::access(&mut self.vals);
        let (key_len, keys): (_, &mut [u64]) = Self::access(&mut self.keys);
        debug_assert_eq!(keys.len(), self.key_capacity as usize);
        debug_assert_eq!(vals.len(), self.value_capacity as usize);

        let prev = keys[observee.index()];

        let new_entry: &mut LinkedListEntry = &mut vals[*val_len as usize];
        new_entry.seq = observer;
        new_entry.set_next(prev);
        keys[observee.index()] = *val_len + 1;

        *val_len += 1;
        /*println!("Final state:\n{:?} keys: {:?}\n{:?} values: {:?}",
            key_len, keys,
            val_len, vals,
        )*/
    }
    fn read_dependency(&mut self, observee: SequenceNr) -> impl Iterator<Item = SequenceNr> {
        let (key_len, keys): (_, &mut [u64]) = Self::access(&mut self.keys);
        let (val_len, vals): (_, &mut [LinkedListEntry]) = Self::access(&mut self.vals);
        debug_assert_eq!(keys.len(), self.key_capacity as usize);
        debug_assert_eq!(vals.len(), self.value_capacity as usize);

        let mut cur: u64 = if observee.index() < keys.len() {
            keys[observee.index()]
        } else {
            0
        };

        iter::from_fn(move || {
            if cur == 0 {
                return None;
            }
            let entry = &vals[cur as usize - 1];
            cur = entry.get_next();
            return Some(entry.seq);
        })
    }
}
impl  MmapMessageDependencyTracker {
    #[inline]
    fn get_count(mmap: &DiskMmapHandle) -> usize {
        *bytemuck::from_bytes::<u64>(&mmap.map()[0..size_of::<u64>()]) as usize
    }
    #[inline]
    fn access<T: Pod>(mmap: &mut DiskMmapHandle) -> (&mut u64, &mut [T]) {
        let slice_bytes: &mut [u8] = mmap.map_mut();
        //println!("Mmap size: {}, item size: {}", slice_bytes.len(), size_of::<T>());
        let (slice_a, slice_b) = slice_bytes.split_at_mut(std::mem::size_of::<u64>());
        let count: &mut u64 = bytemuck::from_bytes_mut(slice_a);
        let slice: &mut [T] = bytemuck::cast_slice_mut(slice_b);
        //println!("Couint: {}, cap: {}", count, slice.len());
        (count, slice)
    }

    // Record mapping from observee to observer

    fn calc_needed_bytes_keys(count: usize) -> usize {
        (std::mem::size_of::<u64>()) * count + std::mem::size_of::<u64>()
    }
    fn calc_needed_bytes_vals(count: usize) -> usize {
        (std::mem::size_of::<u64>() + size_of::<SequenceNr>()) * count + std::mem::size_of::<u64>()
    }

    fn reallocate_keys(&mut self, new_count: usize) -> Result<()> {
        //println!("Reallocating keys to {}", new_count);
        Self::reallocate(
            &mut self.keys,
            &mut *self.key_file,
            Self::calc_needed_bytes_keys(new_count),
        )?;
        self.key_capacity = new_count;
        Ok(())
    }
    fn reallocate_values(&mut self, new_count: usize) -> Result<()> {
        //println!("Reallocating values to {}", new_count);
        Self::reallocate(
            &mut self.vals,
            &mut *self.value_file,
            Self::calc_needed_bytes_vals(new_count),
        )?;
        self.value_capacity = new_count;
        Ok(())
    }

    fn reallocate(mmap: &mut DiskMmapHandle, file: &mut dyn DiskFile, new_bytes: usize) -> Result<()> {
        file.set_len(new_bytes as u64)?;
        file.remap(mmap, new_bytes as u64)?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use crate::buffer::message_dependency_tracker::MessageDependencyTracker;
    use crate::buffer::message_dependency_tracker::MmapMessageDependencyTracker;
    use crate::{SequenceNr, Target};
    use std::path::Path;
    use std::time::Instant;
    use crate::disk_abstraction::StandardDisk;

    #[test]
    fn smoke_deptrack() {
        let mut tracker =
            MmapMessageDependencyTracker::new(&mut StandardDisk, &Target::CreateNewOrOverwrite("test_smoke_deptrack.bin".into()))
                .unwrap();
        tracker.record_dependency(SequenceNr::from_index(1), SequenceNr::from_index(2));

        let result: Vec<_> = tracker.read_dependency(SequenceNr::from_index(1)).collect();
        assert_eq!(result, vec![SequenceNr::from_index(2)]);
    }

    #[test]
    fn smoke_deptrack_many() {
        let mut tracker =
            MmapMessageDependencyTracker::new(&mut StandardDisk, &Target::CreateNewOrOverwrite("test_smoke_deptrack_many.bin".into()))
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
