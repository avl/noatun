use crate::disk_access::{FileAccessor, FileBackend};
use crate::Target;
use anyhow::{bail, Result};
use std::alloc::Layout;
use std::slice;


/// The alignment that memory mappings will always have.
/// This is effectively the largest alignment easily supported by Noatun for
/// its materialized view. It is larger than any known CPU:s layout requirements.
pub const DISK_MAP_ALIGNMENT: usize = 256;

/// Use to abstract away the concrete mmap and disk io implementations.
/// This can be used to easily change implementations of these, but more
/// importantly, it allows us to run under miri
pub(crate) trait Disk {
    /// Open a file as specified by base directory in 'Target', and file name `file`.
    /// If the file is smaller than 'min_size', it will be extended with zeroes to that size.
    fn open_file(
        &mut self,
        target: &Target,
        file: &str,
        min_size: usize,
        max_size: usize,
        //metrics:
        name: &str,
        description: &str,
    ) -> Result<(FileAccessor, bool /*file existed*/)>;
}

pub struct StandardDisk;


struct InMemoryGrowableFileMappingData {
    data: *mut u8,
    total_data_len: usize,
    used_len: usize,
}

/// Safety: InMemoryGrowableFileMappingData contains nothing that can't be Send
unsafe impl Send for InMemoryGrowableFileMappingData {}
/// Safety: InMemoryGrowableFileMappingData contains nothing that can't be Sync
unsafe impl Sync for InMemoryGrowableFileMappingData {}
struct InMemoryGrowableFileMapping {
    backing: InMemoryGrowableFileMappingData,
}

impl InMemoryGrowableFileMappingData {
    #[allow(clippy::mut_from_ref)]
    fn map_mut(&self) -> &mut [u8] {
        // Safety: 'self.data' and 'self.used_len' are always a valid slice.
        let slice = unsafe { slice::from_raw_parts_mut(self.data, self.used_len) };
        slice
    }
}
impl InMemoryGrowableFileMapping {
    fn grow(&mut self, new_size: usize) -> Result<()> {
        if new_size > self.backing.total_data_len {
            bail!(
                "Cannot grow to {}, because max size is {}",
                new_size,
                self.backing.total_data_len
            );
        }
        if new_size > self.backing.used_len {
            self.backing.used_len = new_size;
        }
        Ok(())
    }

    fn get_ptr(&self) -> *mut u8 {
        self.backing.data
    }

    fn usable_len(&self) -> usize {
        self.backing.used_len
    }

    
    fn truncate(&mut self, len: usize) -> Result<()> {
        let backing = &mut self.backing;
        if backing.used_len > len {
            backing.map_mut()[len..].fill(0);
            backing.used_len = len;
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct InMemoryDisk {
    //files: HashMap<PathBuf, Rc<RefCell<InMemoryGrowableFileMappingData>>>,
}

impl Disk for InMemoryDisk {
    fn open_file(
        &mut self,
        target: &Target,
        _file: &str,
        _min_size: usize,
        max_size: usize,
        name: &str,
        description: &str,
    ) -> anyhow::Result<(FileAccessor, bool)> {
        let create = target.create();
        let data = if !create {
            panic!("Open-use case not supported for in-memory db");
        } else {
            let data_len = max_size;
            let new_layout = Layout::from_size_align(data_len, DISK_MAP_ALIGNMENT).unwrap();
            // Safety: new_layout is not zero-sized
            let data_ptr = unsafe { std::alloc::alloc_zeroed(new_layout) };

            let data = InMemoryGrowableFileMappingData {
                data: data_ptr,
                total_data_len: data_len,
                used_len: 0,
            };
            data
        };

        let mapping = InMemoryGrowableFileMapping { backing: data };

        Ok((FileAccessor::from_mapping(mapping, name, description), false))
    }
}

impl FileBackend for InMemoryGrowableFileMapping {
    fn page_size(&self) -> usize {
        4096
    }

    fn sync_all(&self) -> Result<()> {
        Ok(())
    }

    fn sync_range(&self, _start: usize, _len: usize) -> Result<()> {
        Ok(())
    }

    fn ptr(&self) -> *mut u8 {
        self.get_ptr()
    }

    fn len(&self) -> usize {
        self.usable_len()
    }

    fn maximum_size(&self) -> usize {
        self.backing.total_data_len
    }

    fn shrink_committed_mapping(&mut self, new_size: usize) -> Result<()> {
        self.truncate(new_size)
    }

    fn grow_committed_mapping(&mut self, new_size: usize) -> Result<()> {
        self.grow(new_size)?;
        Ok(())
    }

    fn try_lock_exclusive(&self) -> Result<()> {
        Ok(())
    }
}

impl Drop for InMemoryGrowableFileMappingData {
    fn drop(&mut self) {
        if !self.data.is_null() {
            let layout = Layout::from_size_align(self.total_data_len, 256).unwrap();
            // Safety: This is the same layout that was used when allocating
            unsafe { std::alloc::dealloc(self.data, layout) }
        }
    }
}

impl Disk for StandardDisk {
    fn open_file(
        &mut self,
        target: &Target,
        file: &str,
        min_size: usize,
        max_size: usize,
        name: &str,
        description: &str,
    ) -> Result<(FileAccessor, bool)> {
        let mapping = FileAccessor::new(target, file, min_size, max_size, name, description)?;
        Ok(mapping)
    }
}
