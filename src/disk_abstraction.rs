use crate::disk_access::{FileAccessor, FileBackend};
use crate::Target;
use anyhow::{bail, Result};
use std::alloc::Layout;
use std::slice;
/* TODO:

4: Add more tests, hammer it with chaos!
 a) More synch-tests!
 b) Recovery-test! Panic in middle of access (maybe add support for injecting panics in tests)

7: Can we do something to the chain-of-dependent updates problem? Main snapshots?
*/

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
    ) -> Result<FileAccessor>;
}
/*pub trait DiskFile: Seek + Write + Read {
    fn set_len(&mut self, len: u64) -> Result<()>;

    /// Write all zeroes to the entire file, without changing its size
    fn clear(&mut self) -> Result<()>;
    fn mmap(&mut self) -> Result<DiskMmapHandle>;
    fn remap(&mut self, mmap: &mut DiskMmapHandle, new_size: u64) -> Result<()>;
    fn sync_all(&mut self) -> Result<()>;
    fn try_lock_exclusive(&mut self) -> Result<()>;
    fn len(&self) -> Result<usize>;
}*/

pub trait GrowableFileMapping {}

pub struct StandardDisk;

/*struct InMemoryFile {
    data: *mut u8,
    data_size: usize,
    position: usize,
    locked: bool,
    mmaped: bool,
}

#[derive(Clone)]
pub struct InMemoryFileRef(Rc<RefCell<InMemoryFile>>);
*/

struct InMemoryGrowableFileMappingData {
    data: *mut u8,
    total_data_len: usize,
    used_len: usize,
}

unsafe impl Send for InMemoryGrowableFileMappingData {}
unsafe impl Sync for InMemoryGrowableFileMappingData {}
struct InMemoryGrowableFileMapping {
    backing: InMemoryGrowableFileMappingData,
}

impl InMemoryGrowableFileMappingData {
    #[allow(clippy::mut_from_ref)]
    fn map_mut(&self) -> &mut [u8] {
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

    fn flush_range(&self, _offset: usize, _len: usize) -> Result<()> {
        Ok(())
    }

    fn flush_all(&self) -> Result<()> {
        Ok(())
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
    ) -> anyhow::Result<FileAccessor> {
        let create = target.create();
        let data = if !create {
            panic!("Open-use case not supported for in-memory db");
        } else {
            let data_len = max_size;
            let new_layout = Layout::from_size_align(data_len, 256).unwrap();
            let data_ptr = unsafe { std::alloc::alloc_zeroed(new_layout) };

            let data = InMemoryGrowableFileMappingData {
                data: data_ptr,
                total_data_len: data_len,
                used_len: 0,
            };
            data
        };

        let mapping = InMemoryGrowableFileMapping { backing: data };

        Ok(FileAccessor::from_mapping(mapping))
    }
}

impl FileBackend for InMemoryGrowableFileMapping {
    fn page_size(&self) -> usize {
        2 * 1024 * 1024
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
    ) -> Result<FileAccessor> {
        let mapping = FileAccessor::new(target, file, min_size, max_size)?;
        Ok(mapping)
    }
}
