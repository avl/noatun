use crate::disk_access::{DiskAccessor, FileMappingTrait};
use crate::{ Target};
use anyhow::{Context, Result, anyhow, bail};
use fs2::FileExt;
use memmap2::MmapMut;
use std::alloc::Layout;
use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{Cursor, ErrorKind, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsFd, RawFd};
use std::path::{Path, PathBuf};
use std::ptr::null_mut;
use std::rc::Rc;
use std::slice;
use std::sync::{Arc, MutexGuard};
/* TODO
compile_error!("Before the vacation

you just got it compiling again after introducing a memory-only backend, that should be
compatible with miri.

You also changed the main implementation of the message store to the file-based (mmap) one.

Todo:

1: Verify the new backend (message_store)

2: Verify the actual main orchestration logic

3: Run in miri

4: Add more tests, hammer it with chaos!

5: Add a HashMap-data type (probably be inspired by, or lend code from, HashBrown. Does it support
custom allocators?)

6: Try building simple apps. Does it appear convenient?

7: Can we do something to the chain-of-dependent updates problem? Main snapshots?

8: Can we trim messages once certain parts of them turn out unused?

9: Check if we can publish this, or if employer wants it?

")
*/

/// Use to abstract away the concrete mmap and disk io implementations.
/// This can be used to easily change implementations of these, but more
/// importantly, it allows us to run under miri
// TODO: This type should probably not be public
pub trait Disk {
    /// Open a file as specified by base directory in 'Target', and file name `file`.
    /// If the file is smaller than 'min_size', it will be extended with zeroes to that size.
    fn open_file(
        &mut self,
        target: &Target,
        file: &str,
        min_size: usize,
        max_size: usize,
    ) -> Result<DiskAccessor>;
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
struct InMemoryGrowableFileMapping {
    backing: Rc<RefCell<InMemoryGrowableFileMappingData>>,
}

impl InMemoryGrowableFileMappingData {
    fn map_mut(&self) -> &mut [u8] {
        let slice = unsafe { slice::from_raw_parts_mut(self.data, self.used_len) };
        slice
    }
}
impl InMemoryGrowableFileMapping {
    fn grow(&self, new_size: usize) -> Result<()> {
        let mut tself = self.backing.borrow_mut();
        if new_size > tself.total_data_len {
            bail!(
                "Cannot grow to {}, because max size is {}",
                new_size,
                tself.total_data_len
            );
        }
        if new_size > tself.used_len {
            tself.used_len = new_size;
        }
        Ok(())
    }

    fn get_ptr(&self) -> *mut u8 {
        self.backing.borrow().data
    }

    fn usable_len(&self) -> usize {
        self.backing.borrow().used_len
    }

    fn flush_range(&self, offset: usize, len: usize) -> Result<()> {
        Ok(())
    }

    fn flush_all(&self) -> Result<()> {
        Ok(())
    }

    fn truncate(&self, len: usize) -> Result<()> {
        let mut backing = self.backing.borrow_mut();
        if backing.used_len > len {
            unsafe { backing.map_mut()[len..].fill(0) };
            backing.used_len = len;
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct InMemoryDisk {
    files: HashMap<PathBuf, Rc<RefCell<InMemoryGrowableFileMappingData>>>,
}


impl Disk for InMemoryDisk {
    fn open_file(
        &mut self,
        target: &Target,
        file: &str,
        min_size: usize,
        max_size: usize,
    ) -> anyhow::Result<DiskAccessor> {
        //std::fs::create_dir_all(&path).context("create database directory")?;
        let create = target.create();
        let overwrite = target.overwrite();
        let path = target.path().join(file);
        let data = if !create {
            let t = (*self.files.get(&path).unwrap()).clone();
            t.clone()
        } else {
            if !overwrite {
                if self.files.contains_key(&path) {
                    bail!("file already exists");
                }
            }
            let data_len = max_size;
            let new_layout = Layout::from_size_align(data_len as usize, 256).unwrap();
            let data_ptr = unsafe { std::alloc::alloc_zeroed(new_layout) };

            let data = Rc::new(RefCell::new(InMemoryGrowableFileMappingData {
                data: data_ptr,
                total_data_len: data_len,
                used_len: 0,
            }));
            self.files.insert(path.clone(), data.clone());
            data
        };

        let mapping = InMemoryGrowableFileMapping { backing: data };

        Ok(DiskAccessor::from_mapping(mapping))
    }
}

impl FileMappingTrait for InMemoryGrowableFileMapping {
    fn page_size(&self) -> usize {
        2 * 1024 * 1024
    }

    fn flush_all(&self) -> Result<()> {
        Ok(())
    }

    fn flush_range(&self, start: usize, len: usize) -> Result<()> {
        Ok(())
    }

    fn ptr(&self) -> *mut u8 {
        self.get_ptr()
    }

    fn len(&self) -> usize {
        self.usable_len()
    }

    fn maximum_size(&self) -> usize {
        self.backing.borrow().total_data_len
    }

    fn shrink_committed_mapping(&self, new_size: usize) -> Result<()> {
        self.truncate(new_size)
    }

    fn grow_committed_mapping(
        &self,
        new_size: usize,
        filename_for_diagnostics: &str,
    ) -> Result<()> {
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
    ) -> Result<DiskAccessor> {
        let path = target.path().join(format!("{}.bin", file));
        let mut overwrite = target.overwrite();
        let mut create = target.create();

        if !std::fs::metadata(&path).is_ok() {
            overwrite = true;
        }

        let mapping = DiskAccessor::new(target, file, min_size, max_size)?;

        Ok(mapping)
    }
}