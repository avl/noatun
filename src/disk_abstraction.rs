use std::alloc::Layout;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Cursor, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::ptr::null_mut;
use std::rc::Rc;
use anyhow::{anyhow, bail, Context, Result};
use fs2::FileExt;
use memmap2::MmapMut;

/// Use to abstract away the concrete mmap and disk io implementations.
/// This can be used to easily change implementations of these, but more
/// importantly, it allows us to run under miri
pub trait Disk {
    type File: DiskFile;
    type Mmap: DiskMmap;
    fn open_file(&mut self, path: &Path, create: bool, overwrite: bool) -> Result<Self::File>;
    fn mmap(&mut self, file: &Self::File) -> Result<Self::Mmap>;
}
pub trait DiskFile : Seek+ Write + Read {
    fn set_len(&mut self, len: u64) -> Result<()>;
    fn sync_all(&mut self) -> Result<()>;
    fn try_lock_exclusive(&mut self) -> Result<()>;
    fn len(&self) -> Result<usize>;
}
pub trait DiskMmap {
    fn map(&self) -> &[u8];
    fn map_mut(&mut self) -> &mut [u8];
    fn len(&self) -> usize;
    fn flush_range(&mut self, offset: usize, len: usize) -> Result<()>;
}
pub struct StandardDisk;


struct InMemoryFile {
    data: *mut u8,
    data_size: usize,
    position: usize,
    locked: bool,
    mmaped: bool,

}

#[derive(Clone)]
pub struct InMemoryFileRef(Rc<RefCell<InMemoryFile>>);
#[derive(Default)]
pub struct InMemoryDisk {
    files: HashMap<PathBuf, InMemoryFileRef>,
}
pub struct InMemoryMmap {
    file: InMemoryFileRef,
    size_of_map: usize
}

impl Read for InMemoryFileRef {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut tself = self.0.borrow_mut();

        assert!(tself.position + buf.len() <= tself.data_size);
        unsafe {
            std::ptr::copy(tself.data.wrapping_add(tself.position), buf.as_mut_ptr(), buf.len());
        }
        tself.position += buf.len();
        Ok(buf.len())
    }
}
impl Write for InMemoryFileRef {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut tself = self.0.borrow_mut();

        // TODO: Maybe optimize this so it doesn't have to realloc on every
        // write that exceeds size. On the other hand, BufStream protects us from
        // the worst amount of reallocs
        if tself.position + buf.len() > tself.data_size {
            let newlen = tself.position+ buf.len();
            drop(tself);
            self.set_len( (newlen) as u64).unwrap();
        }
        let mut tself = self.0.borrow_mut();

        unsafe {
            std::ptr::copy(buf.as_ptr(), tself.data.wrapping_add(tself.position),buf.len());
        }
        tself.position += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Seek for InMemoryFileRef {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let mut tself = self.0.borrow_mut();
        match pos {
            SeekFrom::Start(p) => {
                if p as usize >= tself.data_size  {
                    return Err(std::io::Error::new(ErrorKind::Other, "SeekFrom::Start - Can't seek after end (not supported by this test routine)"));
                }
                tself.position = p as usize;
            }
            SeekFrom::End(_) => {
                tself.position = tself.data_size;
            }
            SeekFrom::Current(d) => {
                if d== 0 {
                    return Ok(tself.position as u64);
                }
                let new_pos = tself.position.checked_add_signed(d as isize).unwrap();
                if new_pos >= tself.data_size {
                    return Err(std::io::Error::new(ErrorKind::Other, "SeekFrom::Current - Can't seek after end (not supported by this test routine)"));
                }
                tself.position = new_pos;

            }
        }
        Ok(tself.position as u64)
    }
}

impl DiskMmap for InMemoryMmap {
    fn map(&self) -> &[u8] {
        let tfile = self.file.0.borrow();
        let data_p = tfile.data;
        assert!(self.size_of_map <= tfile.data_size);
        unsafe { std::slice::from_raw_parts(data_p, self.size_of_map) }
    }

    fn map_mut(&mut self) -> &mut [u8] {
        let tfile = self.file.0.borrow();
        let data_p = tfile.data;
        assert!(self.size_of_map <= tfile.data_size);
        unsafe { std::slice::from_raw_parts_mut(data_p, self.size_of_map) }
    }

    fn len(&self) -> usize {
        self.size_of_map
    }

    fn flush_range(&mut self, offset: usize, len: usize) -> Result<()> {
        Ok(())
    }
}
impl Disk for InMemoryDisk {
    type File = InMemoryFileRef;
    type Mmap = InMemoryMmap;

    fn open_file(&mut self, path: &Path, create: bool, mut overwrite: bool) -> anyhow::Result<Self::File> {


        //std::fs::create_dir_all(&path).context("create database directory")?;
        if !create {
            let t = (*self.files.get(path).unwrap()).clone();
            return Ok (t);
        } else {
            if !overwrite {
                if self.files.contains_key(path) {
                    bail!("file already exists");
                }
            }

        }



        Ok(InMemoryFileRef(Rc::new(RefCell::new(InMemoryFile {
            data: null_mut(),
            data_size: 0,
            position: 0,
            locked: false,
            mmaped: false,
        }))))
    }

    fn mmap(&mut self, file: &Self::File) -> Result<Self::Mmap> {
        let size = file.0.borrow().data_size;

        Ok(InMemoryMmap{
            file: file.clone(),
            size_of_map: size,
        })
    }
}
impl Drop for InMemoryFile {
    fn drop(&mut self) {
        if !self.data.is_null() {
            let layout = Layout::from_size_align(self.data_size, 256).unwrap();
            unsafe { std::alloc::dealloc(self.data, layout) }
        }
    }
}
impl DiskFile for InMemoryFileRef {
    fn set_len(&mut self, len: u64) -> Result<()> {
        let new_layout = Layout::from_size_align(len as usize, 256).unwrap();
        let new_data = unsafe { std::alloc::alloc_zeroed(new_layout) };

        let theself = self.0.borrow_mut();
        unsafe { std::ptr::copy(theself.data, new_data, theself.data_size) };

        if !theself.data.is_null() {
            let old_layout = Layout::from_size_align(theself.data_size, 256).unwrap();
            unsafe { std::alloc::dealloc(theself.data, old_layout) }
        }
        drop(theself);
        let mut tself = self.0.borrow_mut();
        tself.data_size = len as usize;
        tself.data = new_data;
        Ok(())
    }

    fn sync_all(&mut self) -> Result<()> {
        Ok(())
    }

    fn try_lock_exclusive(&mut self) -> Result<()> {
        let mut tself = self.0.borrow_mut();
        if tself.locked {
            bail!("Already locked");
        }
        tself.locked = true;
        Ok(())
    }

    fn len(&self) -> Result<usize> {
        Ok(self.0.borrow().data_size)
    }
}

impl Disk for StandardDisk {
    type File = std::fs::File;
    type Mmap = MmapMut;

    fn open_file(&mut self, path: &Path, create: bool, mut overwrite: bool) -> Result<Self::File> {
        if !std::fs::metadata(path).is_ok() {
            overwrite = true;
        }
        Ok(OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(overwrite)
            .open(path)?)
    }

    fn mmap(&mut self, file: &Self::File) -> Result<Self::Mmap> {
        Ok(unsafe { MmapMut::map_mut(file)? })
    }
}
#[deny(unconditional_recursion)]
impl DiskFile for File {
    fn set_len(&mut self, len: u64) -> Result<()> {
        Ok(File::set_len(self, len)?)
    }

    fn sync_all(&mut self) -> Result<()> {
        Ok(File::sync_all(self)?)
    }

    fn try_lock_exclusive(&mut self) -> Result<()> {
        Ok(FileExt::try_lock_exclusive(self)?)
    }

    fn len(&self) -> Result<usize> {
        Ok(File::metadata(self)?.len().try_into()?)
    }
}
#[deny(unconditional_recursion)]
impl DiskMmap for MmapMut {
    fn map(&self) -> &[u8] {
        self
    }

    fn map_mut(&mut self) -> &mut [u8] {
        self
    }

    fn len(&self) -> usize {
        self.map().len()
    }

    fn flush_range(&mut self, offset: usize, len: usize) -> Result<()> {
        Ok(MmapMut::flush_range(self, offset, len)?)
    }
}

