use std::alloc::Layout;
use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Cursor, ErrorKind, Read, Seek, SeekFrom, Write};
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};
use std::ptr::null_mut;
use std::rc::Rc;
use anyhow::{anyhow, bail, Context, Result};
use buf_read_write::BufStream;
use fs2::FileExt;
use memmap2::MmapMut;

/* TODO
compile_error!("Before the vacation

you just got it compiling again after introducing a memory-only backend, that should be
compatible with miri.

You also changed the main implementation of the message store to the file-based (mmap) one.

Todo:

1: Verify the new backend (on_disk_message_store.rs)

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
pub trait Disk {
    type File: DiskFile+'static;
    type Mmap: DiskMmap+'static;
    fn open_file(&mut self, path: &Path, create: bool, overwrite: bool) -> Result<Self::File>;
}
pub trait DiskFile : Seek+ Write + Read {
    fn set_len(&mut self, len: u64) -> Result<()>;

    /// Write all zeroes to the entire file, without changing its size
    fn clear(&mut self) -> Result<()>;
    fn mmap(&mut self) -> Result<DiskMmapHandle>;
    fn remap(&mut self, mmap: &mut DiskMmapHandle, new_size: u64) -> Result<()>;
    fn sync_all(&mut self) -> Result<()>;
    fn try_lock_exclusive(&mut self) -> Result<()>;
    fn len(&self) -> Result<usize>;
}
pub trait DiskMmap : std::any::Any {
    fn map(&self) -> &[u8];
    fn map_mut(&mut self) -> &mut [u8];
    fn as_any(&mut self) -> &mut dyn Any;
    fn len(&self) -> usize;
    fn flush_range(&mut self, offset: usize, len: usize) -> Result<()>;
}

pub struct DiskMmapHandle {
    ptr: *mut u8,
    len: usize,
    boxed: Box<dyn DiskMmap>,
}
impl DiskMmapHandle {
    #[inline(always)]
    pub fn map(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
    #[inline(always)]
    pub fn map_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
    #[inline(always)]
    pub fn map_mut_ptr(&self) -> *mut u8 {
        self.ptr

    }
    #[inline(always)]
    pub fn map_const_ptr(&self) -> *const u8 {
        self.ptr

    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn flush_range(&mut self, offset: usize, len: usize) -> Result<()> {
        self.boxed.flush_range(offset, len)
    }
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

    fn as_any(&mut self) -> &mut dyn Any {
        self
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

    fn mmap(&mut self) -> Result<DiskMmapHandle> {
        let size = self.0.borrow().data_size;
        let boxed = Box::new(InMemoryMmap{
            file: self.clone(),
            size_of_map: size,
        });

        let tfile = self.0.borrow();
        let ptr = tfile.data;
        let len = tfile.data_size;
        Ok(DiskMmapHandle {
            boxed,
            ptr,
            len,
        })
    }

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

    fn remap(&mut self, mmap: &mut DiskMmapHandle, new_size: u64) -> Result<()> {
        todo!()
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

    fn clear(&mut self) -> Result<()> {
        // Zero all mem!
        todo!()
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
            .open(path)
            .with_context(||format!("opening file {:?}", path))
            ?)
    }

}
#[deny(unconditional_recursion)]
impl DiskFile for File {
    fn set_len(&mut self, len: u64) -> Result<()> {
        Ok(File::set_len(self, len)?)
    }

    fn clear(&mut self) -> Result<()> {
        let len = self.len()?;
        let mut stream = BufStream::new(self)?;
        stream.seek(SeekFrom::Start(0))?;
        stream.write_zeroes(len)?;
        stream.flush()?;
        Ok(())
    }


    fn mmap(&mut self) -> Result<DiskMmapHandle> {
        let mut boxed = Box::new(unsafe { MmapMut::map_mut(&self.as_fd())?} );

        let len = boxed.len();
        let ptr = boxed.as_mut_ptr();


        Ok(DiskMmapHandle {
            boxed,
            ptr,
            len,
        })
    }

    fn remap(&mut self, mmap: &mut DiskMmapHandle, new_size: u64) -> Result<()> {
        self.set_len(new_size)?;
        let inner = mmap.boxed.as_mut();
        let diskmmap: &mut MmapMut = inner.as_any().downcast_mut::<MmapMut>()
            .expect("MmapMut::downcast should always succeed");

        *diskmmap = unsafe { MmapMut::map_mut(&self.as_fd())?};
        assert_eq!(diskmmap.len(), new_size as usize);
        assert_eq!(diskmmap.map().len(), new_size as usize);
        mmap.len = new_size as usize;
        mmap.ptr = diskmmap.as_mut_ptr();
        println!("REmapped size {} {}", diskmmap.map().len(), diskmmap.len());
        Ok(())
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

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.map().len()
    }

    fn flush_range(&mut self, offset: usize, len: usize) -> Result<()> {
        Ok(MmapMut::flush_range(self, offset, len)?)
    }
}

