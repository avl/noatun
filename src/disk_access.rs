use crate::platform_specific::FileMapping;
use crate::Target;
use anyhow::{bail, Context, Result};
use bytemuck::Pod;
use std::fmt::{Debug, Formatter};
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::slice;

pub trait FileBackend {
    fn page_size(&self) -> usize;
    fn flush_all(&self) -> Result<()>;
    fn flush_range(&self, start: usize, len: usize) -> Result<()>;

    fn ptr(&self) -> *mut u8;

    /// Returns the usable size of this file mapping.
    /// This should be the size of the file on disk.
    fn len(&self) -> usize;

    fn maximum_size(&self) -> usize;

    fn shrink_committed_mapping(&mut self, new_size: usize) -> Result<()>;

    fn grow_committed_mapping(&mut self, new_size: usize) -> Result<()>;

    fn try_lock_exclusive(&self) -> Result<()>;
}

pub(crate) struct FileAccessor {
    mapping: Box<dyn FileBackend + Send>,
    /// This is the start of the memory-map.
    /// I.e, this points at the header.
    /// The actual contents start after the header.
    ptr: *mut u8,
    /// This is the size of the memory mapping. I.e, this value includes the size
    /// of the header. To get the payload/client byte count, subtract HEADER_SIZE
    committed_size: usize,
    seek_pos: usize,
}
unsafe impl Send for FileAccessor {}
unsafe impl Sync for FileAccessor {}

pub(crate) struct ReadonlyFileAccessor<'a> {
    ptr: *mut u8,
    size: usize,
    seek_pos: usize,
    phantom: PhantomData<&'a ()>,
}
impl Read for ReadonlyFileAccessor<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.seek_pos == self.size {
            return Ok(0);
        }
        let getnow = (self.size - self.seek_pos).min(buf.len());
        let m = self.map();
        buf[0..getnow].copy_from_slice(&m[self.seek_pos..self.seek_pos + getnow]);
        self.seek_pos += getnow;
        Ok(getnow)
    }
}
impl ReadonlyFileAccessor<'_> {
    pub(crate) fn map(&self) -> &[u8] {
        let used = self.size;
        unsafe { slice::from_raw_parts(self.ptr.wrapping_add(FileAccessor::HEADER_SIZE), used) }
    }

    pub fn access_pod<R: Pod>(&self, offset: usize) -> Result<&R> {
        if self.seek_pos + size_of::<R>() > self.size {
            bail!("requested number of bytes not available in file");
        }
        let raw = unsafe {
            slice::from_raw_parts(
                self.ptr.wrapping_add(FileAccessor::HEADER_SIZE + offset),
                size_of::<R>(),
            )
        };
        Ok(bytemuck::from_bytes(raw))
    }
    pub fn with_bytes<R>(&mut self, bytes: usize, mut f: impl FnMut(&[u8]) -> R) -> Result<R> {
        if self.seek_pos + bytes > self.size {
            bail!("requested number of bytes not available in file");
        }
        let data = &self.map()[self.seek_pos..self.seek_pos + bytes];
        let ret = f(data);
        self.seek_pos += bytes;
        Ok(ret)
    }
    pub fn with_bytes_at<R>(
        &mut self,
        offset: usize,
        bytes: usize,
        mut f: impl FnMut(&[u8]) -> R,
    ) -> Result<R> {
        if offset + bytes > self.size {
            bail!("requested number of bytes not available in file");
        }
        let data = &self.map()[offset..offset + bytes];
        let ret = f(data);
        Ok(ret)
    }
}
impl Seek for ReadonlyFileAccessor<'_> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(s) => {
                self.seek_pos = s.try_into().map_err(|_| {
                    std::io::Error::new(ErrorKind::InvalidInput, "invalid seek position")
                })?;
            }
            SeekFrom::End(e) => {
                if e == 0 {
                    self.seek_pos = self.size;
                } else {
                    self.seek_pos = self
                        .size
                        .try_into()
                        .ok()
                        .and_then(|x: i64| x.checked_sub(e))
                        .and_then(|x| x.try_into().ok())
                        .ok_or_else(|| {
                            std::io::Error::new(ErrorKind::InvalidInput, "invalid seek position")
                        })?;
                }
            }
            SeekFrom::Current(delta) => {
                self.seek_pos = self
                    .seek_pos
                    .try_into()
                    .ok()
                    .and_then(|x: i64| x.checked_add(delta))
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        std::io::Error::new(ErrorKind::InvalidInput, "invalid seek position")
                    })?;
            }
        }
        Ok(self.seek_pos as u64)
    }
}

impl Debug for FileAccessor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileAccessor({})", self.committed_size)
    }
}
impl Debug for ReadonlyFileAccessor<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadonlyFileAccessor({})", self.size)
    }
}

impl Seek for FileAccessor {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(s) => {
                self.seek_pos = s.try_into().map_err(|_| {
                    std::io::Error::new(ErrorKind::InvalidInput, "invalid seek position")
                })?;
            }
            SeekFrom::End(e) => {
                if e == 0 {
                    self.seek_pos = self.used_space();
                } else {
                    self.seek_pos = self
                        .used_space()
                        .try_into()
                        .ok()
                        .and_then(|x: i64| x.checked_sub(e))
                        .and_then(|x| x.try_into().ok())
                        .ok_or_else(|| {
                            std::io::Error::new(ErrorKind::InvalidInput, "invalid seek position")
                        })?;
                }
            }
            SeekFrom::Current(delta) => {
                self.seek_pos = self
                    .seek_pos
                    .try_into()
                    .ok()
                    .and_then(|x: i64| x.checked_add(delta))
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        std::io::Error::new(ErrorKind::InvalidInput, "invalid seek position")
                    })?;
            }
        }
        Ok(self.seek_pos as u64)
    }
}

impl Read for FileAccessor {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.seek_pos == self.used_space() {
            return Ok(0);
        }
        let getnow = (self.used_space() - self.seek_pos).min(buf.len());
        let m = self.map();
        buf[0..getnow].copy_from_slice(&m[self.seek_pos..self.seek_pos + getnow]);
        self.seek_pos += getnow;
        Ok(getnow)
    }
}

impl FileAccessor {
    pub fn write_uninit(&mut self, buf: &[MaybeUninit<u8>]) -> Result<()> {
        if self.seek_pos + buf.len() > self.used_space() {
            self.grow(self.seek_pos + buf.len())
                .map_err(|e| std::io::Error::new(ErrorKind::Other, e))?;
        }

        let dest = unsafe {
            slice::from_raw_parts_mut(
                self.ptr
                    .wrapping_add(Self::HEADER_SIZE)
                    .wrapping_add(self.seek_pos)
                    as *mut MaybeUninit<u8>,
                buf.len(),
            )
        };
        dest.copy_from_slice(buf);
        self.seek_pos += buf.len();
        Ok(())
    }
}
impl Write for FileAccessor {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.seek_pos + buf.len() > self.used_space() {
            self.grow(self.seek_pos + buf.len())
                .map_err(|e| std::io::Error::new(ErrorKind::Other, e))?;
        }

        let dest = unsafe {
            slice::from_raw_parts_mut(
                self.ptr
                    .wrapping_add(Self::HEADER_SIZE)
                    .wrapping_add(self.seek_pos),
                buf.len(),
            )
        };
        dest.copy_from_slice(buf);
        self.seek_pos += buf.len();

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl FileAccessor {
    /// Use a header size that ensures start of payload is at least 16 byte aligned
    /// 16 bytes is enough for any type used by noatun itself. Client data can be
    /// aligned at even larger values, and this is supported, byt will waste some space.
    const HEADER_SIZE: usize = 16;

    pub fn seek_to(&mut self, offset: usize) -> std::io::Result<()> {
        self.seek(SeekFrom::Start(offset as u64))?;
        Ok(())
    }

    pub(crate) fn readonly(&self) -> ReadonlyFileAccessor {
        ReadonlyFileAccessor {
            ptr: self.ptr,
            size: self.used_space(),
            seek_pos: self.seek_pos,
            phantom: Default::default(),
        }
    }

    /// Does _not_ use or update seek position
    pub fn access_pod<R: Pod>(&self, offset: usize) -> Result<&R> {
        if offset + size_of::<R>() > self.used_space() {
            bail!("requested number of bytes not available in file");
        }
        let raw = unsafe {
            slice::from_raw_parts(
                self.ptr.wrapping_add(FileAccessor::HEADER_SIZE + offset),
                size_of::<R>(),
            )
        };
        Ok(bytemuck::from_bytes(raw))
    }
    /// Does _not_ use or update seek position TODO: This is unsafe as h***! Mark as unsafe!
    pub fn access_pod_mut<R: Pod>(&self, offset: usize) -> Result<&mut R> {
        if offset + size_of::<R>() > self.used_space() {
            bail!("requested number of bytes not available in file");
        }
        let raw = unsafe {
            slice::from_raw_parts_mut(
                self.ptr.wrapping_add(FileAccessor::HEADER_SIZE + offset),
                size_of::<R>(),
            )
        };
        Ok(bytemuck::from_bytes_mut(raw))
    }

    #[inline(always)]
    pub(crate) fn used_space(&self) -> usize {
        unsafe { *(self.ptr as *const usize) }
    }

    /// Update the used size. Note: This must not exceed
    /// committed_len
    pub(crate) fn set_used_space(&self, new_value: usize) {
        if self.committed_size == 0 {
            dbg!(new_value,Self::HEADER_SIZE, self.committed_size);
        }
        assert!(new_value.checked_add(Self::HEADER_SIZE).expect("arithmetic overflow") <= self.committed_size);
        unsafe {
            *(self.mapping.ptr() as *mut usize) = new_value;
        }
    }

    pub(crate) fn set_used_space_to_full_file(&mut self) {
        self.set_used_space(self.committed_size.saturating_sub(Self::HEADER_SIZE));
    }

    #[inline(always)]
    pub(crate) fn free_space(&self) -> usize {
        self.committed_size - Self::HEADER_SIZE - self.used_space()
    }

    pub(crate) fn on_disk_size(&self) -> usize {
        self.committed_size
    }

    pub(crate) fn map_const_ptr(&self) -> *const u8 {
        self.ptr.wrapping_add(Self::HEADER_SIZE)
    }
    pub(crate) fn map_const_ptr_uninit(&self) -> *const MaybeUninit<u8> {
        (self.ptr as *const MaybeUninit<u8>).wrapping_add(Self::HEADER_SIZE)
    }
    #[inline]
    pub(crate) fn map_mut_ptr(&self) -> *mut u8 {
        self.ptr.wrapping_add(Self::HEADER_SIZE)
    }
    #[inline]
    pub(crate) fn map_mut_ptr_uninit(&self) -> *mut MaybeUninit<u8> {
        (self.ptr as *mut MaybeUninit<u8>).wrapping_add(Self::HEADER_SIZE)
    }

    pub(crate) fn map_all_raw(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr.wrapping_add(Self::HEADER_SIZE), self.committed_size.saturating_sub(Self::HEADER_SIZE)) }
    }

    pub(crate) fn map(&self) -> &[u8] {
        let used = self.used_space();
        unsafe { slice::from_raw_parts(self.ptr.wrapping_add(Self::HEADER_SIZE), used) }
    }
    pub(crate) fn map_mut(&mut self) -> &mut [u8] {
        let used = self.used_space();
        unsafe { slice::from_raw_parts_mut(self.ptr.wrapping_add(Self::HEADER_SIZE), used) }
    }

    pub(crate) fn from_mapping(mut mapping: impl FileBackend + Send + 'static) -> Self {
        let initial_len = Self::HEADER_SIZE;
        mapping.grow_committed_mapping(initial_len).unwrap();
        Self {
            ptr: mapping.ptr(),
            committed_size: mapping.len(),
            mapping: Box::new(mapping),
            seek_pos: 0,
        }
    }

    pub(crate) fn new(
        target: &Target,
        file: &str,
        initial_size: usize,
        max_size: usize,
    ) -> Result<Self> {
        if max_size == 0 {
            bail!("max_size must not be 0");
        }

        if initial_size > max_size {
            bail!(
                "initial_size ({}) must not be greater than max_size ({})",
                initial_size,
                max_size
            );
        }

        create_dir_all(target.path()).context("creating directory for data file")?;

        let path = target.path().join(format!("{}.bin", file));
        let mut overwrite = target.overwrite();
        let create = target.create();

        if std::fs::metadata(&path).is_err() {
            overwrite = true;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(overwrite)
            .open(&path)
            .with_context(|| format!("opening file {:?}", path))?;

        let page_size = FileMapping::page_size();

        let mut len = File::metadata(&file)?.len() as usize;
        //let new_used_size = len.saturating_sub(Self::HEADER_SIZE).max(initial_size);
        if len < initial_size + Self::HEADER_SIZE || len % page_size != 0 {
            len = len
                .max(initial_size + Self::HEADER_SIZE)
                .next_multiple_of(page_size);
            file.set_len((len) as u64)
                .with_context(|| format!("Resizing file {:?} to {} bytes", &path, len))?;
            file.sync_all().context("fsync")?;
        }

        let filename = path.to_string_lossy();
        let mapping = FileMapping::new(
            file,
            len,
            (max_size + Self::HEADER_SIZE).next_multiple_of(page_size),
            &filename,
        )
        .with_context(|| format!("failed to memory map file {}", filename))?;

        let temp = FileAccessor {
            committed_size: mapping.committed_size(),
            ptr: mapping.ptr(),
            mapping: Box::new(mapping),
            seek_pos: 0,
        };
        let claimed_used_size = temp.used_space();
        let new_used_size = claimed_used_size.min(len.saturating_sub(Self::HEADER_SIZE));
        temp.set_used_space(new_used_size);
        Ok(temp)
    }
}

impl FileAccessor {
    pub fn try_lock_exclusive(&self) -> Result<()> {
        self.mapping.try_lock_exclusive()
    }
    pub fn write_zeroes(&mut self, bytes: usize) -> Result<()> {
        if self.seek_pos + bytes > self.used_space() {
            self.grow(self.seek_pos + bytes)?;
        }

        unsafe {
            slice::from_raw_parts_mut(
                self.ptr
                    .wrapping_add(self.seek_pos)
                    .wrapping_add(Self::HEADER_SIZE),
                bytes,
            )
            .fill(0)
        }
        self.seek_pos += bytes;
        Ok(())
    }

    pub fn copy_to(&mut self, bytes: usize, target: &mut FileAccessor) -> Result<()> {
        if self.seek_pos + bytes > self.used_space() {
            bail!("requested number of bytes not available in file");
        }
        let src_buf = &self.map()[self.seek_pos..self.seek_pos + bytes];

        if target.seek_pos + bytes > target.used_space() {
            target.grow(target.seek_pos.checked_add(bytes).unwrap())?;
        }
        let target_seek_pos = target.seek_pos;
        let dst_buf = &mut target.map_mut()[target_seek_pos..target_seek_pos + bytes];

        dst_buf.copy_from_slice(src_buf);

        self.seek_pos += bytes;
        target.seek_pos += bytes;

        Ok(())
    }

    /// Give the closure _all_ bytes in the file, without taking into account, or affecting,
    /// the seek position. This includes all bytes in the physical file, except the header.
    /// Specifically, it includes *unused* parts of the file (as if the HEADER was claiming
    /// the entire physical file was used)
    pub fn with_all_bytes<R>(&mut self, mut f: impl FnMut(&[u8]) -> R) -> Result<R> {
        //TODO: This can't fail, doesn't need to return result!
        let data = &self.map_all_raw();
        let ret = f(data);
        Ok(ret)
    }

    /// Read the given number of bytes, and make them available to the closure.
    /// This does advance the file pointer (like all other read methods)
    pub fn with_bytes<R>(&mut self, bytes: usize, mut f: impl FnMut(&[u8]) -> R) -> Result<R> {
        if self.seek_pos + bytes > self.used_space() {
            bail!(
                "requested number of bytes not available in file. Requested: {}, had: {} (seek pos: {})",
                bytes,
                self.used_space().saturating_sub(self.seek_pos),
                self.seek_pos
            );
        }
        let data = &self.map()[self.seek_pos..self.seek_pos + bytes];
        let ret = f(data);
        self.seek_pos += bytes;
        Ok(ret)
    }
    pub fn with_bytes_at<R>(
        &mut self,
        offset: usize,
        bytes: usize,
        mut f: impl FnMut(&[u8]) -> R,
    ) -> Result<R> {
        if offset + bytes > self.used_space() {
            bail!(
                "requested number of bytes not available in file. Requested: {}, had: {} (seek pos: {})",
                bytes,
                self.used_space().saturating_sub(self.seek_pos),
                self.seek_pos
            );
        }
        let data = &self.map()[offset..offset + bytes];
        let ret = f(data);
        Ok(ret)
    }

    pub(crate) fn grow(&mut self, new_size: usize) -> Result<()> {
        if new_size + Self::HEADER_SIZE > self.committed_size {
            let max_size = self.mapping.maximum_size();
            if new_size + Self::HEADER_SIZE >= max_size {
                bail!(
                    "maximum file size exceeded. Requested new size: {}. Max size: {}",
                    new_size + Self::HEADER_SIZE,
                    max_size
                );
            }

            let new_file_size = ((self.committed_size + new_size + Self::HEADER_SIZE) * 2)
                .next_multiple_of(self.mapping.page_size())
                .min(max_size);
            self.mapping.grow_committed_mapping(new_file_size)?;
            self.committed_size = new_file_size;
        }
        self.set_used_space(new_size);
        Ok(())
    }

    pub(crate) fn flush_range(&self, offset: usize, len: usize) -> Result<()> {
        if offset < self.mapping.page_size() {
            self.mapping
                .flush_range(0, offset + Self::HEADER_SIZE + len)?;
        } else {
            self.mapping.flush_range(offset + Self::HEADER_SIZE, len)?;
            self.mapping.flush_range(0, Self::HEADER_SIZE)?;
        }
        Ok(())
    }

    pub(crate) fn flush_all(&self) -> Result<()> {
        self.mapping.flush_all()?;
        Ok(())
    }

    /// This does not require '&mut self', and is still safe. But bear in mind
    /// that if you leave references pointed beyond the new end of file, and then
    /// later expand again and fill that with other data, your original data will be
    /// wrong, just not in a UB-way.
    pub(crate) fn fast_truncate(&self, new_size: usize) {
        if self.used_space() > new_size {
            self.set_used_space(new_size);
        }
    }

    /// This requires &mut self, since it will invalidate old references
    pub(crate) fn truncate(&mut self, new_size: usize) -> Result<()> {
        let new_alloc_size =
            (new_size + Self::HEADER_SIZE).next_multiple_of(self.mapping.page_size());
        if new_alloc_size < self.committed_size {
            self.mapping.shrink_committed_mapping(new_alloc_size)?;
            self.committed_size = new_alloc_size;
        }
        self.set_used_space(new_size);
        Ok(())
    }
}
