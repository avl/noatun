#![feature(test)]
#![allow(unused)]
#![allow(dead_code)]

extern crate test;

use crate::disk_abstraction::{Disk, InMemoryDisk, StandardDisk};
use crate::on_disk_message_store::OnDiskMessageStore;
use crate::platform_specific::{FileMapping, get_boot_time};
use anyhow::{Context, Result, bail};
pub use buffer::DatabaseContext;
use bumpalo::Bump;
use bytemuck::{Pod, Zeroable};
use fs2::FileExt;
use indexmap::IndexMap;
use memmap2::MmapMut;
use savefile_derive::Savefile;
use serde::{Deserialize, Serialize};
use serde_derive::{Deserialize, Serialize};
use sha2::digest::FixedOutput;
use sha2::{Digest, Sha256};
use std::cell::{Cell, OnceCell};
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::mem::{transmute, transmute_copy};
use std::ops::{Deref, Range};
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::ptr::null_mut;
use std::slice::SliceIndex;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub(crate) mod backing_store;

pub(crate) mod buffer;
mod disk_abstraction;
mod on_disk_message_store;
pub(crate) mod undo_store;

struct MessageComponent<const ID: u32, T> {
    value: Option<T>,
}

pub(crate) mod platform_specific {

    #[cfg(unix)]
    pub(crate) use unix::FileMapping;
    #[cfg(unix)]
    pub(crate) use unix::get_boot_time;

    #[cfg(unix)]
    mod unix {
        use crate::FileMappingTrait;
        use anyhow::{Context, Result, anyhow, bail};
        use fs2::FileExt;
        use std::cell::Cell;
        use std::cmp::max;
        use std::ffi::c_void;
        use std::fs::File;
        use std::os::fd::{AsRawFd, RawFd};
        use std::process::Command;
        use std::ptr::{null, null_mut};
        use std::sync::OnceLock;

        #[cfg(not(miri))]
        pub(crate) fn get_boot_time() -> String {
            let output = Command::new("who")
                .arg("-b")
                .output()
                .expect("failed to execute 'who -b' to determine boot time");
            if !output.status.success() {
                panic!("'who -b' command failed. This command is needed to determine boot time.");
            }

            let result = String::from_utf8_lossy(&output.stdout).to_string();
            if result.is_empty() {
                panic!("'who -b' command returned an empty string");
            }
            if !result.contains("system boot") {
                panic!("'who -b' command output did not contain the string 'system boot'");
            }
            result
        }
        #[cfg(miri)]
        pub(crate) fn get_boot_time() -> String {
            "system boot miri".to_string()
        }

        pub(crate) struct FileMapping {
            file: File,
            base_ptr: *mut u8,
            committed_size: Cell<usize>,
            total_size: usize,
        }

        impl Drop for FileMapping {
            fn drop(&mut self) {
                unsafe {
                    if libc::munmap(self.base_ptr as *mut _, self.total_size) != 0 {
                        eprintln!("munmap failed");
                    }
                }
            }
        }

        static PAGE_SIZE: OnceLock<usize> = OnceLock::new();

        impl FileMapping {
            pub fn page_size() -> usize {
                (*PAGE_SIZE.get_or_init(|| unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) as usize }))
                    .max(2usize * 1024 * 1024)
            }
            pub(crate) fn committed_size(&self) -> usize {
                self.committed_size.get()
            }
            pub fn new(
                file: File,
                min_initial_size: usize,
                max_size: usize,
                filename_for_diagnostics: &str,
            ) -> Result<FileMapping> {
                if max_size == 0 {
                    bail!("max_size cannot be 0");
                }
                let actual_page_size = Self::page_size();
                let actual_size = max_size.next_multiple_of(actual_page_size);

                assert_eq!(actual_size % actual_page_size, 0);

                let ptr = unsafe {
                    libc::mmap(
                        null_mut(),
                        max_size,
                        libc::PROT_READ | libc::PROT_WRITE,
                        libc::MAP_ANONYMOUS
                            | libc::MAP_SHARED
                            | libc::MAP_NORESERVE
                            | libc::MAP_HUGE_2MB,
                        -1,
                        0,
                    )
                };

                if ptr as usize == 0 || ptr as usize == usize::MAX {
                    bail!(
                        "mmap of file {} failed: {:x?}",
                        filename_for_diagnostics,
                        std::io::Error::from_raw_os_error(
                            std::io::Error::last_os_error().raw_os_error().unwrap()
                        )
                    );
                }

                let mut temp = FileMapping {
                    file: file,
                    base_ptr: ptr as *mut u8,
                    committed_size: Cell::new(0),
                    total_size: actual_size,
                };
                temp.grow_committed_mapping(min_initial_size, filename_for_diagnostics)?;
                Ok(temp)
            }
        }

        impl FileMappingTrait for FileMapping {
            fn page_size(&self) -> usize {
                Self::page_size()
            }
            fn flush_all(&self) -> Result<()> {
                self.flush_range(0, self.committed_size.get())
            }
            fn flush_range(&self, start: usize, len: usize) -> Result<()> {
                unsafe {
                    let start_rounded_down = if start % Self::page_size() == 0 {
                        start
                    } else {
                        start.next_multiple_of(Self::page_size()) - Self::page_size()
                    };
                    let rounding_delta = start - start_rounded_down;
                    if libc::msync(
                        self.base_ptr.wrapping_add(start_rounded_down) as *mut _,
                        len + rounding_delta,
                        libc::MS_SYNC,
                    ) != 0
                    {
                        // TODO: Provide filename here?
                        bail!(
                            "msync {}..{} of file {} failed: {:x?}",
                            start,
                            start + len,
                            "?",
                            std::io::Error::from_raw_os_error(
                                std::io::Error::last_os_error().raw_os_error().unwrap()
                            )
                        );
                    }
                }
                Ok(())
            }

            fn ptr(&self) -> *mut u8 {
                assert!(!self.base_ptr.is_null());
                self.base_ptr
            }

            /// Returns the usable size of this file mapping
            #[inline(always)]
            fn len(&self) -> usize {
                self.committed_size.get()
            }

            fn maximum_size(&self) -> usize {
                self.total_size
            }

            fn shrink_committed_mapping(&self, new_size: usize) -> Result<()> {
                assert_eq!(new_size % self.page_size(), 0);
                if new_size >= self.committed_size.get() {
                    return Ok(());
                }
                println!("Shrinking to {}", new_size);
                let prev_size = self.committed_size.get();
                let shrink_by = prev_size - new_size;
                let ptr = unsafe {
                    libc::mmap(
                        self.base_ptr.wrapping_add(new_size) as *mut _,
                        shrink_by,
                        libc::PROT_READ | libc::PROT_WRITE,
                        libc::MAP_FIXED
                            | libc::MAP_ANONYMOUS
                            | libc::MAP_SHARED
                            | libc::MAP_NORESERVE
                            | libc::MAP_HUGE_2MB,
                        -1,
                        0,
                    )
                };
                if ptr as usize == 0 || ptr as usize == usize::MAX {
                    bail!("while remapping, mmap failed");
                }

                self.file.set_len(new_size as u64)?;
                self.file.sync_all().context("fsync")?;

                Ok(())
            }

            fn grow_committed_mapping(
                &self,
                new_size: usize,
                filename_for_diagnostics: &str,
            ) -> Result<()> {
                assert_eq!(new_size % self.page_size(), 0);
                if new_size <= self.committed_size.get() {
                    return Ok(());
                }
                if new_size > self.total_size {
                    bail!(
                        "Attempt to grow file mapping size beyond configured maximum size: {} (new size: {})",
                        self.total_size,
                        new_size
                    );
                }

                self.file.set_len(new_size as u64)?;
                self.file.sync_all().context("fsync")?;

                let ptr = unsafe {
                    libc::mmap(
                        self.base_ptr as *mut _,
                        new_size,
                        libc::PROT_READ | libc::PROT_WRITE,
                        libc::MAP_FIXED | libc::MAP_SHARED | libc::MAP_HUGE_2MB,
                        self.file.as_raw_fd(),
                        0,
                    )
                };
                if ptr as usize == 0 || ptr as usize == usize::MAX {
                    bail!(
                        "while remapping, mmap of file {} failed",
                        filename_for_diagnostics
                    );
                }

                self.committed_size.set(new_size);

                Ok(())
            }

            fn try_lock_exclusive(&self) -> Result<()> {
                Ok(self.file.try_lock_exclusive()?)
            }
        }
    }
}

//TODO: Rename this
pub trait FileMappingTrait {
    fn page_size(&self) -> usize;
    fn flush_all(&self) -> Result<()>;
    fn flush_range(&self, start: usize, len: usize) -> Result<()>;

    fn ptr(&self) -> *mut u8;

    /// Returns the usable size of this file mapping.
    /// This should be the size of the file on disk.
    fn len(&self) -> usize;

    fn maximum_size(&self) -> usize;

    fn shrink_committed_mapping(&self, new_size: usize) -> Result<()>;

    fn grow_committed_mapping(&self, new_size: usize, filename_for_diagnostics: &str)
    -> Result<()>;

    fn try_lock_exclusive(&self) -> Result<()>;
}

pub(crate) mod growable_file_mapping {
    use crate::disk_abstraction::DiskMmapHandleLegacy;
    use crate::platform_specific::FileMapping;
    use crate::{FileMappingTrait, Target};
    use anyhow::{Context, Result, bail};
    use std::cell::Cell;
    use std::fmt::{Debug, Formatter};
    use std::fs::{File, OpenOptions, create_dir_all};
    use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
    use std::os::fd::AsRawFd;
    use std::ptr::slice_from_raw_parts_mut;
    use std::slice;
    /*pub trait GrowableFileMapping {
        //fn new(target: &Target, file: &str, initial_size: usize, max_size: usize) -> Result<Self> where Self:Sized;
        fn grow(&self, new_size: usize) -> Result<()>;

        fn get_ptr(&self) -> *mut u8;
        /// Actual count of bytes that may be accessed through `get_ptr`
        fn usable_len(&self) -> usize;



        fn flush_range(&self, offset: usize, len: usize) -> Result<()>;
        fn flush_all(&self) -> Result<()>;

        /// Truncate the file to the given size.
        /// This does nothing if the file is already this size or smaller.
        fn truncate(&self, len: usize) -> Result<()>;

    }*/

    //TODO: Rename
    // TODO: This type should probably not be public
    pub struct DiskMmapHandleNew {
        mapping: Box<dyn FileMappingTrait>,
        /// This is the start of the memory-map.
        /// I.e, this points at the header.
        /// The actual contents start after the header.
        ptr: *mut u8,
        /// This is the size of the memory mapping. I.e, this value includes the size
        /// of the header. To get the payload/client byte count, subtract HEADER_SIZE
        committed_size: Cell<usize>,
        seek_pos: usize,
    }

    impl Debug for DiskMmapHandleNew {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "DiskMmapHandleNew({})", self.committed_size.get())
        }
    }

    impl Seek for DiskMmapHandleNew {
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
                                std::io::Error::new(
                                    ErrorKind::InvalidInput,
                                    "invalid seek position",
                                )
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

    impl Read for DiskMmapHandleNew {
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
    impl Write for DiskMmapHandleNew {
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

    impl DiskMmapHandleNew {
        /// Use a header size that ensures start of payload is at least 16 byte aligned
        /// 16 bytes is enough for any type used by noatun itself. Client data can be
        /// aligned at even larger values, and this is supported, byt will waste some space.
        const HEADER_SIZE: usize = 16;

        #[inline(always)]
        pub(crate) fn used_space(&self) -> usize {
            unsafe { *(self.ptr as *const usize) }
        }

        /// Update the used size. Note: This must not exceed
        /// committed_len
        pub(crate) fn set_used_space(&self, new_value: usize) {
            assert!(new_value <= self.committed_size.get());
            *self.used_space_mut() = new_value;
        }

        fn used_space_mut(&self) -> &mut usize {
            unsafe { &mut *(self.mapping.ptr() as *mut usize) }
        }
        #[inline(always)]
        pub(crate) fn free_space(&self) -> usize {
            self.committed_size.get() - Self::HEADER_SIZE - self.used_space()
        }

        pub(crate) fn map_const_ptr(&self) -> *const u8 {
            self.ptr.wrapping_add(Self::HEADER_SIZE)
        }
        pub(crate) fn map_mut_ptr(&self) -> *mut u8 {
            self.ptr.wrapping_add(Self::HEADER_SIZE)
        }

        pub(crate) fn map(&self) -> &[u8] {
            let used = self.used_space();
            unsafe { slice::from_raw_parts(self.ptr.wrapping_add(Self::HEADER_SIZE), used) }
        }
        pub(crate) fn map_mut(&mut self) -> &mut [u8] {
            let used = self.used_space();
            unsafe { slice::from_raw_parts_mut(self.ptr.wrapping_add(Self::HEADER_SIZE), used) }
        }

        pub(crate) fn from_mapping(mapping: impl FileMappingTrait + 'static) -> Self {
            Self {
                ptr: mapping.ptr(),
                mapping: Box::new(mapping),
                committed_size: Cell::new(0),
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
            let mut create = target.create();

            if !std::fs::metadata(&path).is_ok() {
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
            let new_used_size = len.saturating_sub(Self::HEADER_SIZE).max(initial_size);
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

            let mut temp = DiskMmapHandleNew {
                committed_size: Cell::new(mapping.committed_size()),
                ptr: mapping.ptr(),
                mapping: Box::new(mapping),
                seek_pos: 0,
            };
            temp.set_used_space(new_used_size);
            Ok(temp)
        }
    }

    impl DiskMmapHandleNew {
        pub fn try_lock_exclusive(&self) -> Result<()> {
            self.mapping.try_lock_exclusive()
        }
        pub fn write_zeroes(&mut self, bytes: usize) -> Result<()> {
            if self.seek_pos + bytes > self.used_space() {
                self.grow(self.seek_pos + bytes);
            }

            unsafe {
                slice::from_raw_parts_mut(
                    self.ptr
                        .wrapping_add(self.seek_pos as usize)
                        .wrapping_add(Self::HEADER_SIZE),
                    bytes,
                )
                .fill(0)
            }
            self.seek_pos += bytes;
            Ok(())
        }

        pub fn copy_to(&mut self, bytes: usize, target: &mut DiskMmapHandleNew) -> Result<()> {
            if self.seek_pos + bytes > self.used_space() {
                bail!("requested number of bytes not available in file");
            }
            let src_buf = &self.map()[self.seek_pos..self.seek_pos + bytes];

            if target.seek_pos + bytes > target.used_space() {
                target.grow((target.seek_pos + bytes))?; //TODO: Use checked arithmetic
            }
            let target_seek_pos = target.seek_pos;
            let dst_buf = &mut target.map_mut()[target_seek_pos..target_seek_pos + bytes];

            dst_buf.copy_from_slice(src_buf);

            self.seek_pos += bytes;
            target.seek_pos += bytes;

            Ok(())
        }

        /// Read the given number of bytes, and make them available to the closure.
        /// This does advance the file pointer (like all other read methods)
        pub fn with_bytes<R>(&mut self, bytes: usize, mut f: impl FnMut(&[u8]) -> R) -> Result<R> {
            if self.seek_pos + bytes > self.used_space() {
                bail!("requested number of bytes not available in file");
            }
            let data = &self.map()[self.seek_pos..self.seek_pos + bytes];
            let ret = f(data);
            self.seek_pos += bytes;
            Ok(ret)
        }

        pub(crate) fn grow(&self, new_size: usize) -> Result<()> {
            if new_size + Self::HEADER_SIZE > self.committed_size.get() {
                let max_size = self.mapping.maximum_size();
                if new_size + Self::HEADER_SIZE >= max_size {
                    bail!("maximum file size exceeded");
                }

                let new_file_size = ((self.committed_size.get() + new_size + Self::HEADER_SIZE)
                    * 2)
                .next_multiple_of(self.mapping.page_size())
                .min(max_size);
                self.mapping.grow_committed_mapping(
                    new_file_size,
                    "?", /*TODO: Use real name here!*/
                )?;
                self.committed_size.set(new_file_size);
            }
            *self.used_space_mut() = new_size;
            Ok(())
        }

        // TODO: Remove, merge with 'map_mut_ptr'
        pub(crate) fn get_ptr(&self) -> *mut u8 {
            //TODO: Consider storing this value instead of mapping, with the HEADER_SIZE pre-added,
            // for perf
            self.mapping.ptr().wrapping_add(Self::HEADER_SIZE)
        }

        /// This is the actual size of the file on disk. This is not the 'logical' size
        /// Should probably not be exposed, since that would mean we had a leaky abstraction
        fn committed_len(&self) -> usize {
            self.committed_size.get().saturating_sub(Self::HEADER_SIZE)
        }

        pub(crate) fn flush_range(&self, offset: usize, len: usize) -> Result<()> {
            self.mapping.flush_range(offset + Self::HEADER_SIZE, len)?;
            self.mapping.flush_range(0, Self::HEADER_SIZE)?; //TODO: We could detect if this range is in the same page as the preivous, and do a single flush in that case
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
            if new_alloc_size < self.committed_size.get() {
                self.mapping.shrink_committed_mapping(new_alloc_size)?;
                self.committed_size.set(new_alloc_size);
            }
            *self.used_space_mut() = new_size;
            Ok(())
        }
    }
}

fn sha2(bytes: &[u8]) -> [u8; 16] {
    let mut hasher = Sha256::new();
    // write input message
    hasher.update(bytes);

    // read hash digest and consume hasher
    hasher.finalize_fixed()[0..16].try_into().unwrap()
}

static BOOT_CHECKSUM: OnceLock<[u8; 16]> = OnceLock::new();

/// Returns a unique identifier that identifies this current boot.
/// After a reboot of the operating system, due to power-loss or otherwise,
/// this method will return a different value.
///
/// We use this to determine if unflushed memory mapped data might have been lost
fn get_boot_checksum() -> [u8; 16] {
    *BOOT_CHECKSUM.get_or_init(|| {
        let boot_time = get_boot_time();
        sha2(boot_time.as_bytes())
    })
}

#[derive(
    Pod,
    Zeroable,
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Savefile,
    Serialize,
    Deserialize,
)]
#[repr(transparent)]
pub struct MessageId {
    data: [u32; 4],
}

impl Display for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let time_ms = ((self.data[0] as u64) << 16) + ((self.data[1]) >> 16) as u64;

        let unix_timestamp = (time_ms / 1000) as i64;
        let timestamp_ms = time_ms % 1000;

        let time = OffsetDateTime::from_unix_timestamp(unix_timestamp).unwrap();

        let time_str = time.format(&Rfc3339).unwrap(); //All values representable should be formattable here
        write!(
            f,
            "{:?}-{:x}-{:x}-{:x}",
            time_str,
            self.data[1] & 0xffff,
            self.data[2],
            self.data[3]
        )
    }
}

impl Debug for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "#{:x}_{:x}_{:x}",
            self.data[0], self.data[1], self.data[2]
        )
    }
}

impl MessageId {
    pub fn min(self, other: MessageId) -> MessageId {
        if self < other { self } else { other }
    }
    pub fn is_zero(&self) -> bool {
        self.data[0] == 0 && self.data[1] == 0
    }
    pub fn zero() -> MessageId {
        MessageId { data: [0, 0, 0, 0] }
    }
    pub fn new_debug(nr: u32) -> Self {
        Self {
            data: [nr, 0, 0, 0],
        }
    }
}

#[derive(Pod, Zeroable, Copy, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
// 0 is an invalid sequence number, used to represent 'not a number'
pub struct SequenceNr(u32);

impl Display for SequenceNr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.0 == 0 {
            write!(f, "#INVALID")
        } else {
            write!(f, "#{}", self.0 - 1)
        }
    }
}
impl Debug for SequenceNr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.0 == 0 {
            write!(f, "#INVALID")
        } else {
            write!(f, "#{}", self.0 - 1)
        }
    }
}

impl SequenceNr {
    pub const INVALID: SequenceNr = SequenceNr(0);
    pub fn is_invalid(self) -> bool {
        self.0 == 0
    }
    pub fn is_valid(self) -> bool {
        self.0 != 0
    }
    pub fn successor(self) -> SequenceNr {
        SequenceNr(self.0 + 1)
    }
    pub fn from_index(index: usize) -> SequenceNr {
        if index >= (u32::MAX - 1) as usize {
            panic!("More than 2^32 elements created. Not supported by noatun");
        }
        SequenceNr(index as u32 + 1)
    }
    pub fn index(self) -> usize {
        if self.0 == 0 {
            panic!("0 SequenceNr does not have an index")
        }
        self.0 as usize - 1
    }
    pub fn try_index(self) -> Option<usize> {
        if self.0 == 0 {
            return None;
        }
        Some(self.0 as usize - 1)
    }
}

pub trait Message: Debug {
    type Root: Object;
    fn id(&self) -> MessageId;
    fn parents(&self) -> impl ExactSizeIterator<Item = MessageId>;
    fn apply(&self, context: &mut DatabaseContext, root: &mut Self::Root);

    fn deserialize(buf: &[u8]) -> Result<Self>
    where
        Self: Sized;
    fn serialize<W: Write>(&self, writer: W) -> Result<()>;
}

/// A state-less object, mostly useful for testing
pub struct DummyUnitObject;

impl Object for DummyUnitObject {
    type Ptr = ThinPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        &DummyUnitObject
    }

    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        // # SAFETY
        // Any dangling pointer is a valid pointer to a zero-sized type
        unsafe { &mut *(std::ptr::dangling_mut() as *mut Self) }
    }
}

// TODO: This type should probably not be public
pub struct MessageStore<APP: Application> {
    messages: OnDiskMessageStore<APP::Message>,
    phantom_data: PhantomData<(*const APP::Root, APP::Message)>,
}

impl<APP: Application> MessageStore<APP> {
    pub fn new<D: Disk>(s: &mut D, target: &Target) -> Result<MessageStore<APP>> {
        Ok(MessageStore {
            messages: OnDiskMessageStore::new(s, target)?,
            phantom_data: PhantomData,
        })
    }
    /// Returns true if the message did not exist and was inserted
    fn push_message(
        &mut self,
        context: &mut DatabaseContext,
        message: APP::Message,
    ) -> Result<bool> {
        if let Some(insert_point) = self.messages.append_single(message)? {
            self.rewind(context, insert_point)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Returns true if any of the messages were not previously present
    fn push_messages(
        &mut self,
        context: &mut DatabaseContext,
        message: impl Iterator<Item = APP::Message>,
    ) -> Result<bool> {
        let mut messages: Vec<APP::Message> = message.collect();
        messages.sort_unstable_by_key(|x| x.id());

        if let Some(insert_point) = self.messages.append_many_sorted(messages.into_iter())? {
            if let Some(cur_main_db_next_index) = context.next_seqnr().try_index() {
                if insert_point < cur_main_db_next_index {
                    self.rewind(context, insert_point)?;
                }
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn rewind(&mut self, context: &mut DatabaseContext, point: usize) -> Result<()> {
        context.rewind(SequenceNr::from_index(point));
        Ok(())
    }

    fn apply_single_message(
        context: &mut DatabaseContext,
        root: &mut APP::Root,
        msg: &APP::Message,
        seqnr: SequenceNr,
    ) {
        msg.apply(context, root); //TODO: Handle panics in apply gracefully
        context.set_next_seqnr(seqnr.successor()); //TODO: Don't record a snapshot for _every_ message.
        context.finalize_message(seqnr);
    }

    fn apply_missing_messages(
        &mut self,
        root: &mut APP::Root,
        context: &mut DatabaseContext,
    ) -> Result<()> {
        for (seq, msg) in self
            .messages
            .query_by_index(context.next_seqnr().try_index().unwrap())?
        {
            let seqnr = SequenceNr::from_index(seq);
            Self::apply_single_message(context, root, &msg, seqnr);
        }
        //let next_index = self.messages.next_index()?;
        //context.set_next_seqnr(SequenceNr::from_index(next_index));
        let must_remove = context.finalize_transaction(&mut self.messages)?;
        for index in must_remove {
            println!("Removing message {:?}", index);
            self.messages.mark_deleted_by_index(index.index());
            //*self.messages.get_index_mut(index.index()).unwrap().1 = None;
        }
        Ok(())
    }
}

pub enum GenPtr {
    Thin(ThinPtr),
    Fat(FatPtr),
}

pub trait Pointer: Copy + Debug + 'static {
    fn start(self) -> usize;
    fn create<T>(addr: &T, buffer_start: *const u8) -> Self;
    fn as_generic(&self) -> GenPtr;
}

pub trait Object {
    /// This is meant to be either ThinPtr for sized objects, or
    /// FatPtr for dynamically sized objects. Other types are likely to not make sense.
    type Ptr: Pointer;
    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self;
    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self;
}

pub trait FixedSizeObject: Object<Ptr = ThinPtr> + Sized + Pod {
    const SIZE: usize = std::mem::size_of::<Self>();
    const ALIGN: usize = std::mem::align_of::<Self>();
}

impl<T: Object<Ptr = ThinPtr> + Sized + Copy + Pod> FixedSizeObject for T {
    const SIZE: usize = std::mem::size_of::<Self>();
    const ALIGN: usize = std::mem::align_of::<Self>();
}

impl Object for usize {
    type Ptr = ThinPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index) }
    }

    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}

impl Object for u32 {
    type Ptr = ThinPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index) }
    }

    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}
impl Object for u8 {
    type Ptr = ThinPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index) }
    }

    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}
pub trait Application {
    type Root: Object;
    type Message: Message<Root = Self::Root>;

    fn initialize_root(ctx: &mut DatabaseContext) -> <Self::Root as Object>::Ptr;
}

pub struct Database<Base: Application> {
    context: DatabaseContext,
    message_store: MessageStore<Base>,
    app: Base,
}

#[derive(Copy, Clone)]
#[repr(C)]
pub struct DatabaseCell<T: Copy> {
    value: T,
    registrar: SequenceNr,
}

impl<T: Copy> Deref for DatabaseCell<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

#[derive(Copy, Clone, Debug)]
pub struct FatPtr {
    start: usize,
    len: usize,
}
impl FatPtr {
    pub fn from(start: usize, len: usize) -> FatPtr {
        FatPtr { start, len }
    }
}
#[derive(Copy, Clone, Debug)]
pub struct ThinPtr(pub usize);

unsafe impl Zeroable for FatPtr {}

unsafe impl Pod for FatPtr {}
unsafe impl Zeroable for ThinPtr {}

unsafe impl Pod for ThinPtr {}

impl Pointer for ThinPtr {
    fn start(self) -> usize {
        self.0
    }

    fn create<T>(addr: &T, buffer_start: *const u8) -> Self {
        let index = (addr as *const T as *const u8 as usize) - (buffer_start as usize);
        ThinPtr(index)
    }

    fn as_generic(&self) -> GenPtr {
        GenPtr::Thin(*self)
    }
}

impl Pointer for FatPtr {
    fn start(self) -> usize {
        self.start
    }
    fn create<T>(addr: &T, buffer_start: *const u8) -> Self {
        let addr: *const T = addr as *const T;
        let dummy: (*const u8, usize);
        assert_eq!(
            std::mem::size_of::<*const T>(),
            2 * std::mem::size_of::<usize>()
        );
        dummy = unsafe { transmute_copy(&addr) };
        FatPtr {
            start: ((dummy.0 as usize) - (buffer_start as usize)),
            len: dummy.1,
        }
    }
    fn as_generic(&self) -> GenPtr {
        GenPtr::Fat(*self)
    }
}

impl Object for [u8] {
    type Ptr = FatPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access(index) }
    }

    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_mut(index) }
    }
}

unsafe impl<T> Zeroable for DatabaseCell<T> where T: Pod {}

unsafe impl<T> Pod for DatabaseCell<T> where T: Pod {}
impl<T: Pod> DatabaseCell<T> {
    pub fn get(&self, context: &DatabaseContext) -> T {
        context.observe_registrar(self.registrar);
        self.value
    }
    pub fn set<'a>(&'a mut self, context: &'a mut DatabaseContext, new_value: T) {
        let index = context.index_of(self);
        //context.write(index, bytes_of(&new_value));
        context.write_pod(new_value, &mut self.value);
        context.update_registrar(&mut self.registrar);
    }
}

#[repr(C)]
pub struct DatabaseVec<T> {
    length: usize,
    capacity: usize,
    data: usize,
    length_registrar: SequenceNr,
    phantom_data: PhantomData<T>,
}

unsafe impl<T> Zeroable for DatabaseVec<T> {}

impl<T> Copy for DatabaseVec<T> {}

impl<T> Clone for DatabaseVec<T> {
    fn clone(&self) -> Self {
        *self
    }
}

unsafe impl<T> Pod for DatabaseVec<T> where T: 'static {}

impl<T> DatabaseVec<T>
where
    T: FixedSizeObject + 'static,
{
    pub fn new(ctx: &DatabaseContext) -> &mut DatabaseVec<T> {
        unsafe { ctx.allocate_pod::<DatabaseVec<T>>() }
    }
    pub fn len(&self, ctx: &mut DatabaseContext) -> usize {
        ctx.observe_registrar(self.length_registrar);
        self.length
    }
    pub fn get(&self, ctx: &DatabaseContext, index: usize) -> &T {
        let offset = self.data + index * T::SIZE;
        unsafe { T::access(ctx, ThinPtr(offset)) }
    }
    pub fn get_mut(&mut self, ctx: &mut DatabaseContext, index: usize) -> &mut T {
        let offset = self.data + index * T::SIZE;
        let t = unsafe { T::access_mut(ctx, ThinPtr(offset)) };
        t
    }

    fn realloc_add(&mut self, ctx: &mut DatabaseContext, new_capacity: usize) {
        let dest = ctx.allocate_raw(new_capacity * T::SIZE, T::ALIGN);
        let dest_index = ctx.index_of_ptr(dest);

        if self.length > 0 {
            let old_ptr = FatPtr::from(self.data, T::SIZE * self.length);
            println!("Copy old");
            ctx.copy(old_ptr, dest_index.0);
        }

        let new_len = self.length + 1;

        println!("Write-pod with db.vec");

        ctx.write_pod(
            DatabaseVec {
                length: new_len,
                capacity: new_capacity,
                data: dest_index.0,
                length_registrar: SequenceNr::default(),
                phantom_data: Default::default(),
            },
            self,
        )
    }

    pub fn push_new<'a>(&'a mut self, ctx: &mut DatabaseContext) -> &'a mut T {
        if self.length >= self.capacity {
            println!("Realloc-leg");
            self.realloc_add(ctx, (self.capacity + 1) * 2);
        } else {
            println!("no-Realloc-leg");
            ctx.write_pod(self.length + 1, &mut self.length);
        }
        ctx.update_registrar(&mut self.length_registrar);

        self.get_mut(ctx, self.length - 1)
    }
}

impl<T> Object for DatabaseVec<T>
where
    T: FixedSizeObject + 'static,
{
    type Ptr = ThinPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { &*(context.start_ptr().wrapping_add(index.0) as *const DatabaseVec<T>) }
    }
    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}

#[repr(transparent)]
struct DatabaseObjectHandle<T: Object> {
    object_index: T::Ptr,
    phantom: PhantomData<T>,
}

unsafe impl<T: Object> Zeroable for DatabaseObjectHandle<T> {}

impl<T: Object> Copy for DatabaseObjectHandle<T> {}

impl<T: Object> Clone for DatabaseObjectHandle<T> {
    fn clone(&self) -> Self {
        *self
    }
}

unsafe impl<T: Object + 'static> Pod for DatabaseObjectHandle<T> {}
impl<T: Object> Object for DatabaseObjectHandle<T> {
    type Ptr = ThinPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe {
            &*(context.start_ptr().wrapping_add(index.0) as *const _
                as *const DatabaseObjectHandle<T>)
        }
    }
    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe {
            &mut *(context.start_ptr().wrapping_add(index.0) as *const _
                as *mut DatabaseObjectHandle<T>)
        }
    }
}

impl<T: Object> DatabaseObjectHandle<T> {
    pub fn get<'a>(&'a self, context: &DatabaseContext) -> &'a T {
        unsafe { T::access(context, self.object_index) }
    }
    pub fn get_mut<'a>(&'a self, context: &mut DatabaseContext) -> &'a mut T {
        unsafe { T::access_mut(context, self.object_index) }
    }
    pub fn new(context: &DatabaseContext, value: T) -> &mut Self
    where
        T: Object<Ptr = ThinPtr>,
        T: Pod,
    {
        let this = unsafe { context.allocate_pod::<DatabaseObjectHandle<T>>() };
        let target = unsafe { context.allocate_pod::<T>() };
        *target = value;
        this.object_index = context.index_of(target);
        this
    }
}

impl<T: Pod + Object> DatabaseCell<T> {
    pub fn new(context: &DatabaseContext) -> &mut Self {
        let memory = unsafe { context.allocate_pod::<T>() };
        unsafe { &mut *(memory as *mut _ as *mut DatabaseCell<T>) }
    }
}

impl<T: Pod> Object for DatabaseCell<T> {
    type Ptr = ThinPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index) }
    }

    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}
thread_local! {
    static MULTI_INSTANCE_BLOCKER: Cell<bool> = Cell::new(false);
}

#[derive(Clone)]
pub enum Target {
    OpenExisting(PathBuf),
    CreateNewOrOverwrite(PathBuf),
    CreateNew(PathBuf),
}
impl Target {
    pub fn path(&self) -> &Path {
        let (Target::CreateNew(x) | Target::CreateNewOrOverwrite(x) | Target::OpenExisting(x)) =
            self;
        &*x
    }
    pub fn create(&self) -> bool {
        matches!(self, Target::CreateNewOrOverwrite(_) | Target::CreateNew(_))
    }
    pub fn overwrite(&self) -> bool {
        matches!(self, Target::CreateNewOrOverwrite(_))
    }
}
impl<APP: Application> Database<APP> {
    pub fn get_root(&self) -> (&APP::Root, &DatabaseContext) {
        let root_ptr = self.context.get_root_ptr::<<APP::Root as Object>::Ptr>();
        let root = unsafe { <APP::Root as Object>::access(&self.context, root_ptr) };
        (root, &self.context)
    }
    // TODO: This method should probably not be public (changes should only happen through messages)
    pub fn with_root_mut<R>(
        &mut self,
        f: impl FnOnce(&mut APP::Root, &mut DatabaseContext, &mut MessageStore<APP>) -> R,
    ) -> Result<R> {
        if !self.context.mark_dirty()? {
            // Recovery needed
            Self::recover(&mut self.app, &mut self.context, &mut self.message_store)?;
        }

        let root_ptr = self.context.get_root_ptr::<<APP::Root as Object>::Ptr>();
        let root = unsafe { <APP::Root as Object>::access_mut(&mut self.context, root_ptr) };

        let t = f(root, &mut self.context, &mut self.message_store);

        self.context.mark_clean();
        Ok(t)
    }

    fn recover(
        app: &mut APP,
        context: &mut DatabaseContext,
        message_store: &mut MessageStore<APP>,
    ) -> Result<()> {
        context.clear()?;

        let root_ptr = APP::initialize_root(context);
        context.set_root_ptr(root_ptr.as_generic());

        context.set_next_seqnr(SequenceNr::from_index(0));

        let root = <APP::Root as Object>::access_mut(context, root_ptr);
        message_store.apply_missing_messages(root, context)?;

        Ok(())
    }

    pub fn create_new(
        path: impl AsRef<Path>,
        app: APP,
        overwrite_existing: bool,
    ) -> Result<Database<APP>> {
        Self::create(
            app,
            if overwrite_existing {
                Target::CreateNewOrOverwrite(path.as_ref().to_path_buf())
            } else {
                Target::CreateNew(path.as_ref().to_path_buf())
            },
        )
    }
    pub fn open(path: impl AsRef<Path>, app: APP) -> Result<Database<APP>> {
        Self::create(app, Target::OpenExisting(path.as_ref().to_path_buf()))
    }

    pub fn append_single(&mut self, message: APP::Message) -> Result<()> {
        self.append_many(std::iter::once(message))
    }

    pub fn append_many(&mut self, messages: impl Iterator<Item = APP::Message>) -> Result<()> {
        self.with_root_mut(|root, context, message_store| {
            message_store.push_messages(context, messages)?;
            message_store.apply_missing_messages(root, context)?;

            Ok(())
        })?
    }

    /// Create a database residing entirely in memory.
    /// This is mostly useful for tests
    pub fn create_in_memory(mut app: APP) -> Result<Database<APP>> {
        let mut disk = InMemoryDisk::default();
        let target = Target::CreateNew(PathBuf::default());
        let mut ctx =
            DatabaseContext::new(&mut disk, &target).context("creating database in memory")?;
        let mut message_store = MessageStore::new(&mut disk, &target)?;
        Ok(Database {
            context: ctx,
            app,
            message_store,
        })
    }

    fn create(mut app: APP, target: Target) -> Result<Database<APP>> {
        if MULTI_INSTANCE_BLOCKER.get() {
            if !std::env::var("NOATUN_UNSAFE_ALLOW_MULTI_INSTANCE").is_ok() {
                panic!(
                    "Noatun: Multiple active DB-roots in the same thread are not allowed.\
                        You can disable this diagnostic by setting env-var NOATUN_UNSAFE_ALLOW_MULTI_INSTANCE.\
                        Note, unsoundness can occur if DatabaseContext from one instance is used by data \
                        for other."
                );
            }
        }
        MULTI_INSTANCE_BLOCKER.set(true);

        let mut ctx =
            DatabaseContext::new(&mut StandardDisk, &target).context("opening database")?;

        let is_dirty = ctx.is_dirty();

        let mut message_store = MessageStore::new(&mut StandardDisk, &target)?;
        if is_dirty {
            Self::recover(&mut app, &mut ctx, &mut message_store)?;
            ctx.mark_clean()?;
        }
        Ok(Database {
            context: ctx,
            app,
            message_store,
        })
    }
}
impl<APP: Application> Drop for Database<APP> {
    fn drop(&mut self) {
        MULTI_INSTANCE_BLOCKER.set(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::MainDbHeader;
    use crate::growable_file_mapping::DiskMmapHandleNew;
    use byteorder::{LittleEndian, WriteBytesExt};
    use savefile::{load_noschema, save_noschema};
    use savefile_derive::Savefile;
    use sha2::{Digest, Sha256};
    use std::io::{Cursor, SeekFrom};
    use test::Bencher;

    #[test]
    fn test_mmap_helper() {
        let mut mmap = DiskMmapHandleNew::new(
            &Target::CreateNewOrOverwrite("test/mmap_test1".into()),
            "mmap",
            0,
            16 * 1024 * 1024,
        )
        .unwrap();
        mmap.write_u32::<LittleEndian>(0x2b).unwrap();
        use byteorder::ReadBytesExt;
        use std::io::Read;
        use std::io::Seek;
        mmap.seek(SeekFrom::Start(12)).unwrap();
        mmap.write_u64::<LittleEndian>(0x2c).unwrap();
        mmap.seek(SeekFrom::Start(12)).unwrap();
        let initial_ptr = mmap.get_ptr();
        assert_eq!(mmap.read_u64::<LittleEndian>().unwrap(), 0x2c);

        mmap.seek(SeekFrom::Start(3000_000)).unwrap();
        mmap.write_u8(1).unwrap();
        assert_eq!(initial_ptr, mmap.get_ptr());

        mmap.seek(SeekFrom::Start(3000_000)).unwrap();
        assert_eq!(mmap.read_u8().unwrap(), 1);

        mmap.flush_all().unwrap();

        println!("Truncate");
        mmap.truncate(0).unwrap();
        mmap.seek(SeekFrom::Start(0)).unwrap();

        let mut buf = [0];
        let got = mmap.read(&mut buf).unwrap();
        assert_eq!(got, 0);
        mmap.write_u8(42).unwrap();
        mmap.write_u8(42).unwrap();
        mmap.seek(SeekFrom::Start(0)).unwrap();
        assert_eq!(mmap.read_u8().unwrap(), 42);
    }

    struct DummyMessage<T> {
        phantom_data: PhantomData<T>,
    }
    impl<T> Debug for DummyMessage<T> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "DummyMessage")
        }
    }

    impl<T: Object> Message for DummyMessage<T> {
        type Root = T;

        fn id(&self) -> MessageId {
            todo!()
        }

        fn parents(&self) -> impl ExactSizeIterator<Item = MessageId> {
            std::iter::empty()
        }

        fn apply(&self, context: &mut DatabaseContext, root: &mut Self::Root) {
            todo!()
        }

        fn deserialize(buf: &[u8]) -> Result<Self>
        where
            Self: Sized,
        {
            todo!()
        }

        fn serialize<W: Write>(&self, writer: W) -> Result<()> {
            todo!()
        }
    }

    #[derive(Clone, Copy, Zeroable, Pod)]
    #[repr(C)]
    struct CounterObject {
        counter: DatabaseCell<u32>,
        counter2: DatabaseCell<u32>,
    }

    impl Object for CounterObject {
        type Ptr = ThinPtr;

        fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
            unsafe { context.access_pod(index) }
        }

        fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
            unsafe { context.access_pod_mut(index) }
        }
    }

    impl CounterObject {
        fn set_counter(&mut self, ctx: &mut DatabaseContext, value1: u32, value2: u32) {
            self.counter.set(ctx, value1);
            self.counter2.set(ctx, value2);
        }
        fn new(ctx: &DatabaseContext) -> &mut CounterObject {
            ctx.allocate_pod()
        }
    }

    struct CounterApplication;

    impl Application for CounterApplication {
        type Root = CounterObject;
        type Message = CounterMessage;

        fn initialize_root(mut ctx: &mut DatabaseContext) -> ThinPtr {
            let new_obj = CounterObject::new(&ctx);
            //assert_eq!(ctx.index_of(&new_obj.counter), 0);
            ctx.index_of(new_obj)
            //new_obj
        }
    }

    #[derive(Debug)]
    struct IncrementMessage {
        increment_by: u32,
    }

    impl Message for IncrementMessage {
        type Root = CounterObject;

        fn id(&self) -> MessageId {
            MessageId::new_debug(self.increment_by)
        }

        fn parents(&self) -> impl ExactSizeIterator<Item = MessageId> {
            std::iter::empty()
        }

        fn apply(&self, context: &mut DatabaseContext, root: &mut Self::Root) {
            todo!()
        }

        fn deserialize(buf: &[u8]) -> Result<Self>
        where
            Self: Sized,
        {
            todo!()
        }

        fn serialize<W: Write>(&self, writer: W) -> Result<()> {
            todo!()
        }
    }

    #[test]
    fn test1() {
        let mut db: Database<CounterApplication> =
            Database::create_new("test/test1.bin", CounterApplication, true).unwrap();

        let context = &db.context;

        db.with_root_mut(|counter, context, _message_store| {
            assert_eq!(counter.counter.get(context), 0);
            counter.counter.set(context, 42);
            counter.counter2.set(context, 43);
            counter.counter.set(context, 44);

            assert_eq!(*counter.counter, 44);
            assert_eq!(counter.counter.get(context), 44);
            assert_eq!(counter.counter2.get(context), 43);
        });
    }

    #[derive(Debug, Savefile)]
    struct CounterMessage {
        id: MessageId,
        parent: Option<MessageId>,
        inc1: i32,
        set1: u32,
    }
    impl Message for CounterMessage {
        type Root = CounterObject;
        fn id(&self) -> MessageId {
            self.id
        }

        fn parents(&self) -> impl ExactSizeIterator<Item = MessageId> {
            self.parent.into_iter()
        }

        fn apply(&self, context: &mut DatabaseContext, root: &mut CounterObject) {
            println!("Applying {:?}", self);
            if self.inc1 != 0 {
                root.counter.set(
                    context,
                    root.counter.get(context).saturating_add_signed(self.inc1),
                );
            } else {
                root.counter.set(context, self.set1);
            }
        }

        fn deserialize(buf: &[u8]) -> Result<Self>
        where
            Self: Sized,
        {
            Ok(load_noschema(&mut Cursor::new(buf), 1)?)
        }

        fn serialize<W: Write>(&self, mut writer: W) -> Result<()> {
            Ok(save_noschema(&mut writer, 1, self)?)
        }
    }

    #[test]
    fn test_msg_store_real() {
        let mut db: Database<CounterApplication> =
            Database::create_new("test/msg_store.bin", CounterApplication, true).unwrap();
        //        let mut messages = MessageStore::new(StandardDisk, &Target::CreateNewOrOverwrite("test/msg_store.bin".into())).unwrap();

        db.append_single(CounterMessage {
            parent: None,
            id: MessageId::new_debug(0x100),
            inc1: 2,
            set1: 0,
        })
        .unwrap();
        db.append_single(CounterMessage {
            parent: Some(MessageId::new_debug(0x100)),
            id: MessageId::new_debug(0x101),
            inc1: 0,
            set1: 42,
        })
        .unwrap();
        db.append_single(CounterMessage {
            parent: Some(MessageId::new_debug(0x101)),
            id: MessageId::new_debug(0x102),
            inc1: 1,
            set1: 0,
        })
        .unwrap();

        // Fix, this is what was done here before: messages.apply_missing_messages(&mut db);

        db.with_root_mut(|root, context, _| {
            assert_eq!(root.counter.get(context), 43);
        });
    }

    #[test]
    fn test_msg_store_inmem_miri() {
        let mut db: Database<CounterApplication> =
            Database::create_in_memory(CounterApplication).unwrap();
        //        let mut messages = MessageStore::new(StandardDisk, &Target::CreateNewOrOverwrite("test/msg_store.bin".into())).unwrap();

        db.append_single(CounterMessage {
            parent: None,
            id: MessageId::new_debug(0x100),
            inc1: 2,
            set1: 0,
        })
        .unwrap();
        db.append_single(CounterMessage {
            parent: Some(MessageId::new_debug(0x100)),
            id: MessageId::new_debug(0x101),
            inc1: 0,
            set1: 42,
        })
        .unwrap();
        db.append_single(CounterMessage {
            parent: Some(MessageId::new_debug(0x101)),
            id: MessageId::new_debug(0x102),
            inc1: 1,
            set1: 0,
        })
        .unwrap();

        // Fix, this is what was done here before: messages.apply_missing_messages(&mut db);

        db.with_root_mut(|root, context, _| {
            assert_eq!(root.counter.get(context), 43);
        });
    }

    #[test]
    fn test_handle() {
        struct HandleApplication;

        impl Application for HandleApplication {
            type Root = DatabaseObjectHandle<u32>;
            type Message = DummyMessage<DatabaseObjectHandle<u32>>;

            fn initialize_root(mut ctx: &mut DatabaseContext) -> ThinPtr {
                let obj = DatabaseObjectHandle::new(&ctx, 43u32);

                ctx.index_of(obj)
            }
        }

        let mut db: Database<HandleApplication> =
            Database::create_new("test/test_handle.bin", HandleApplication, true).unwrap();

        let app = HandleApplication;
        let (handle, context) = db.get_root();
        assert_eq!(*handle.get(context), 43);
    }

    #[test]
    fn test_vec0() {
        struct CounterVecApplication;

        impl Application for CounterVecApplication {
            type Root = DatabaseVec<CounterObject>;

            fn initialize_root(ctx: &mut DatabaseContext) -> ThinPtr {
                let obj: &mut DatabaseVec<CounterObject> = DatabaseVec::new(ctx);
                ctx.index_of(obj)
            }

            type Message = DummyMessage<DatabaseVec<CounterObject>>;
        }

        let mut db: Database<CounterVecApplication> =
            Database::create_new("test/test_vec0", CounterVecApplication, true).unwrap();
        db.with_root_mut(|counter_vec, context, _| {
            assert_eq!(counter_vec.len(context), 0);

            println!("1");
            let new_element = counter_vec.push_new(context);
            let new_element = counter_vec.get_mut(context, 0);
            println!("2");

            new_element.counter.set(context, 47);
            let new_element = counter_vec.push_new(context);
            new_element.counter.set(context, 48);
            println!("3");

            assert_eq!(counter_vec.len(context), 2);

            let item = counter_vec.get_mut(context, 1);
            assert_eq!(*item.counter, 48);

            println!("4");

            for _ in 0..10 {
                let new_element = counter_vec.push_new(context);
                println!("5");
            }
            println!("6");

            let item = counter_vec.get_mut(context, 1);
            assert_eq!(*item.counter, 48);
        });
    }

    #[test]
    fn test_vec_undo() {
        struct CounterVecApplication;

        impl Application for CounterVecApplication {
            type Root = DatabaseVec<CounterObject>;

            fn initialize_root(ctx: &mut DatabaseContext) -> ThinPtr {
                let obj: &mut DatabaseVec<CounterObject> = DatabaseVec::new(ctx);
                let index = ctx.index_of(obj);

                // This assert might not hold in the future, if we redesign things.
                // It's a bit white-box, consider removing.
                assert_eq!(index.start(), size_of::<MainDbHeader>());
                index
            }

            type Message = DummyMessage<DatabaseVec<CounterObject>>;
        }

        let mut db: Database<CounterVecApplication> =
            Database::create_new("test/vec_undo", CounterVecApplication, true).unwrap();

        {
            db.with_root_mut(|counter_vec, context, _| {
                context.set_next_seqnr(SequenceNr::from_index(1));
                assert_eq!(counter_vec.len(context), 0);
                println!("Pre-push");
                let new_element = counter_vec.push_new(context);
                new_element.counter.set(context, 47);
                new_element.counter2.set(context, 48);
                println!("Pre-set-time");
                context.set_next_seqnr(SequenceNr::from_index(2));
                assert_eq!(counter_vec.len(context), 1);
                context.set_next_seqnr(SequenceNr::from_index(3));
            });
        }

        {
            db.with_root_mut(|counter_vec, context, _| {
                let counter = counter_vec.get_mut(context, 0);
                counter.counter.set(context, 50);
                context.rewind(SequenceNr::from_index(2));
                assert_eq!(counter.counter.get(context), 47);
            });
        }

        println!("Rewind!");
        db.context.rewind(SequenceNr::from_index(1));

        {
            db.with_root_mut(|counter_vec, context, _| {
                assert_eq!(counter_vec.len(context), 0);
            });
        }
    }

    #[bench]
    fn bench_sha256(b: &mut Bencher) {
        // write input message

        // read hash digest and consume hasher

        b.iter(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"hello world");
            hasher.finalize()
        });
    }
}
