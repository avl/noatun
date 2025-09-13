#[cfg(unix)]
pub(crate) use unix::get_boot_time;
#[cfg(unix)]
pub(crate) use unix::FileMapping;

#[cfg(windows)]
compile_error!("noatun does not currently support windows");

#[cfg(target_os="macos")]
compile_error!("noatun does not currently support macos");

#[cfg(unix)]
mod unix {
    use crate::disk_access::FileBackend;
    use anyhow::{bail, Context, Result};
    use fs2::FileExt;

    use std::fs::File;
    use std::os::fd::AsRawFd;
    #[cfg(not(miri))]
    use std::process::Command;
    use std::ptr::null_mut;
    use std::sync::OnceLock;
    use tracing::error;

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
        file_name: String,
        committed_size: usize,
        total_size: usize,
    }
    // Safety: FileMapping contains nothing that can't be Sync
    unsafe impl Sync for FileMapping {}
    // Safety: FileMapping contains nothing that can't be Send
    unsafe impl Send for FileMapping {}

    impl Drop for FileMapping {
        fn drop(&mut self) {
            // Safety: munmap is used with the correct parameters
            unsafe {
                if libc::munmap(self.base_ptr as *mut _, self.total_size) != 0 {
                    error!("munmap failed");
                    eprintln!("munmap failed");
                }
            }
        }
    }

    static PAGE_SIZE: OnceLock<usize> = OnceLock::new();

    impl FileMapping {
        pub fn page_size() -> usize {
            // Safety: sysconf is used with the correct parameters
            (*PAGE_SIZE.get_or_init(|| unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) as usize }))
                .max(4096)
        }
        pub(crate) fn committed_size(&self) -> usize {
            self.committed_size
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

            // Safety: mmap is used correctly
            let ptr = unsafe {
                libc::mmap(
                    null_mut(),
                    max_size,
                    libc::PROT_NONE,
                    libc::MAP_ANONYMOUS | libc::MAP_PRIVATE | libc::MAP_NORESERVE,
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
                file,
                base_ptr: ptr as *mut u8,
                file_name: filename_for_diagnostics.to_string(),
                committed_size: 0,
                total_size: actual_size,
            };
            temp.grow_committed_mapping(min_initial_size)?;
            Ok(temp)
        }
    }

    impl FileBackend for FileMapping {
        fn page_size(&self) -> usize {
            Self::page_size()
        }
        fn sync_all(&self) -> Result<()> {
            self.sync_range(0, self.committed_size)
        }
        fn sync_range(&self, start: usize, len: usize) -> Result<()> {
            // Safety: libc::msync is used with correct parameters
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
                    bail!(
                        "msync {}..{} of file {} failed: {:x?}",
                        start,
                        start + len,
                        self.file_name,
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
            self.committed_size
        }

        fn maximum_size(&self) -> usize {
            self.total_size
        }

        fn shrink_committed_mapping(&mut self, new_size: usize) -> Result<()> {
            assert_eq!(new_size % self.page_size(), 0);
            if new_size >= self.committed_size {
                return Ok(());
            }

            let prev_size = self.committed_size;
            let shrink_by = prev_size - new_size;
            // Safety: mmap is used with correct parameters
            let ptr = unsafe {
                libc::mmap(
                    self.base_ptr.wrapping_add(new_size) as *mut _,
                    shrink_by,
                    libc::PROT_NONE,
                    libc::MAP_FIXED | libc::MAP_ANONYMOUS | libc::MAP_PRIVATE | libc::MAP_NORESERVE,
                    -1,
                    0,
                )
            };
            if ptr as usize == 0 || ptr as usize == usize::MAX {
                bail!("while remapping(shrink), mmap failed. prev_size = {}, shrink_by = {}, new_size = {}, ptr = {:?}, base_ptr = {:?}, err = {:?}", prev_size, shrink_by, new_size, ptr, self.base_ptr,
                std::io::Error::from_raw_os_error(
                        std::io::Error::last_os_error().raw_os_error().unwrap()
                    )
                );
            }

            self.file.set_len(new_size as u64)?;
            self.file.sync_all().context("fsync")?;

            Ok(())
        }

        fn grow_committed_mapping(&mut self, new_size: usize) -> Result<()> {
            assert_eq!(new_size % self.page_size(), 0);
            if new_size <= self.committed_size {
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
            // We should sync, because otherwise the metadata change (to file length)
            // might not be persisted even though file contents are, which  seems like something
            // we don't want to deal with.
            self.file.sync_all().context("fsync")?;

            // Safety: mmap is used correctly
            let ptr = unsafe {
                libc::mmap(
                    self.base_ptr as *mut _,
                    new_size,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_FIXED | libc::MAP_SHARED,
                    self.file.as_raw_fd(),
                    0,
                )
            };
            if ptr as usize == 0 || ptr as usize == usize::MAX {
                bail!(
                    "error remapping (grow), mmap of file {} new_size = {}, ptr = {:?}, base_ptr = {:?}, prev_size = {}, failed: {:?}",
                    self.file_name,new_size, ptr, self.base_ptr,
                    self.committed_size,
                    std::io::Error::from_raw_os_error(
                        std::io::Error::last_os_error().raw_os_error().unwrap()
                    )
                );
            }

            self.committed_size = new_size;

            Ok(())
        }

        fn try_lock_exclusive(&self) -> Result<()> {
            Ok(self.file.try_lock_exclusive()?)
        }
    }
}
