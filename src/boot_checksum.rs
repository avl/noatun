use crate::platform_specific::get_boot_time;
use crate::sha2_helper::sha2;
use std::sync::OnceLock;

static BOOT_CHECKSUM: OnceLock<[u8; 16]> = OnceLock::new();

/// Returns a unique identifier that identifies this current boot.
/// After a reboot of the operating system, due to power-loss or otherwise,
/// this method will return a different value.
///
/// We use this to determine if unflushed memory mapped data might have been lost
pub(crate) fn get_boot_checksum() -> [u8; 16] {
    *BOOT_CHECKSUM.get_or_init(|| {
        let boot_time = get_boot_time();
        sha2(boot_time.as_bytes())
    })
}
