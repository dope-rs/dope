use crate::Errno;

pub(in crate::thread) fn try_crc32(_: impl Fn() -> i32) -> Result<bool, Errno> {
    // SAFETY: the mandatory HWCAP entry is immutable loader data;
    // reading it neither initializes a cache nor consults errno.
    Ok(unsafe { libc::getauxval(libc::AT_HWCAP) } & libc::HWCAP_CRC32 != 0)
}
