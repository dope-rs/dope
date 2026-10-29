use crate::Errno;

pub(in crate::thread) fn try_crc32(_: impl Fn() -> i32) -> Result<bool, Errno> {
    Ok(false)
}
