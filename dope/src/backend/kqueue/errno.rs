use std::io;

pub(super) struct Errno;

impl Errno {
    pub(super) fn last_raw() -> i32 {
        io::Error::last_os_error().raw_os_error().unwrap_or(0)
    }

    pub(super) const fn is_block_raw(raw: i32) -> bool {
        raw == libc::EAGAIN || raw == libc::EWOULDBLOCK
    }
}
