use std::io::Error;

#[derive(Clone, Copy)]
pub(super) struct Errno(i32);

impl Errno {
    pub(super) fn last() -> Self {
        Self(Error::last_os_error().raw_os_error().unwrap_or(0))
    }

    pub(super) const fn raw(self) -> i32 {
        self.0
    }

    pub(super) const fn is_block(self) -> bool {
        self.0 == libc::EAGAIN || self.0 == libc::EWOULDBLOCK
    }
}
