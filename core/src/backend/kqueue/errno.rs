use std::io;

#[derive(Clone, Copy)]
pub(in crate::backend::kqueue) struct Errno(pub(in crate::backend::kqueue) i32);

impl Errno {
    pub(in crate::backend::kqueue) fn last() -> Self {
        let Some(raw) = io::Error::last_os_error().raw_os_error() else {
            use std::process::abort;
            abort();
        };
        Self(raw)
    }

    pub(in crate::backend::kqueue) const fn raw(self) -> i32 {
        self.0
    }

    pub(in crate::backend::kqueue) const fn is_block(self) -> bool {
        use libc::{EAGAIN, EWOULDBLOCK};
        self.0 == EAGAIN || self.0 == EWOULDBLOCK
    }
}
