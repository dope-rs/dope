use std::{ffi::CStr, os::fd};

use o3::base::marker::ThreadBound;

pub use dope_sys::io::file::watch::Event;
pub mod raw;

#[repr(transparent)]
pub struct Watch {
    file: fd::OwnedFd,
    thread: ThreadBound,
}

impl Watch {
    /// Observes moved-in entries and invalidation of one fixed directory.
    pub fn try_new(path: &CStr) -> Result<Self, dope_sys::Errno> {
        Ok(Self {
            file: raw::create(path)?,
            thread: ThreadBound::NEW,
        })
    }
}

impl fd::AsFd for Watch {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.file.as_fd()
    }
}
