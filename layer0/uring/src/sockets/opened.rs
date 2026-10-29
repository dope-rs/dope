use std::os::fd;

use dope_sys::{Errno, io::file};

/// One completed open and the descriptor it owns on success.
#[must_use = "an open completion must be delivered or reclaimed"]
pub struct Opened(pub(crate) Result<fd::OwnedFd, Errno>);

impl file::Opened for Opened {
    fn deliver(self) -> Result<fd::OwnedFd, Errno> {
        self.0
    }
}
