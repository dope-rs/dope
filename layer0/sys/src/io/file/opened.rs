use std::os::fd;

use crate::Errno;

/// A decoded file completion retaining any descriptor it owns.
pub trait Opened {
    fn deliver(self) -> Result<fd::OwnedFd, Errno>;
}
