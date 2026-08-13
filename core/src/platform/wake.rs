use std::{io, os::fd};

/// Owning wake descriptor pair supplied by the selected backend.
pub struct Pair {
    read: fd::OwnedFd,
    write: fd::OwnedFd,
}

impl Pair {
    /// Opens blocking wake descriptor ends.
    pub fn blocking() -> io::Result<Self> {
        use crate::backend::{Backend, WakeFactory};

        let (read, write) = <Backend as WakeFactory>::open_blocking_wake_ends()?;
        Ok(Self { read, write })
    }

    /// Opens nonblocking wake descriptor ends.
    pub fn nonblocking() -> io::Result<Self> {
        use crate::backend::{Backend, WakeFactory};

        let (read, write) = <Backend as WakeFactory>::open_nonblocking_wake_ends()?;
        Ok(Self { read, write })
    }

    pub fn split(self) -> (fd::OwnedFd, fd::OwnedFd) {
        (self.read, self.write)
    }
}
