use std::{io, os::fd};

pub(in crate::backend::uring) struct Pipe {
    read: fd::OwnedFd,
    write: fd::OwnedFd,
}

impl Pipe {
    pub(in crate::backend::uring) fn open() -> io::Result<Self> {
        Self::open_with(libc::O_CLOEXEC)
    }

    pub(in crate::backend::uring) fn open_nonblocking() -> io::Result<Self> {
        Self::open_with(libc::O_CLOEXEC | libc::O_NONBLOCK)
    }

    fn open_with(flags: libc::c_int) -> io::Result<Self> {
        let mut fds = [0 as fd::RawFd; 2];
        // SAFETY: fds names writable storage for exactly two descriptors.
        let result = unsafe { libc::pipe2(fds.as_mut_ptr(), flags) };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        let [read, write] = fds;
        // SAFETY: pipe2 returned a fresh owned descriptor.
        let read = unsafe { fd::FromRawFd::from_raw_fd(read) };
        // SAFETY: pipe2 returned a fresh owned descriptor.
        let write = unsafe { fd::FromRawFd::from_raw_fd(write) };
        Ok(Self { read, write })
    }

    pub(in crate::backend::uring) fn into_ends(self) -> (fd::OwnedFd, fd::OwnedFd) {
        (self.read, self.write)
    }
}
