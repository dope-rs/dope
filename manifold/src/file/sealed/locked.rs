use std::{fs, io, os::fd};

pub(in crate::file) struct Locked(fs::File);

impl Locked {
    pub(in crate::file) fn acquire(file: fs::File) -> io::Result<(Self, u64)> {
        // SAFETY: `file` owns a live descriptor for this call and remains
        // inside `Locked` until close releases the advisory lock.
        let result =
            unsafe { libc::flock(fd::AsRawFd::as_raw_fd(&file), libc::LOCK_EX | libc::LOCK_NB) };
        if result == 0 {
            let metadata = file.metadata()?;
            if metadata.is_file() {
                Ok((Self(file), metadata.len()))
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "dope::file: append target is not a regular file",
                ))
            }
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub(in crate::file) fn file_mut(&mut self) -> &mut fs::File {
        &mut self.0
    }
}

impl fd::AsFd for Locked {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        // SAFETY: the borrowed descriptor cannot outlive `self`, which owns
        // and keeps the descriptor open for the duration of this borrow.
        unsafe { fd::BorrowedFd::borrow_raw(fd::AsRawFd::as_raw_fd(&self.0)) }
    }
}
