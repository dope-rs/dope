use std::os::fd;

use dope_core::{
    Errno,
    io::fs::{self, File},
};

use crate::file::LockError;

/// An owned regular file retaining its exclusive advisory lock.
#[repr(transparent)]
pub struct Locked(File);

impl Locked {
    /// Acquires the lock and validates the file kind and length once.
    pub fn try_new(file: File) -> Result<(Self, fs::Offset), LockError> {
        file.try_lock().map_err(LockError::Lock)?;
        let metadata = file.metadata().map_err(LockError::Metadata)?;
        let kind = metadata.kind();
        if !kind.is_regular() {
            return Err(LockError::NotRegular(kind));
        }
        let length = metadata.len().map_err(LockError::Length)?;
        Ok((Self(file), fs::Offset::from(length)))
    }

    pub(super) fn try_read_at<'a>(
        &self,
        bytes: &'a mut [u8],
        offset: fs::Offset,
    ) -> Result<&'a mut [u8], Errno> {
        self.0.try_read_at(bytes, offset)
    }
}

impl fd::AsFd for Locked {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.0.as_fd()
    }
}
