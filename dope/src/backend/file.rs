use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, RawFd};

pub struct OsFile {
    inner: File,
}

impl OsFile {
    pub fn create(path: &str) -> io::Result<Self> {
        let inner = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Self { inner })
    }

    pub fn fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}
