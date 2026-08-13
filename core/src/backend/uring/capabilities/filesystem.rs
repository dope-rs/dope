use std::{fs, io, os::fd, path};

use crate::{backend, platform};

impl platform::Filesystem for backend::Uring {
    fn open_directory(path: &path::Path) -> io::Result<fd::OwnedFd> {
        use std::os::unix::fs::OpenOptionsExt;

        let mut options = fs::OpenOptions::new();
        options.read(true);
        OpenOptionsExt::custom_flags(
            &mut options,
            libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC,
        );
        let directory = options.open(path)?;
        Ok(directory.into())
    }
}
