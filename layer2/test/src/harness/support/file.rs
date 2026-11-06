use std::{
    ffi::{CStr, OsStr},
    os::{fd::BorrowedFd, unix::ffi::OsStrExt as _},
    path::Path,
    process, time,
};

use dope_core::{
    Backend, Errno,
    dope_sys::io::file::{self, Dir, File as _, Mode, Open},
    io::fs::path::{self, CStrExt as _},
};
use o3::base::marker::ThreadBound;

const CREATE_ATTEMPTS: u8 = 64;

#[derive(Debug)]
pub enum Error {
    Syscall(Errno),
    Path(path::CopyError),
    Allocation(o3::alloc::Exhausted),
    Time(time::SystemTimeError),
    Exhausted,
}

#[must_use = "keep the fixture directory alive until its files are no longer used"]
pub struct Directory<F: FnOnce(Result<(), Error>)> {
    path: Box<CStr>,
    report: Option<F>,
    _thread: ThreadBound,
}

impl<F: FnOnce(Result<(), Error>)> Directory<F> {
    pub fn try_with(base: &Path, tag: &str, report: F) -> Result<Self, Error> {
        let nonce = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .map_err(Error::Time)?
            .as_nanos();
        let pid = process::id();
        for attempt in 0..CREATE_ATTEMPTS {
            let path = base.join(format!("dt-{tag}-{pid:x}-{nonce:x}-{attempt:x}"));
            let path = Box::try_copy(&path).map_err(Error::Path)?;
            match Backend::mkdir::<Errno>(&path, Mode::new(0o700)) {
                Ok(()) => {
                    return Ok(Self {
                        path,
                        report: Some(report),
                        _thread: ThreadBound::NEW,
                    });
                }
                Err(Errno::ALREADY_EXISTS) => {}
                Err(error) => return Err(Error::Syscall(error)),
            }
        }
        Err(Error::Exhausted)
    }

    pub fn path(&self) -> &Path {
        Path::new(OsStr::from_bytes(self.path.to_bytes()))
    }

    /// Removes the directory now, returning its result instead of calling the observer.
    pub fn try_close(mut self) -> Result<(), Error> {
        let _report = self.report.take();
        self.try_remove()
    }

    /// Removes an owned fixture tree without following entries replaced by symlinks.
    /// The caller retains its own policy for an initially missing root.
    pub fn try_remove(&self) -> Result<(), Error> {
        if Backend::metadata_at::<Errno>(None, &self.path)?
            .kind()
            .is_symlink()
        {
            return Backend::unlink(&self.path);
        }
        remove(None, &self.path)
    }
}

impl<F: FnOnce(Result<(), Error>)> Drop for Directory<F> {
    fn drop(&mut self) {
        if let Some(report) = self.report.take() {
            report(self.try_remove());
        }
    }
}

fn remove(parent: Option<BorrowedFd<'_>>, name: &CStr) -> Result<(), Error> {
    let flags = Open::DIRECTORY | Open::CLOSE_ON_EXEC | Open::NO_FOLLOW;
    let file = loop {
        let result = match parent {
            Some(parent) => Backend::open_at::<Errno>(parent, name, flags, Mode::new(0)),
            None => Backend::open::<Errno>(name, flags, Mode::new(0)),
        };
        match (result, parent) {
            (Err(Errno::INTERRUPTED), _) => {}
            (Err(Errno::NOT_DIRECTORY | Errno::SYMLINK_LOOP), Some(parent)) => {
                return Backend::unlink_at(parent, name);
            }
            (result, _) => break result?,
        }
    };
    let mut directory = Dir::<Backend>::from_fd::<Errno>(file)?;
    let (parent_fd, mut entries) = directory.entries();
    while let Some(entry) = entries.try_next()? {
        if entry.name == c"." || entry.name == c".." {
            continue;
        }
        let result = if entry.kind.is_some_and(|kind| !kind.is_dir()) {
            Backend::unlink_at(parent_fd, entry.name)
        } else {
            remove(Some(parent_fd), entry.name)
        };
        match result {
            Ok(()) | Err(Error::Syscall(Errno::NOT_FOUND)) => {}
            Err(error) => return Err(error),
        }
    }
    match Backend::remove_dir_at(parent, name) {
        Ok(()) | Err(Errno::NOT_FOUND) => Ok(()),
        Err(error) => Err(Error::Syscall(error)),
    }
}

impl From<Errno> for Error {
    fn from(error: Errno) -> Self {
        Self::Syscall(error)
    }
}

impl From<file::OpenError<Errno>> for Error {
    fn from(error: file::OpenError<Errno>) -> Self {
        match error {
            file::OpenError::Syscall(error) => Self::Syscall(error),
            file::OpenError::Allocation(error) => Self::Allocation(error),
        }
    }
}
