use std::{
    fs, io,
    os::{fd, unix::fs::DirBuilderExt as _},
    path, process, time,
};

use dope_core::{
    Backend, Errno,
    dope_sys::io::file::{self, File as _, Mode, Open},
    io::fs::path::{CStrExt as _, CopyError},
};

const CREATE_ATTEMPTS: u8 = 64;

#[derive(Debug)]
pub enum Error {
    Create(io::Error),
    Time(time::SystemTimeError),
    Exhausted,
}

#[derive(Debug)]
pub enum Remove {
    Path(CopyError),
    Directory(file::OpenError<Errno>),
    Read(file::ReadError<Errno>),
    Io(Errno),
}

impl Remove {
    fn is_not_found(&self) -> bool {
        match self {
            Self::Io(error) | Self::Read(file::ReadError::Syscall(error)) => {
                *error == Errno::NOT_FOUND
            }
            _ => false,
        }
    }
}

#[must_use = "keep the fixture directory alive until its files are no longer used"]
pub struct Directory<F: FnOnce(Result<(), Remove>)> {
    path: path::PathBuf,
    report: Option<F>,
}

impl<F: FnOnce(Result<(), Remove>)> Directory<F> {
    pub fn try_with(base: &path::Path, tag: &str, report: F) -> Result<Self, Error> {
        let nonce = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .map_err(Error::Time)?
            .as_nanos();
        let pid = process::id();
        for attempt in 0..CREATE_ATTEMPTS {
            let path = base.join(format!("dt-{tag}-{pid:x}-{nonce:x}-{attempt:x}"));
            match fs::DirBuilder::new().mode(0o700).create(&path) {
                Ok(()) => {
                    return Ok(Self {
                        path,
                        report: Some(report),
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(Error::Create(error)),
            }
        }
        Err(Error::Exhausted)
    }

    pub fn path(&self) -> &path::Path {
        &self.path
    }

    /// Removes the directory now, returning its result instead of calling the observer.
    pub fn try_close(mut self) -> Result<(), Remove> {
        let _report = self.report.take();
        self.try_remove()
    }

    /// Removes an owned fixture tree without following entries replaced by symlinks.
    /// The caller retains its own policy for an initially missing root.
    pub fn try_remove(&self) -> Result<(), Remove> {
        let path = Box::try_copy(self.path()).map_err(Remove::Path)?;
        let kind = Backend::metadata_at::<Errno>(None, &path)
            .map_err(Remove::Io)?
            .kind();
        if kind.is_symlink() {
            return Backend::unlink::<Errno>(&path).map_err(Remove::Io);
        }
        let dir = open(None, &path).map_err(Remove::Io)?;
        remove_dir(None, &path, dir)
    }
}

impl<F: FnOnce(Result<(), Remove>)> Drop for Directory<F> {
    fn drop(&mut self) {
        if let Some(report) = self.report.take() {
            report(self.try_remove());
        }
    }
}

fn open(parent: Option<fd::BorrowedFd<'_>>, name: &std::ffi::CStr) -> Result<fd::OwnedFd, Errno> {
    let flags = Open::DIRECTORY | Open::NO_FOLLOW | Open::CLOSE_ON_EXEC;
    loop {
        let opened = match parent {
            Some(parent) => Backend::open_at::<Errno>(parent, name, flags, Mode::new(0)),
            None => Backend::open::<Errno>(name, flags, Mode::new(0)),
        };
        match opened {
            Err(error) if error.is_intr() => {}
            result => return result,
        }
    }
}

fn remove_entry(parent: fd::BorrowedFd<'_>, name: &std::ffi::CStr) -> Result<(), Remove> {
    match open(Some(parent), name) {
        Ok(dir) => remove_dir(Some(parent), name, dir),
        Err(Errno::NOT_DIRECTORY | Errno::SYMLINK_LOOP) => {
            Backend::unlink_at::<Errno>(parent, name).map_err(Remove::Io)
        }
        Err(error) => Err(Remove::Io(error)),
    }
}

fn remove_dir(
    parent: Option<fd::BorrowedFd<'_>>,
    name: &std::ffi::CStr,
    dir: fd::OwnedFd,
) -> Result<(), Remove> {
    let mut dir = file::Dir::<Backend>::from_fd::<Errno>(dir).map_err(Remove::Directory)?;
    let (fd, mut entries) = dir.entries();
    while let Some(entry) = entries.try_next::<Errno>().map_err(Remove::Read)? {
        let name = entry.name;
        if matches!(name.to_bytes(), b"." | b"..") {
            continue;
        }
        let removed = if entry.kind.is_some_and(|kind| !kind.is_dir()) {
            Backend::unlink_at::<Errno>(fd, name).map_err(Remove::Io)
        } else {
            remove_entry(fd, name)
        };
        if let Err(error) = removed
            && !error.is_not_found()
        {
            return Err(error);
        }
    }
    match Backend::remove_dir::<Errno>(parent, name) {
        Ok(()) | Err(Errno::NOT_FOUND) => Ok(()),
        Err(error) => Err(Remove::Io(error)),
    }
}
