use dope_sys::io::file::{Mode, Open};
use o3::alloc;

use crate::io::fs::{File, path};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Path(path::Error),
    Allocation(alloc::Error),
    System(crate::Errno),
    Directory(crate::Errno),
}

impl From<path::CopyError> for Error {
    fn from(error: path::CopyError) -> Self {
        match error {
            path::CopyError::Nul { position } => Self::Path(path::Error::Nul { position }),
            path::CopyError::Allocation(error) => Self::Allocation(error),
        }
    }
}

const _: () = assert!(size_of::<Error>() == 4 * size_of::<usize>());

/// Opening policy independent of the selected asynchronous backend.
#[derive(Clone, Copy, Debug)]
pub struct Options {
    flags: Open,
    mode: Mode,
}

#[derive(Clone, Copy, Debug)]
pub enum Access {
    Read,
    Write,
    ReadWrite,
    Directory,
}

#[derive(Clone, Copy, Debug)]
pub enum Creation {
    Existing,
    Create,
    Exclusive,
}

impl Options {
    pub const fn new(access: Access) -> Self {
        let flags = match access {
            Access::Read => Open::READ,
            Access::Write => Open::WRITE,
            Access::ReadWrite => Open::READ_WRITE,
            Access::Directory => Open::DIRECTORY,
        };
        Self {
            flags: flags.union(Open::CLOSE_ON_EXEC),
            mode: Mode::new(0o666),
        }
    }

    pub const fn creation(mut self, creation: Creation) -> Self {
        let flags = match creation {
            Creation::Existing => Open::NONE,
            Creation::Create => Open::CREATE,
            Creation::Exclusive => Open::CREATE.union(Open::EXCLUSIVE),
        };
        self.flags = self
            .flags
            .difference(Open::CREATE.union(Open::EXCLUSIVE))
            .union(flags);
        self
    }

    pub const fn nofollow(mut self) -> Self {
        self.flags = self.flags.union(Open::NO_FOLLOW);
        self
    }

    pub const fn nonblock(mut self) -> Self {
        self.flags = self.flags.union(Open::NONBLOCK);
        self
    }

    pub const fn truncate(mut self) -> Self {
        self.flags = self.flags.union(Open::TRUNCATE);
        self
    }

    pub const fn mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    pub(super) const fn lower(self) -> (Open, Mode) {
        let mode = if self.flags.contains(Open::CREATE) {
            self.mode
        } else {
            Mode::new(0)
        };
        (self.flags, mode)
    }

    pub fn open(self, path: &std::ffi::CStr) -> Result<File, crate::Errno> {
        use dope_sys::io::file::File as _;

        let (flags, mode) = self.lower();
        crate::Backend::open::<dope_sys::Errno>(path, flags, mode).map(File::from)
    }
}
