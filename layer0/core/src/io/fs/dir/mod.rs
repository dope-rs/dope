use std::{
    mem::{align_of, size_of},
    os::fd,
    path::Path,
};

use dope_sys::io::file::{File as _, Mode, Open};
use o3::{alloc, mem};

mod raw;

use crate::{
    Backend,
    io::fs::{open, path},
};

pub struct Dir {
    inner: mem::raw::Owner<raw::Header>,
}

impl Dir {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, open::Error> {
        use open::Error;
        use path::CStrExt as _;

        let path = Box::try_copy(path.as_ref())?;
        let file = Backend::open::<dope_sys::Errno>(
            &path,
            Open::DIRECTORY | Open::CLOSE_ON_EXEC,
            Mode::new(0),
        )
        .map_err(Error::Directory)?;
        Ok(Self {
            inner: raw::Header::try_new(file)
                .map_err(|error| Error::Allocation(alloc::Error::from(error)))?,
        })
    }

    pub fn confine_name(
        &self,
        name: path::Component<'_>,
    ) -> Result<path::Confined, path::ConfineError> {
        use path::ConfineError;

        let path = name.try_to_owned().map_err(ConfineError::Allocation)?;
        Ok(path::Confined::new(
            Self {
                inner: self.inner.try_clone().map_err(ConfineError::Retain)?,
            },
            path,
        ))
    }

    pub fn entry<'a>(&'a self, name: &'a path::Owned) -> path::Entry<'a> {
        path::Entry::new(self, name)
    }

    pub fn rename<'a>(&'a self, old: &'a path::Owned, new: &'a path::Owned) -> path::Rename<'a> {
        path::Rename::new(self, old, new)
    }
}

impl fd::AsFd for Dir {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.inner.get().file.as_fd()
    }
}

const _: () = {
    assert!(size_of::<Dir>() == size_of::<usize>());
    assert!(size_of::<Option<Dir>>() == size_of::<usize>());
    assert!(align_of::<Dir>() == align_of::<usize>());
};
