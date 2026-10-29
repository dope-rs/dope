mod error;
mod ffi;

use std::{os::unix, path::Path};

pub use error::Error;
use o3::{alloc, alloc::VecExt, mem::rc};

use crate::io::fs;

const MAX_COMPONENT_BYTES: usize = 255;
const _: () = assert!(MAX_COMPONENT_BYTES == u8::MAX as usize);

#[derive(Clone, Copy)]
pub struct Component<'a> {
    bytes: &'a [u8],
}

pub struct Owned {
    bytes: Box<std::ffi::CStr>,
}

pub struct Confined {
    pub(super) directory: fs::Dir,
    pub(super) path: Owned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfineError {
    Allocation(alloc::Error),
    Retain(rc::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopyError {
    Nul { position: usize },
    Allocation(alloc::Error),
}

/// Fallible copying of a Unix path into an owned C string.
pub trait CStrExt: Sized {
    fn try_copy(path: &Path) -> Result<Self, CopyError>;
}

impl CStrExt for Box<std::ffi::CStr> {
    #[inline]
    fn try_copy(path: &Path) -> Result<Self, CopyError> {
        let bytes = unix::ffi::OsStrExt::as_bytes(path.as_os_str());
        if let Ok(prefix) = std::ffi::CStr::from_bytes_until_nul(bytes) {
            return Err(CopyError::Nul {
                position: prefix.to_bytes().len(),
            });
        }
        try_copy(bytes).map_err(CopyError::Allocation)
    }
}

#[derive(Clone, Copy)]
pub struct Entry<'a> {
    directory: &'a fs::Dir,
    name: &'a Owned,
}

#[derive(Clone, Copy)]
pub struct Rename<'a> {
    directory: &'a fs::Dir,
    old: &'a Owned,
    new: &'a Owned,
}

impl<'a> Component<'a> {
    pub fn parse_os(value: &'a std::ffi::OsStr) -> Result<Self, Error> {
        Self::parse_bytes(unix::ffi::OsStrExt::as_bytes(value))
    }

    fn parse_bytes(bytes: &'a [u8]) -> Result<Self, Error> {
        if bytes.is_empty() {
            return Err(Error::Empty);
        }
        if bytes == b".." {
            return Err(Error::Parent { offset: 0 });
        }
        if bytes == b"." {
            return Err(Error::NoComponent { length: 1 });
        }
        if let Some(offset) = bytes.iter().position(|byte| *byte == b'/') {
            return Err(Error::Nested { offset });
        }
        if let Some(pos) = bytes.iter().position(|byte| *byte == 0) {
            return Err(Error::Nul { position: pos });
        }
        if bytes.len() > MAX_COMPONENT_BYTES {
            return Err(Error::Component {
                offset: 0,
                length: bytes.len(),
                limit: MAX_COMPONENT_BYTES,
            });
        }
        Ok(Self { bytes })
    }

    pub fn try_to_owned(self) -> Result<Owned, alloc::Error> {
        let bytes = try_copy(self.bytes)?;
        Ok(Owned { bytes })
    }
}

/// Callers have rejected interior NULs before copying the admitted path bytes.
#[inline]
fn try_copy(base: &[u8]) -> Result<Box<std::ffi::CStr>, alloc::Error> {
    let len = base.len().checked_add(1).ok_or(alloc::Error::Overflow)?;
    let mut bytes: Vec<u8> = VecExt::try_new(len)?;
    bytes.extend_from_slice(base);
    bytes.push(0);
    Ok(ffi::finish(bytes))
}

impl Owned {
    pub(crate) fn as_c_str(&self) -> &std::ffi::CStr {
        &self.bytes
    }
}

impl Confined {
    pub(in crate::io::fs) fn new(dir: fs::Dir, path: Owned) -> Self {
        Self {
            directory: dir,
            path,
        }
    }

    pub(crate) fn view(&self) -> Entry<'_> {
        Entry::new(&self.directory, &self.path)
    }
}

impl Confined {
    pub fn into_owned(self) -> Owned {
        self.path
    }
}

impl<'a> Entry<'a> {
    pub(in crate::io::fs) fn new(dir: &'a fs::Dir, name: &'a Owned) -> Self {
        Self {
            directory: dir,
            name,
        }
    }

    pub(crate) fn directory(self) -> &'a fs::Dir {
        self.directory
    }

    pub(crate) fn name(self) -> &'a std::ffi::CStr {
        self.name.as_c_str()
    }
}

impl<'a> Rename<'a> {
    pub(in crate::io::fs) fn new(dir: &'a fs::Dir, old: &'a Owned, new: &'a Owned) -> Self {
        Self {
            directory: dir,
            old,
            new,
        }
    }

    pub(crate) fn directory(self) -> &'a fs::Dir {
        self.directory
    }

    pub(crate) fn old(self) -> &'a std::ffi::CStr {
        self.old.as_c_str()
    }

    pub(crate) fn new_name(self) -> &'a std::ffi::CStr {
        self.new.as_c_str()
    }
}

const _: () = {
    assert!(std::mem::size_of::<Owned>() == 2 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Confined>() == 3 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Entry<'_>>() == 2 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Rename<'_>>() == 3 * std::mem::size_of::<usize>());
};
