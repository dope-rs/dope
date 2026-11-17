use std::mem;

pub use dope_sys::io::file::{Identity, MetadataError as Error, Owner};

use crate::io::fs::{Kind, Len};

pub type Output = <crate::Backend as dope_sys::io::file::File>::Metadata;
pub type Value = (Kind, Len, Option<Identity>);

/// Exact signed file length rejected before entering the filesystem domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Negative([u32; 2]);

impl Negative {
    pub(super) const fn new(value: i64) -> Self {
        let bits = value as u64;
        Self([bits as u32, (bits >> 32) as u32])
    }

    pub const fn into_i64(self) -> i64 {
        (self.0[0] as u64 | ((self.0[1] as u64) << 32)) as i64
    }
}

const _: () = {
    assert!(mem::size_of::<Negative>() == mem::size_of::<i64>());
    assert!(mem::align_of::<Negative>() == mem::align_of::<u32>());
};

/// Descriptor metadata retains its signed length until the caller validates it.
#[derive(Clone, Copy, Debug)]
pub struct Metadata {
    kind: Kind,
    length: i64,
    owner: Owner,
}

impl Metadata {
    pub(super) fn new(value: dope_sys::io::file::Metadata) -> Self {
        Self {
            kind: value.kind(),
            length: value.size(),
            owner: value.owner(),
        }
    }

    pub const fn kind(&self) -> Kind {
        self.kind
    }

    pub const fn owner(&self) -> Owner {
        self.owner
    }

    pub fn len(&self) -> Result<Len, Negative> {
        Len::try_from(self.length)
    }

    pub fn read(path: &std::ffi::CStr) -> Result<Self, crate::Errno> {
        use dope_sys::io::file::File as _;

        use crate::Backend;
        Backend::stat(path).map(Self::new)
    }
}
