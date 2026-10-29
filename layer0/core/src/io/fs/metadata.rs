use std::mem;

use crate::io::fs::{Kind, Len};

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
    pub(super) kind: Kind,
    pub(super) length: i64,
}

impl Metadata {
    pub const fn kind(&self) -> Kind {
        self.kind
    }
    pub fn len(&self) -> Result<Len, Negative> {
        Len::try_from(self.length)
    }

    pub fn read(path: &std::ffi::CStr) -> Result<Self, crate::Errno> {
        use dope_sys::io::file::File as _;

        use crate::Backend;
        let raw = Backend::stat::<dope_sys::Errno>(path)?;
        Ok(Self {
            kind: Kind::from_meta(raw),
            length: raw.size(),
        })
    }
}
