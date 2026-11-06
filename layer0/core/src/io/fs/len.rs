use std::mem;

use dope_sys::io::file;

use crate::io::fs::{self, metadata};

/// Nonnegative byte length of one file generation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Len(pub(super) file::Offset);

impl Len {
    pub const fn fixed<const BYTES: u64>() -> Self {
        Self(fs::Offset::fixed::<BYTES>().0)
    }

    pub const fn try_new(value: u64) -> Result<Self, fs::Overflow> {
        match fs::Offset::try_new(value) {
            Ok(value) => Ok(Self(value.0)),
            Err(error) => Err(error),
        }
    }
}

impl TryFrom<i64> for Len {
    type Error = metadata::Negative;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match file::Offset::new(value as u64) {
            Some(value) => Ok(Self(value)),
            None => Err(metadata::Negative::new(value)),
        }
    }
}

impl From<file::Offset> for Len {
    fn from(value: file::Offset) -> Self {
        Self(value)
    }
}

const _: () = {
    assert!(mem::size_of::<Len>() == mem::size_of::<u64>());
    assert!(mem::align_of::<Len>() == mem::align_of::<u64>());
};
