use std::num::NonZero;

use crate::io::file::Kind;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Missing(NonZero<u32>),
    Negative(i64),
    Overflow(u64),
}

/// Native metadata for one opened or named file.
#[derive(Clone, Copy, Debug)]
pub struct Metadata {
    kind: Kind,
    size: i64,
}

impl Metadata {
    pub(super) const fn new(raw: libc::stat) -> Self {
        Self {
            kind: Kind::from_mode(raw.st_mode),
            size: raw.st_size,
        }
    }

    pub const fn kind(self) -> Kind {
        self.kind
    }

    /// Exact signed st_size reported by the native metadata operation.
    pub const fn size(self) -> i64 {
        self.size
    }
}
