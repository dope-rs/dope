use std::num::NonZero;

use crate::io::file::Kind;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Identity {
    pub device: libc::dev_t,
    pub inode: libc::ino_t,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Owner {
    pub(super) uid: libc::uid_t,
    pub(super) gid: libc::gid_t,
}

pub type Value = (Kind, super::Offset, Option<Identity>);

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
    identity: Identity,
    owner: Owner,
}

impl Metadata {
    pub(super) const fn new(raw: libc::stat) -> Self {
        Self {
            kind: Kind::from_mode(raw.st_mode),
            size: raw.st_size,
            identity: Identity {
                device: raw.st_dev,
                inode: raw.st_ino,
            },
            owner: Owner {
                uid: raw.st_uid,
                gid: raw.st_gid,
            },
        }
    }

    pub const fn kind(self) -> Kind {
        self.kind
    }

    /// Exact signed st_size reported by the native metadata operation.
    pub const fn size(self) -> i64 {
        self.size
    }

    pub const fn identity(self) -> Identity {
        self.identity
    }

    pub const fn owner(self) -> Owner {
        self.owner
    }
}
