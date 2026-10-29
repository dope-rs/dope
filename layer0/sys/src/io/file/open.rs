use std::ops;

/// One exact `open(2)` flag set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Open(libc::c_int);

impl Open {
    pub const NONE: Self = Self(0);
    pub const READ: Self = Self(libc::O_RDONLY);
    pub const WRITE: Self = Self(libc::O_WRONLY);
    pub const READ_WRITE: Self = Self(libc::O_RDWR);
    pub const DIRECTORY: Self = Self(libc::O_RDONLY | libc::O_DIRECTORY);
    pub const CREATE: Self = Self(libc::O_CREAT);
    pub const EXCLUSIVE: Self = Self(libc::O_EXCL);
    pub const TRUNCATE: Self = Self(libc::O_TRUNC);
    pub const NO_FOLLOW: Self = Self(libc::O_NOFOLLOW);
    pub const NONBLOCK: Self = Self(libc::O_NONBLOCK);
    pub const CLOSE_ON_EXEC: Self = Self(libc::O_CLOEXEC);

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub const fn difference(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub const fn raw(self) -> libc::c_int {
        self.0
    }
}

impl ops::BitOr for Open {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}
