use std::ops;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub(crate) struct Flags(u8);

impl Flags {
    pub(crate) const FIXED_FILE: Self = Self(1 << 0);
    pub(crate) const LINK: Self = Self(1 << 2);
    pub(crate) const HARD_LINK: Self = Self(1 << 3);
    pub(crate) const ASYNC: Self = Self(1 << 4);
    pub(crate) const BUFFER_SELECT: Self = Self(1 << 5);
    pub(crate) const SKIP_SUCCESS: Self = Self(1 << 6);

    pub(crate) const fn raw(self) -> u8 {
        self.0
    }
}

impl ops::BitOr for Flags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

const _: () = assert!(std::mem::size_of::<Flags>() == 1);
