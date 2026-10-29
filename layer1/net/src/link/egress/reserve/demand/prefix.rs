use std::mem;

use dope_core::io::transfer;

/// Copied-prefix length bounded by one exact destination capacity.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Prefix<const MAX: usize>(transfer::Len);

impl<const MAX: usize> Prefix<MAX> {
    pub const FULL: Self = {
        assert!(MAX <= transfer::MAX_BYTES);
        Self(transfer::Len::fixed::<MAX>())
    };

    pub fn min(self, len: transfer::Len) -> Self {
        Self(self.0.min(len))
    }

    pub const fn len(self) -> transfer::Len {
        self.0
    }
}

const _: () = assert!(mem::size_of::<Prefix<1>>() == mem::size_of::<transfer::Len>());
