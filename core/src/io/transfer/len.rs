use std::io;

use o3::num::bounded;

use crate::io::transfer;

type BoundedLen = bounded::U32<0, { transfer::MAX_BYTES as u32 }>;

/// A byte count proven representable by every backend and completion ABI.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Len(BoundedLen);

impl Len {
    pub const ZERO: Self = Self(BoundedLen::clamp_from_usize(0));

    #[doc(hidden)]
    #[must_use]
    pub const fn clamp(bytes: usize) -> Self {
        Self(BoundedLen::clamp_from_usize(bytes))
    }

    #[must_use]
    pub const fn new(bytes: usize) -> Option<Self> {
        match BoundedLen::from_usize(bytes) {
            Some(bytes) => Some(Self(bytes)),
            None => None,
        }
    }

    pub(crate) fn try_io(bytes: usize) -> io::Result<Self> {
        Self::new(bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope: I/O buffer exceeds the completion length limit",
            )
        })
    }

    #[must_use]
    pub const fn checked_add(self, bytes: usize) -> Option<Self> {
        match self.0.checked_add_usize(bytes) {
            Some(total) => Some(Self(total)),
            None => None,
        }
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }

    #[must_use]
    pub const fn into_usize(self) -> usize {
        self.0.into_usize()
    }

    pub(in crate::io) const fn from_bounded(bytes: usize) -> Self {
        debug_assert!(bytes <= transfer::MAX_BYTES);
        Self(BoundedLen::clamp_from_usize(bytes))
    }
}

const _: () = {
    assert!(Len::new(transfer::MAX_BYTES).is_some());
    assert!(Len::new(transfer::MAX_BYTES + 1).is_none());
    assert!(Len::clamp(transfer::MAX_BYTES + 1).get() == transfer::MAX_BYTES as u32);
    assert!(Len::ZERO.checked_add(transfer::MAX_BYTES).is_some());
    assert!(Len::ZERO.checked_add(transfer::MAX_BYTES + 1).is_none());
    assert!(std::mem::size_of::<Len>() == std::mem::size_of::<u32>());
};
