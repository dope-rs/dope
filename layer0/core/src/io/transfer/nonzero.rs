use std::{mem, num};

use o3::{
    base::num::bounded,
    buffer::{bytes::bounded::Overflow, storage::Zero},
};

use crate::io::{datagram::payload, transfer};

/// A positive byte count representable by every backend and completion ABI.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub struct NonZero(num::NonZero<u32>);

impl NonZero {
    pub(super) const fn new(len: transfer::Len) -> Option<Self> {
        match num::NonZero::new(len.into_raw() as u32) {
            Some(len) => Some(Self(len)),
            None => None,
        }
    }

    /// Admits one nonzero byte count that fits the transfer ABI bound.
    pub const fn try_new(bytes: usize) -> Option<Self> {
        match transfer::Len::try_new(bytes) {
            Ok(len) => Self::new(len),
            Err(_) => None,
        }
    }

    pub(crate) const fn from_payload(len: payload::Len) -> Self {
        Self(len.nonzero())
    }

    pub const fn fixed<const BYTES: u32>() -> Self {
        const { assert!(BYTES > 0 && BYTES <= transfer::MAX_BYTES as u32) };
        Self(num::NonZero::<u32>::MIN.saturating_add(BYTES - 1))
    }

    pub const fn fixed_size<const BYTES: usize>() -> Self {
        const { assert!(BYTES > 0 && BYTES <= transfer::MAX_BYTES) };
        Self(num::NonZero::<u32>::MIN.saturating_add(BYTES as u32 - 1))
    }

    #[must_use]
    pub const fn into_nonzero(self) -> num::NonZero<u32> {
        self.0
    }

    pub(super) const fn bound(self) -> bounded::NonZeroU32<1, { transfer::MAX_BYTES as u32 }> {
        // SAFETY: every private constructor retains Len's upper bound, a
        // const-admitted count, or payload::Len's checked framing extent.
        // The stored NonZero retains positivity through every projection.
        unsafe { bounded::raw::NonZeroProof::assume(self.0) }.retain()
    }

    #[must_use]
    pub const fn widen(self) -> num::NonZero<usize> {
        num::NonZero::<usize>::MIN.saturating_add(self.0.get() as usize - 1)
    }

    /// A positive input and bounded sum place the final offset in `0..i32::MAX`.
    pub fn checked_add(self, bytes: transfer::Len) -> Result<Self, Overflow> {
        let actual = self.0.get() as usize + bytes.into_raw();
        let total = transfer::Len::try_new(actual)?;
        Ok(Self(
            num::NonZero::<u32>::MIN.saturating_add(total.into_raw() as u32 - 1),
        ))
    }
}

impl TryFrom<transfer::Len> for NonZero {
    type Error = Zero;

    fn try_from(len: transfer::Len) -> Result<Self, Self::Error> {
        match Self::new(len) {
            Some(len) => Ok(len),
            None => Err(Zero),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<NonZero>() == mem::size_of::<transfer::Len>());
    assert!(mem::size_of::<Option<NonZero>>() == mem::size_of::<transfer::Len>());
    assert!(mem::size_of::<Result<NonZero, Zero>>() == mem::size_of::<transfer::Len>());
};

impl From<NonZero> for num::NonZero<u32> {
    fn from(bytes: NonZero) -> Self {
        bytes.0
    }
}
