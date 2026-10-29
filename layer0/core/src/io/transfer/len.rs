use o3::{base::num::bounded, buffer::bytes::bounded::Overflow};

use crate::io::transfer;

/// A byte count proven representable by every backend and completion ABI.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(transparent)]
pub struct Len(bounded::U32<0, { transfer::MAX_BYTES as u32 }>);

impl Len {
    pub const ZERO: Self = Self(bounded::U32::clamp(0));
    pub const MAX: Self = Self(bounded::U32::clamp(transfer::MAX_BYTES));

    /// A compile-time byte count proven representable by every backend.
    pub const fn fixed<const BYTES: usize>() -> Self {
        const { assert!(BYTES <= transfer::MAX_BYTES) };
        Self(bounded::U32::clamp(BYTES))
    }

    pub(crate) const fn from_bound(bytes: bounded::U32<0, { transfer::MAX_BYTES as u32 }>) -> Self {
        Self(bytes)
    }

    /// Consumes a fixed destination's count without checking its value again.
    pub const fn from_count<const MAX: usize>(bytes: bounded::Usize<MAX>) -> Self {
        Self(bytes.into_u32())
    }

    pub const fn from_nonzero(bytes: transfer::NonZero) -> Self {
        Self(bytes.bound().into_u32().widen())
    }

    pub const fn try_new(bytes: usize) -> Result<Self, Overflow> {
        match bounded::U32::try_usize(bytes) {
            Ok(bytes) => Ok(Self(bytes)),
            Err(error) => Err(Overflow::from_num(error)),
        }
    }

    /// Selects a byte prefix no longer than an already-proven limit.
    #[must_use]
    pub const fn prefix(bytes: usize, limit: Self) -> Self {
        Self(bounded::U32::prefix(bytes, limit.0))
    }

    pub const fn checked_add(self, added: Self) -> Result<Self, Overflow> {
        let actual = self.0.get() as usize + added.0.get() as usize;
        Self::try_new(actual)
    }

    /// Returns the positive remainder, or `None` when `used` exhausts it.
    #[must_use]
    pub const fn checked_sub(self, used: Self) -> Option<transfer::NonZero> {
        if self.0.get() <= used.0.get() {
            None
        } else {
            transfer::NonZero::new(self.abs_diff(used))
        }
    }

    #[must_use]
    pub const fn abs_diff(self, bytes: Self) -> Self {
        Self(self.0.abs_diff(bytes.0))
    }

    #[must_use]
    pub const fn min(self, bytes: Self) -> Self {
        if self.0.get() <= bytes.0.get() {
            self
        } else {
            bytes
        }
    }

    /// Classifies this exact count without treating zero as a failed conversion.
    #[must_use]
    pub const fn nonzero(self) -> Option<transfer::NonZero> {
        transfer::NonZero::new(self)
    }

    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.0.get() == 0
    }

    pub const fn is_greater(self, other: Self) -> bool {
        self.0.get() > other.0.get()
    }

    pub const fn is_at_least(self, other: Self) -> bool {
        self.0.get() >= other.0.get()
    }

    #[must_use]
    pub const fn into_raw(self) -> usize {
        self.0.get() as usize
    }
}

const _: () = {
    assert!(Len::prefix(transfer::MAX_BYTES + 1, Len::MAX).into_raw() == transfer::MAX_BYTES);
    assert!(matches!(
        Len::ZERO.checked_add(Len::MAX),
        Ok(value) if value.into_raw() == transfer::MAX_BYTES
    ));
    assert!(matches!(
        Len::MAX.checked_add(Len::fixed::<1>()),
        Err(cause) if cause.actual() == transfer::MAX_BYTES + 1
    ));
    assert!(Len::fixed::<1>().abs_diff(Len::fixed::<1>()).is_zero());
    assert!(Len::ZERO.abs_diff(Len::fixed::<1>()).into_raw() == 1);
    assert!(std::mem::size_of::<Len>() == std::mem::size_of::<u32>());
};

impl From<bounded::U32<0, { transfer::MAX_BYTES as u32 }>> for Len {
    fn from(bytes: bounded::U32<0, { transfer::MAX_BYTES as u32 }>) -> Self {
        Self(bytes)
    }
}
impl From<Len> for bounded::U32<0, { transfer::MAX_BYTES as u32 }> {
    fn from(bytes: Len) -> Self {
        bytes.0
    }
}
