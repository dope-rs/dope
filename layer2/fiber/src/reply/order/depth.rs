//! Ordered reply depth.

use o3::depot::capacity;

/// Protocol order entries retained by one lane, including replies and boundaries.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Depth(pub(in crate::reply) capacity::Count);

impl Depth {
    pub const ZERO: Self = Self(capacity::Count::EMPTY);

    pub const fn get(self) -> usize {
        self.0.get()
    }

    pub(in crate::reply) const fn from_order(count: capacity::Count) -> Self {
        Self(count)
    }
}

impl Default for Depth {
    fn default() -> Self {
        Self::ZERO
    }
}
