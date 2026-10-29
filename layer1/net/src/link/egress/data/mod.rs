use std::array;

pub use bound::Bound;
pub use chunks::Chunks;
use dope_core::io::transfer;
use o3::{
    buffer,
    buffer::{bytes, storage::inline},
};

use crate::link::egress::ResidentSize;

mod chunks;
pub mod compose;
pub mod exec;
pub mod pair;
pub mod raw;

/// Absolute byte position within one logical payload.
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Offset(usize);

/// Position relative to one payload span.
pub enum Split {
    /// The position lies within the span.
    Before(Offset),
    /// The position lies at or beyond the end of the span.
    After(Offset),
}

impl Offset {
    pub const ZERO: Self = Self(0);

    /// Starts an offset from a transfer length already bounded by the wire.
    pub const fn from_len(bytes: transfer::Len) -> Self {
        Self(bytes.into_raw())
    }

    pub const fn get(self) -> usize {
        self.0
    }

    pub(in crate::link) const fn is_at_least(self, bytes: usize) -> bool {
        self.0 >= bytes
    }

    pub const fn split_at(self, at: usize) -> Split {
        if self.0 < at {
            Split::Before(self)
        } else {
            Split::After(Self(self.0 - at))
        }
    }

    /// Selects the nonempty tail beginning at this position.
    pub fn tail(self, bytes: &[u8]) -> Option<&[u8]> {
        bytes.get(self.0..).filter(|tail| !tail.is_empty())
    }
}

const _: () = assert!(std::mem::size_of::<Offset>() == std::mem::size_of::<usize>());

/// Startup-time execution demand over the full retained allocation.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Execution {
    Fallible,
    Bounded(ResidentSize),
    Unbounded,
}

mod bound;

/// Fixed-capacity bytes for allocation-free control frames.
#[repr(transparent)]
pub struct Inline<const N: usize>(pub inline::Bytes<N, u16>);

impl<const N: usize> Default for Inline<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> Inline<N> {
    pub const fn new() -> Self {
        Self(inline::Bytes::new())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<const CAP: usize, const N: usize> From<[u8; N]> for Inline<CAP> {
    fn from(bytes: [u8; N]) -> Self {
        Self(bytes.into())
    }
}

impl<const N: usize> From<array::IntoIter<u8, N>> for Inline<N> {
    fn from(source: array::IntoIter<u8, N>) -> Self {
        Self(bytes::Prefix::from(&source).into())
    }
}

impl<const N: usize> AsRef<[u8]> for Inline<N> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<const N: usize> buffer::Residency for Inline<N> {
    fn resident(&self) -> usize {
        N
    }
}

const _: () = assert!(std::mem::size_of::<Inline<514>>() == 516);
const _: () =
    assert!(std::mem::size_of::<Inline<514>>() == std::mem::size_of::<inline::Bytes<514, u16>>());

// SAFETY: a shared borrow fixes the initialized inline prefix and capacity.
unsafe impl<'d, const N: usize> raw::Contiguous<'d> for Inline<N> {}
