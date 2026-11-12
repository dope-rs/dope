pub use bound::Bound;
pub use chunks::Chunks;
use dope_core::io::transfer;
use o3::buffer::storage::inline;

use crate::link::egress::ResidentSize;

mod chunks;
pub mod compose;
pub mod exec;
pub mod pair;
pub mod position;
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
pub type Inline<const N: usize> = inline::Bytes<N, u16>;

const _: () = assert!(std::mem::size_of::<Inline<514>>() == 516);

// SAFETY: a shared borrow fixes the initialized inline prefix and capacity.
unsafe impl<'d, const N: usize> raw::Contiguous<'d> for Inline<N> {}
