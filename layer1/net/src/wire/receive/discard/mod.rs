//! Type-selected raw receive discard.

use dope_core::io::{recv, transfer};
use o3::buffer::write::consume::Consume as _;
use receive::mode;

use crate::wire::receive;

mod state;

pub use state::State;

/// A non-empty raw receive prefix owned by one discard request.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Prefix(transfer::NonZero);

impl Prefix {
    pub const ONE: Self = Self(transfer::NonZero::fixed_size::<1>());

    /// Seals an already-proved non-zero length.
    pub const fn new(bytes: transfer::NonZero) -> Self {
        Self(bytes)
    }

    /// Admits a nonzero prefix that fits the transfer ABI bound.
    pub const fn try_new(bytes: usize) -> Option<Self> {
        match transfer::NonZero::try_new(bytes) {
            Some(bytes) => Some(Self(bytes)),
            None => None,
        }
    }

    /// Seals a compile-time non-empty logical prefix.
    pub const fn fixed<const BYTES: usize>() -> Self {
        Self(transfer::NonZero::fixed_size::<BYTES>())
    }

    fn consume(self, bytes: &mut recv::Lease<'_>) -> Option<Self> {
        let total = transfer::Len::from_nonzero(self.0);
        let used = transfer::Len::prefix(bytes.consume_max(total.into_raw()), total);
        total.checked_sub(used).map(Self)
    }
}

/// Storage and operations selected by a wire's raw receive capability.
/// Wires select a capability marker rather than supplying policy objects.
pub trait Policy: mode::Flag {
    /// Per-connection discard state selected by this capability.
    type State;

    /// Initial discard state.
    const STATE: Self::State;

    /// Starts discarding an exact raw prefix.
    fn begin(state: &mut Self::State, prefix: Prefix) -> Start;

    /// Reports whether the connection still owns an incomplete raw prefix.
    fn is_pending(state: &Self::State) -> bool;

    /// Returns the input remainder and whether this call completed an active prefix.
    fn consume(state: &mut Self::State, bytes: &mut recv::Lease<'_>) -> (transfer::Len, bool);
}

/// Exact outcome of starting a raw-prefix discard.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Start {
    Started,
    Busy,
    Closing,
    Unsupported {
        /// Exact raw-prefix length rejected by this capability.
        requested: Prefix,
    },
}

impl Policy for mode::Disabled {
    type State = ();

    const STATE: Self::State = ();

    fn begin(state: &mut Self::State, prefix: Prefix) -> Start {
        let () = *state;
        Start::Unsupported { requested: prefix }
    }

    fn is_pending(_: &Self::State) -> bool {
        false
    }

    fn consume(state: &mut Self::State, bytes: &mut recv::Lease<'_>) -> (transfer::Len, bool) {
        let () = *state;
        (bytes.len(), false)
    }
}

impl Policy for mode::Enabled {
    type State = State;

    const STATE: Self::State = State::EMPTY;

    fn begin(state: &mut Self::State, prefix: Prefix) -> Start {
        if state.remaining.is_some() {
            return Start::Busy;
        }
        state.remaining = Some(prefix);
        Start::Started
    }

    fn is_pending(state: &Self::State) -> bool {
        state.remaining.is_some()
    }

    fn consume(state: &mut Self::State, bytes: &mut recv::Lease<'_>) -> (transfer::Len, bool) {
        let Some(prefix) = state.remaining else {
            return (bytes.len(), false);
        };
        state.remaining = prefix.consume(bytes);
        (bytes.len(), state.remaining.is_none())
    }
}

const _: () = {
    use std::mem;

    assert!(mem::size_of::<Prefix>() == mem::size_of::<transfer::NonZero>());
    assert!(mem::size_of::<Option<Prefix>>() == mem::size_of::<transfer::NonZero>());
    assert!(mem::size_of::<<mode::Disabled as Policy>::State>() == 0);
    assert!(mem::size_of::<<mode::Enabled as Policy>::State>() == mem::size_of::<Prefix>());
    assert!(mem::size_of::<Start>() == 2 * mem::size_of::<Prefix>());
};
