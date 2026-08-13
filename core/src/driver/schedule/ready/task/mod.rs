mod admission;
mod domain;
mod error;
mod lease;
#[doc(hidden)]
pub mod raw;

use std::{cell, marker};

pub use admission::Admission;
pub use domain::Domain;
pub use error::Error;
pub use lease::Lease;

/// Maximum task-to-parent edges followed by one wake.
pub const MAX_WAKE_HOPS: usize = 16;

#[doc(hidden)]
pub struct Node<'d> {
    binding: cell::Cell<Option<raw::Binding<'d>>>,
    _pin: marker::PhantomPinned,
    _thread: o3::ThreadBound,
}

impl<'d> Node<'d> {
    pub const fn new() -> Self {
        use std::cell::Cell;

        use o3::ThreadBound;
        Self {
            binding: Cell::new(None),
            _pin: marker::PhantomPinned,
            _thread: ThreadBound::NEW,
        }
    }
}

impl Default for Node<'_> {
    fn default() -> Self {
        Self::new()
    }
}
