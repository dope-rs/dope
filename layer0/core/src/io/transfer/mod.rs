use dope_sys::memory;
pub use len::Len;
pub use nonzero::NonZero;
use o3::buffer::bytes::bounded;
pub use view::View;

use crate::io::transfer;

/// Largest successful byte count representable by every backend completion.
pub const MAX_BYTES: usize = bounded::MAX;

mod len;
mod nonzero;
pub mod progress;
mod view;

/// A value which preserves one backend-representable byte-count proof.
pub trait Length {
    #[must_use]
    fn len(&self) -> Len;

    #[must_use]
    fn is_empty(&self) -> bool {
        self.len().is_zero()
    }
}

impl<T: AsRef<[u8]>> Length for bounded::Value<T> {
    fn len(&self) -> Len {
        Len::from_bound(self.into())
    }
}

pub(crate) enum Extent {}

// SAFETY: both transfer proof types preserve one immutable ABI-bounded count.
unsafe impl memory::raw::Extent for Extent {
    type Length = transfer::Len;
    type Capacity = transfer::NonZero;
}
