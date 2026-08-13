use std::num;

/// Receive batch bounded by `MAX_ITEMS`, with producer floor `MIN_CAPACITY`.
/// # Safety
/// Bounds must be ordered, `len()` bounded, and producers must honor every supported capacity.
pub unsafe trait Source: ExactSizeIterator {
    /// Absolute maximum number of items returned by one receive call.
    const MAX_ITEMS: num::NonZeroUsize = num::NonZeroUsize::MIN;

    /// Smallest per-call capacity under which the producer can consume valid input.
    const MIN_CAPACITY: num::NonZeroUsize = Self::MAX_ITEMS;
}
