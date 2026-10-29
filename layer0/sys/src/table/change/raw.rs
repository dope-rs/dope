/// # Safety
/// Copies preserve domains: capacities have <= `MAX` slots; `Physical::into` keeps its index.
/// Iteration yields exactly `count` indices from zero in ascending order; `nth` returns the ordinal-th.
pub unsafe trait Slots {
    type Index: Copy + Eq;
    type Physical: Copy + Eq + Into<Self::Index>;
    type Capacity: Copy
        + IntoIterator<Item = Self::Index, IntoIter: DoubleEndedIterator + ExactSizeIterator>;
    type Data: Copy + Into<usize>;

    const MAX: usize;

    fn offset(index: Self::Index) -> usize;
    fn count(capacity: Self::Capacity) -> usize;
    /// Returns the `ordinal`-th index of `capacity`.
    /// # Safety
    /// `ordinal` is below `count(capacity)`.
    unsafe fn nth(capacity: Self::Capacity, ordinal: usize) -> Self::Index;
}

/// A borrowed native changelist with a type-level upper bound.
///
/// # Safety
/// `as_slice` must never return more than `MAX` events.
pub unsafe trait Batch {
    type Event;

    const MAX: usize;

    fn as_slice(&self) -> &[Self::Event];
}

// SAFETY: an array contains exactly N events.
unsafe impl<T, const N: usize> Batch for [T; N] {
    type Event = T;

    const MAX: usize = N;

    fn as_slice(&self) -> &[T] {
        self
    }
}
