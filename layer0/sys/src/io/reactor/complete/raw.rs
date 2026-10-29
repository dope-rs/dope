use o3::depot;

/// Exact eight-byte identity accepted from every kernel bit pattern.
///
/// # Safety
/// The type has no padding, is eight bytes, and admits every bit pattern.
pub unsafe trait Token: Copy + Eq {}

/// One immutable bounded completion queue capacity.
/// # Safety
/// Every copy retains the original Count proof.
pub unsafe trait Capacity: depot::raw::Count {}
