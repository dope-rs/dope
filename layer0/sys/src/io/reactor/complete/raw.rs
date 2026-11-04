/// Exact eight-byte identity accepted from every kernel bit pattern.
///
/// # Safety
/// The type has no padding, is eight bytes, and admits every bit pattern.
pub unsafe trait Token: Copy + Eq {}
