pub(crate) mod access;
pub(crate) mod domain;
pub mod handles;
pub(crate) mod identity;
mod lease;
pub mod outbound;
pub(crate) mod states;

/// Returns the process's soft and hard descriptor limits unchanged.
#[inline]
pub fn try_limit() -> Result<(u64, u64), crate::Errno> {
    <crate::Backend as dope_sys::io::fd::Fd>::try_limit()
}
