use std::num;

use o3::alloc;

/// Allocates the actual address-stable option owner before kernel registration.
pub trait Create: Sized {
    fn try_new(capacity: num::NonZero<u32>) -> Result<Self, alloc::Error>;
}
