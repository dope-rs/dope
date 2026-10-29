use std::num;

use o3::{base::num::bounded, buffer::bytes::bounded::MAX};

/// # Safety
/// Copies preserve values. Entries are powers of two >= 2; capacity <= `o3::buffer::bytes::bounded::MAX`.
/// The product of entry count and capacity is at most `isize::MAX`.
pub unsafe trait Layout: Copy {
    fn entries(self) -> num::NonZero<u16>;
    fn capacity(self) -> num::NonZero<u32>;
}

/// Exact bounded lengths carried by pending and completed receive owners.
///
/// # Safety
/// Conversions preserve the exact ABI-bounded count and nonzero capacity.
pub unsafe trait Extent {
    type Length: Copy
        + From<bounded::U32<0, { MAX as u32 }>>
        + Into<bounded::U32<0, { MAX as u32 }>>;
    type Capacity: Copy + Into<num::NonZero<u32>>;
}

/// A receive layout retaining its original capacity proof.
///
/// # Safety
/// `limit` must expose exactly the same capacity as `Layout::capacity`.
pub unsafe trait Receive: Layout {
    type Extent: Extent;

    fn limit(self) -> <Self::Extent as Extent>::Capacity;
}

/// Fixed receive storage used by native capability canaries.
/// # Safety
/// `LAYOUT` has two slots, each large enough for a `libc::c_int`.
pub unsafe trait Canary: Receive {
    const LAYOUT: Self;
}
