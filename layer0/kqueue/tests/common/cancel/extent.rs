use std::num::NonZeroU32;

use dope_sys::memory;
use o3::{base::num::bounded, buffer::bytes::bounded::MAX};

pub struct Extent;

type Length = bounded::U32<0, { MAX as u32 }>;

// SAFETY: Length retains the original admitted count and Capacity is unchanged.
unsafe impl memory::raw::Extent for Extent {
    type Length = Length;
    type Capacity = NonZeroU32;
}
