use o3::{base::num::bounded, buffer::bytes::bounded::MAX};

use crate::io::socket::gso::Mode;

/// Native segmentation ownership and statically bounded payload limits.
pub trait Source {
    type Mode: Mode;

    const LIMITS: Option<(
        bounded::U32<1, { MAX as u32 }>,
        bounded::U32<2, { u16::MAX as u32 }>,
    )>;
}
