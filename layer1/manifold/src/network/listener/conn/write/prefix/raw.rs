use o3::{base::num::bounded, buffer::write::Slice};

use crate::network::listener::writer::memory;

pub(super) fn written(bytes: Slice<'_>) -> memory::Len {
    // SAFETY: Prefix's private constructor fixes this writer's capacity; lent
    // views preserve its initialized count within that exact header.
    let count =
        unsafe { bounded::raw::u32::<0, { memory::WRITE_BUF_CAP as u32 }>(bytes.finish() as u32) };
    memory::Len::from(count)
}
