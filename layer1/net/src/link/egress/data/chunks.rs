use dope_core::io::transfer::{self, progress};
use o3::buffer;

use crate::link::egress::data;

/// Allocation-free traversal retaining the original payload borrow.
pub struct Chunks<'a, 'd, B>
where
    B: data::raw::Payload<'d> + 'a,
{
    cursor: B::Cursor<'a>,
}

impl<'a, 'd, B> Chunks<'a, 'd, B>
where
    B: data::raw::Payload<'d> + 'a,
{
    pub fn from_offset(data: &'a B, offset: data::Offset) -> Option<Self> {
        let cursor = data.cursor(offset)?;
        Some(Self { cursor })
    }

    pub fn take(
        &mut self,
        cap: transfer::NonZero,
    ) -> Option<buffer::raw::Chunk<'a, { transfer::MAX_BYTES }>> {
        progress::State::new(transfer::Len::from_nonzero(cap)).scatter(&mut self.cursor)
    }
}
