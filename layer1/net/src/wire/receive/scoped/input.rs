use o3::buffer::{self, resident};

use crate::wire::{
    self,
    receive::{Chunk, mode::Disabled},
};

/// One callback-owned receive cursor and its exact retention authority.
/// A plan keeps its exact projection borrowed until the plan is consumed.
pub struct Input<'input, 'w, 'd: 'w, W: wire::Spec + 'w> {
    chunk: &'input mut Chunk<'w, 'd, W, Disabled>,
    budget: &'input resident::Budget<'d>,
}

impl<'input, 'w, 'd: 'w, W: wire::Spec + 'w> Input<'input, 'w, 'd, W> {
    pub fn new(
        chunk: &'input mut Chunk<'w, 'd, W, Disabled>,
        budget: &'input resident::Budget<'d>,
    ) -> Self {
        Self { chunk, budget }
    }

    pub fn into_parts(
        self,
    ) -> (
        &'input mut Chunk<'w, 'd, W, Disabled>,
        &'input resident::Budget<'d>,
    ) {
        (self.chunk, self.budget)
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w> AsRef<[u8]> for Input<'_, 'w, 'd, W>
where
    Chunk<'w, 'd, W, Disabled>: buffer::Cursor,
{
    fn as_ref(&self) -> &[u8] {
        self.chunk.as_ref()
    }
}
