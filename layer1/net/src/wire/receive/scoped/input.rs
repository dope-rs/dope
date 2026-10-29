use std::ops;

use dope_core::io::transfer;
use hold::scope;
use o3::{
    buffer,
    buffer::{hold, resident, storage::range, write::consume},
};

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

    pub fn retention(&self) -> scope::Input<'_, 'd, Chunk<'w, 'd, W, Disabled>> {
        scope::Input::new(self.chunk, self.budget)
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

impl<'w, 'd: 'w, W: wire::Spec + 'w> buffer::Source for Input<'_, 'w, 'd, W> {
    type View<'input>
        = <Chunk<'w, 'd, W, Disabled> as buffer::Source>::View<'input>
    where
        Self: 'input;

    fn remaining(&self) -> usize {
        self.chunk.remaining()
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w> transfer::Length for Input<'_, 'w, 'd, W> {
    fn len(&self) -> transfer::Len {
        self.chunk.len()
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w> buffer::Cursor for Input<'_, 'w, 'd, W>
where
    Chunk<'w, 'd, W, Disabled>: buffer::Cursor,
{
    fn try_get<'input>(
        &'input self,
        range: ops::Range<usize>,
    ) -> Result<Self::View<'input>, range::Error> {
        self.chunk.try_get(range)
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

impl<'w, 'd: 'w, W: wire::Spec + 'w> consume::Consume for Input<'_, 'w, 'd, W>
where
    Chunk<'w, 'd, W, Disabled>: buffer::Cursor + consume::Consume,
{
    fn remaining(&self) -> usize {
        self.chunk.as_ref().len()
    }

    fn try_consume(&mut self, amount: usize) -> Result<(), consume::Error> {
        self.chunk.try_consume(amount)
    }

    fn consume_max(&mut self, max: usize) -> usize {
        self.chunk.consume_max(max)
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w> buffer::Complete for Input<'_, 'w, 'd, W>
where
    Chunk<'w, 'd, W, Disabled>: buffer::Complete,
{
    fn view(&self) -> Self::View<'_> {
        self.chunk.view()
    }
}
