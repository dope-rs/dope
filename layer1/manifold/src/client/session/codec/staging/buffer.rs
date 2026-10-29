use std::{num::NonZero, ops};

use o3::buffer::{
    self,
    resident::staging::{Buf, reserve},
    storage::range,
};

use crate::client::session::codec::staging::{Bound, Limit};

/// Exact incremental staging selected by the codec's storage policy.
pub trait Buffer<'d>: Sized + Unpin {
    type Staged: Copy + Unpin;
    type View<'input>: buffer::Read<'input>
    where
        Self: 'input;

    fn limit(&self) -> Limit;
    fn try_reserve(
        &mut self,
        target: Option<NonZero<usize>>,
    ) -> Result<Self::Staged, reserve::Error>;
    fn staged(&self, proof: &Self::Staged) -> &Buf<'d>;
    fn staged_mut(&mut self, proof: &Self::Staged) -> &mut Buf<'d>;
    fn try_view<'input>(
        &'input self,
        proof: &Self::Staged,
        range: ops::Range<usize>,
    ) -> Result<Self::View<'input>, range::Error>;
    fn staged_view<'a, 'input>(view: &'a Self::View<'input>) -> &'a buffer::View<'input, Buf<'d>>
    where
        'd: 'input,
        Self: 'input;
    /// Borrows incremental storage only when this mode owns a buffer.
    fn buffered(&mut self) -> Option<&mut Buf<'d>>;
}

impl<'d> Buffer<'d> for Buf<'d> {
    type Staged = ();
    type View<'input>
        = buffer::View<'input, Self>
    where
        Self: 'input;

    fn limit(&self) -> Limit {
        Limit(Bound::Buffered(self.plan().ceiling()))
    }

    fn try_reserve(&mut self, target: Option<NonZero<usize>>) -> Result<(), reserve::Error> {
        match target {
            Some(target) => Buf::try_reserve(self, target.get()),
            None => Buf::try_grow(self),
        }
    }

    fn staged(&self, _: &()) -> &Buf<'d> {
        self
    }

    fn staged_mut(&mut self, _: &()) -> &mut Buf<'d> {
        self
    }

    fn try_view<'input>(
        &'input self,
        _: &(),
        range: ops::Range<usize>,
    ) -> Result<Self::View<'input>, range::Error> {
        buffer::Cursor::try_get(self, range)
    }

    fn staged_view<'a, 'input>(view: &'a Self::View<'input>) -> &'a buffer::View<'input, Buf<'d>>
    where
        'd: 'input,
        Self: 'input,
    {
        view
    }

    fn buffered(&mut self) -> Option<&mut Buf<'d>> {
        Some(self)
    }
}
