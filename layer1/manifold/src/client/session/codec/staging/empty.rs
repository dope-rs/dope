use std::{convert::Infallible, marker, num::NonZero, ops};

use o3::{
    buffer::{
        self,
        resident::staging::{Buf, reserve},
        storage::range,
        write::consume,
    },
    mem::rc,
};

use crate::client::session::codec::staging::{Bound, Buffer, Direct, Limit};

/// Zero-capacity staging buffer for codecs that retain no incremental input.
pub struct Empty<'d> {
    brand: marker::PhantomData<*mut &'d ()>,
}

impl<'d> TryFrom<&Direct> for Empty<'d> {
    /// The direct pool has no capacity to exhaust, so acquisition is total.
    type Error = rc::Error;

    fn try_from(_: &Direct) -> Result<Self, Self::Error> {
        Ok(Self {
            brand: marker::PhantomData,
        })
    }
}

impl<'d> Buffer<'d> for Empty<'d> {
    type Staged = Infallible;
    type View<'input>
        = Infallible
    where
        Self: 'input;

    fn limit(&self) -> Limit {
        Limit(Bound::Direct)
    }
    fn try_reserve(
        &mut self,
        target: Option<NonZero<usize>>,
    ) -> Result<Infallible, reserve::Error> {
        Err(reserve::Error::Capacity(consume::Error::empty(
            target.unwrap_or(NonZero::<usize>::MIN),
        )))
    }
    fn staged(&self, proof: &Infallible) -> &Buf<'d> {
        match *proof {}
    }
    fn staged_mut(&mut self, proof: &Infallible) -> &mut Buf<'d> {
        match *proof {}
    }
    fn try_view(
        &self,
        proof: &Infallible,
        _: ops::Range<usize>,
    ) -> Result<Infallible, range::Error> {
        match *proof {}
    }
    fn staged_view<'a, 'input>(view: &'a Infallible) -> &'a buffer::View<'input, Buf<'d>>
    where
        'd: 'input,
        Self: 'input,
    {
        match *view {}
    }
    fn buffered(&mut self) -> Option<&mut Buf<'d>> {
        None
    }
}

const _: () = assert!(size_of::<Empty<'static>>() == 0);
