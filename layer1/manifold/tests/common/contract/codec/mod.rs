use std::{convert, mem, num, ops};

use dope_manifold::client::session::codec::{
    Codec, Finish, Input, Parse, Stateless, eof,
    staging::{Buffered, Policy},
};
use o3::{
    buffer,
    buffer::{hold, resident, resident::staging, storage::range, write::consume::scope},
};

pub mod errors;

/// A zero-sized codec that never completes a frame within `N` bytes.
///
/// EOF rejects any partial input.
#[derive(Clone, Copy, Default)]
pub struct Pending<const N: usize = 1>;

impl<const N: usize> Codec for Pending<N> {
    type Head<'input, 'd, R: buffer::Cursor>
        = convert::Infallible
    where
        R: 'input,
        'd: 'input;
    type Error = errors::Truncated;
    type Staging = Buffered;

    fn staging(&self) -> Result<staging::Plan, staging::Error> {
        let limit = N.max(1);
        staging::Plan::new(limit, limit)
    }
}

impl<const N: usize> Stateless for Pending<N> {
    fn parse<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        input: Input<'id, 'input, 'd, R>,
    ) -> Result<Parse<'id, Self::Head<'input, 'd, R>>, Self::Error>
    where
        'd: 'input,
    {
        Ok(match input.need_more(None) {
            Some(need) => Parse::More(need),
            None => Parse::CapacityExhausted,
        })
    }
}

impl<const N: usize> eof::Finish for Pending<N> {
    fn finish<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        input: Input<'id, 'input, 'd, R>,
    ) -> Result<Finish<Self::Head<'input, 'd, R>>, Self::Error>
    where
        'd: 'input,
    {
        if input.is_empty() {
            Ok(Finish::Empty)
        } else {
            Err(errors::Truncated)
        }
    }
}

/// A zero-sized codec that treats every inbound byte as a protocol failure.
#[derive(Clone, Copy, Default)]
pub struct Outbound;

impl Codec for Outbound {
    type Head<'input, 'd, R: buffer::Cursor>
        = convert::Infallible
    where
        R: 'input,
        'd: 'input;
    type Error = errors::Inbound;
    type Staging = Buffered;

    fn staging(&self) -> Result<staging::Plan, staging::Error> {
        staging::Plan::new(1, 1)
    }
}

impl Stateless for Outbound {
    fn parse<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        input: Input<'id, 'input, 'd, R>,
    ) -> Result<Parse<'id, Self::Head<'input, 'd, R>>, Self::Error>
    where
        'd: 'input,
    {
        if input.is_empty() {
            Ok(match input.need_more(None) {
                Some(need) => Parse::More(need),
                None => Parse::CapacityExhausted,
            })
        } else {
            Err(errors::Inbound)
        }
    }
}

impl eof::Finish for Outbound {
    fn finish<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        input: Input<'id, 'input, 'd, R>,
    ) -> Result<Finish<Self::Head<'input, 'd, R>>, Self::Error>
    where
        'd: 'input,
    {
        if input.is_empty() {
            Ok(Finish::Empty)
        } else {
            Err(errors::Inbound)
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Pending<1>>() == 0);
    assert!(mem::size_of::<Outbound>() == 0);
};

pub struct Borrowed;

impl Codec for Borrowed {
    type Head<'input, 'd, R: buffer::Cursor>
        = &'input [u8]
    where
        R: 'input,
        'd: 'input;
    type Error = scope::Error;
    type Staging = Buffered;

    fn staging(&self) -> <Self::Staging as Policy>::Spec {
        staging::Plan::new(1, 1)
    }
}

impl Stateless for Borrowed {
    fn parse<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        input: Input<'id, 'input, 'd, R>,
    ) -> Result<Parse<'id, Self::Head<'input, 'd, R>>, Self::Error>
    where
        'd: 'input,
    {
        let bytes = input.as_slice();
        let Some(count) = num::NonZero::new(bytes.len()) else {
            return Ok(match input.need_more(None) {
                Some(need) => Parse::More(need),
                None => Parse::CapacityExhausted,
            });
        };
        let consumed = input.try_consumed(count)?;
        Ok(Parse::Item {
            head: bytes,
            consumed,
        })
    }
}

pub struct Cursor(pub(crate) [u8; 5]);

impl buffer::Source for Cursor {
    type View<'input>
        = &'input [u8]
    where
        Self: 'input;

    fn remaining(&self) -> usize {
        self.0.len()
    }
}

// SAFETY: Immutable fixture views preserve their selected ranges; default retention copies them exactly.
unsafe impl<'d> hold::raw::Retain<'d> for Cursor {}

impl buffer::Cursor for Cursor {
    fn try_get<'input>(
        &'input self,
        range: ops::Range<usize>,
    ) -> Result<Self::View<'input>, range::Error> {
        self.0.as_slice().try_get(range)
    }
}

impl AsRef<[u8]> for Cursor {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

pub fn input<'input, 'd, C: Codec, R: buffer::Cursor, T>(
    codec: &C,
    source: &'input R,
    budget: &'input resident::Budget<'d>,
    run: impl for<'id> FnOnce(Input<'id, 'input, 'd, R>) -> T,
) -> Result<T, staging::Error> {
    let plan = <C::Staging as Policy>::plan(codec.staging())?;
    let limit = <C::Staging as Policy>::limit(plan);
    Ok(Input::with(source, budget, limit, run))
}
