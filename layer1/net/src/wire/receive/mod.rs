use std::{marker, ops};

use dope_core::{
    driver::{receive::credit, route::Identity},
    io::{recv, transfer},
};
use mode::access;
use o3::{
    buffer,
    buffer::{hold, resident, storage::range, write::consume},
};
use reserve::Completion as _;

use crate::wire::{
    self, receive,
    terminal::{self, Processed},
};

pub mod batch;
pub mod discard;
pub mod eof;
pub mod mode;
pub mod raw;
pub mod reserve;
pub mod scoped;

/// Receive conversion and resource policy selected by a wire.
pub trait Policy<W: wire::Spec> {
    type Batch<'input>: raw::Batch<'input>
    where
        W: 'input;
    type Cursor<'w, 'd>: access::Cursor<'d, Self::Access> + transfer::Length + 'w
    where
        W: 'w,
        'd: 'w;
    type Access;
    type Eof;
    type Reserve: reserve::Kind<W>;
    type Credit: mode::Credit;
    type Discard: discard::Policy;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: <Self::Reserve as reserve::Kind<W>>::Transaction<'a, 'w, 'd, ID>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, W>,
        end: terminal::Both<'_, terminal::Error<'w, W>>,
    ) -> Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        W: 'w,
        'd: 'w,
        'w: 'a + 'input;

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: <Self::Reserve as reserve::Kind<W>>::Transaction<'a, 'w, 'd, ID>,
        bytes: recv::Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, W>>,
    ) -> Processed<Option<Self::Cursor<'w, 'd>>>
    where
        W: 'w,
        'd: 'w,
        'w: 'a;
}

/// Direct, prefix-preserving plaintext receive conversion.
pub struct Transparent<D = mode::Enabled>(marker::PhantomData<fn() -> D>);

impl<W, D> Policy<W> for Transparent<D>
where
    W: wire::Spec,
    D: discard::Policy,
{
    type Batch<'input>
        = batch::One<&'input [u8]>
    where
        W: 'input;
    type Cursor<'w, 'd>
        = recv::Shared<'d>
    where
        W: 'w,
        'd: 'w;
    type Access = access::Direct;
    type Eof = eof::Clean;
    type Reserve = reserve::Direct;
    type Credit = mode::Disabled;
    type Discard = D;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Guard<ID, W>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, W>,
        end: terminal::Both<'_, terminal::Error<'w, W>>,
    ) -> Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        W: 'w,
        'd: 'w,
        'w: 'a + 'input,
    {
        let output = capacity.one(&*bytes);
        txn.complete();
        end.output(output)
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Guard<ID, W>,
        bytes: recv::Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, W>>,
    ) -> Processed<Option<Self::Cursor<'w, 'd>>>
    where
        W: 'w,
        'd: 'w,
        'w: 'a,
    {
        let output = Some(bytes.into_shared());
        txn.complete();
        end.output(output)
    }
}

/// A wire cursor and its statically selected receive-credit obligation.
#[repr(transparent)]
pub struct Chunk<
    'w,
    'd: 'w,
    W: wire::Spec + 'w,
    C: mode::Credit = <<W as wire::Spec>::Receive as receive::Policy<W>>::Credit,
> {
    inner: (
        <W::Receive as receive::Policy<W>>::Cursor<'w, 'd>,
        C::Guard<'d>,
    ),
}

impl<'w, 'd: 'w, W: wire::Spec + 'w> Chunk<'w, 'd, W, mode::Disabled> {
    pub(crate) fn from_raw(inner: <W::Receive as receive::Policy<W>>::Cursor<'w, 'd>) -> Self {
        Self { inner: (inner, ()) }
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w> Chunk<'w, 'd, W, mode::Disabled>
where
    W::Receive: receive::Policy<W, Credit = mode::Enabled>,
{
    pub(crate) fn retain(self, credit: credit::Return<'d>) -> Chunk<'w, 'd, W, mode::Enabled> {
        Chunk {
            inner: (self.inner.0, credit),
        }
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w, C: mode::Credit> Chunk<'w, 'd, W, C> {
    /// Visits the exact cursor while retaining this chunk's receive credit.
    pub fn drive<T>(&mut self, done: T, use_head: &mut impl access::Use<'d, T>) -> T {
        access::Cursor::drive(&mut self.inner.0, done, use_head)
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w, C: mode::Credit> transfer::Length for Chunk<'w, 'd, W, C> {
    fn len(&self) -> transfer::Len {
        self.inner.0.len()
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w, C: mode::Credit> buffer::Source for Chunk<'w, 'd, W, C> {
    type View<'input>
        = <<W::Receive as receive::Policy<W>>::Cursor<'w, 'd> as buffer::Source>::View<'input>
    where
        Self: 'input;

    fn remaining(&self) -> usize {
        self.inner.0.remaining()
    }
}

// SAFETY: The selected view and all retention operations delegate to the same proven cursor.
unsafe impl<'w, 'd: 'w, W: wire::Spec + 'w, C: mode::Credit> hold::raw::Retain<'d>
    for Chunk<'w, 'd, W, C>
where
    <W::Receive as receive::Policy<W>>::Cursor<'w, 'd>: hold::raw::Retain<'d>,
{
    fn charge<'a>(plan: &Self::View<'a>) -> hold::Charge
    where
        Self: 'a,
    {
        <<W::Receive as receive::Policy<W>>::Cursor<'w, 'd> as hold::raw::Retain<'d>>::charge(plan)
    }

    fn try_copy<'a>(
        plan: &Self::View<'a>,
        budget: &resident::Budget<'d>,
    ) -> Result<resident::Shared<'d>, hold::Error>
    where
        Self: 'a,
    {
        <<W::Receive as receive::Policy<W>>::Cursor<'w, 'd> as hold::raw::Retain<'d>>::try_copy(
            plan, budget,
        )
    }

    fn try_retain<'a>(
        plan: &Self::View<'a>,
        budget: &resident::Budget<'d>,
    ) -> Result<resident::Shared<'d>, hold::Error>
    where
        Self: 'a,
    {
        <<W::Receive as receive::Policy<W>>::Cursor<'w, 'd> as hold::raw::Retain<'d>>::try_retain(
            plan, budget,
        )
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w, C: mode::Credit> buffer::Cursor for Chunk<'w, 'd, W, C>
where
    <W::Receive as receive::Policy<W>>::Cursor<'w, 'd>: buffer::Cursor,
{
    fn try_get<'input>(
        &'input self,
        range: ops::Range<usize>,
    ) -> Result<Self::View<'input>, range::Error> {
        self.inner.0.try_get(range)
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w, C: mode::Credit> AsRef<[u8]> for Chunk<'w, 'd, W, C>
where
    <W::Receive as receive::Policy<W>>::Cursor<'w, 'd>: buffer::Cursor,
{
    fn as_ref(&self) -> &[u8] {
        self.inner.0.as_ref()
    }
}

// SAFETY: the callback wrapper retains its exact stable wire cursor.
unsafe impl<'w, 'd: 'w, W: wire::Spec + 'w, C: mode::Credit> buffer::raw::Consume
    for Chunk<'w, 'd, W, C>
where
    <W::Receive as receive::Policy<W>>::Cursor<'w, 'd>: buffer::Cursor + buffer::raw::Consume,
{
    fn consume<const MAX: u32>(prefix: buffer::raw::Consumed<'_, '_, Self, MAX>) {
        // SAFETY: inner exposes exactly the callback wrapper's current bytes.
        <<W::Receive as receive::Policy<W>>::Cursor<'w, 'd> as buffer::raw::Consume>::consume(
            unsafe { prefix.map(|chunk| &mut chunk.inner.0) },
        );
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w, C: mode::Credit> consume::Consume for Chunk<'w, 'd, W, C>
where
    <W::Receive as receive::Policy<W>>::Cursor<'w, 'd>: buffer::Cursor + consume::Consume,
{
    fn remaining(&self) -> usize {
        self.inner.0.as_ref().len()
    }

    fn try_consume(&mut self, amount: usize) -> Result<(), consume::Error> {
        self.inner.0.try_consume(amount)
    }

    fn consume_max(&mut self, max: usize) -> usize {
        self.inner.0.consume_max(max)
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w, C: mode::Credit> buffer::Complete for Chunk<'w, 'd, W, C>
where
    <W::Receive as receive::Policy<W>>::Cursor<'w, 'd>: buffer::Complete,
{
    fn view(&self) -> Self::View<'_> {
        self.inner.0.view()
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w, C: mode::Credit> access::Sequence<'d> for Chunk<'w, 'd, W, C>
where
    <W::Receive as receive::Policy<W>>::Cursor<'w, 'd>: access::Sequence<'d>,
{
    type Head<'head>
        = <<W::Receive as receive::Policy<W>>::Cursor<'w, 'd> as access::Sequence<'d>>::Head<'head>
    where
        Self: 'head;

    fn next(&mut self) -> Option<Self::Head<'_>> {
        access::Sequence::next(&mut self.inner.0)
    }
}

const _: () = {
    use std::mem;

    use dope_core::io::recv;
    assert!(
        mem::size_of::<Chunk<'static, 'static, wire::Identity>>()
            == mem::size_of::<recv::Shared<'static>>()
    );
    assert!(
        mem::size_of::<Option<Chunk<'static, 'static, wire::Identity>>>()
            == mem::size_of::<Option<recv::Shared<'static>>>()
    );
};
