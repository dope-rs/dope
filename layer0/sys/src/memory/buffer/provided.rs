use std::{num, ptr};

use crate::{
    io::{Close, Retry},
    memory,
    memory::{buffer, buffer::canary},
};

/// Selects native pending and completed provided-buffer ownership.
pub trait Source {
    const PROVIDED: u16;
    const CANARY: u16;
    type Entry: Copy;
    type Index: buffer::Index;
    type Context<L: memory::raw::Receive>: Context<Layout = L, Complete = Self::Complete<L::Extent>, Cleanup = Self::Cleanup<L>>;
    type Canary<L: memory::raw::Canary>: canary::raw::Storage<
            Layout = L,
            Index = Self::Index,
            State = Self::Cleanup<L>,
            Error = <Self::Context<L> as Context>::Error,
        >;
    type Cleanup<L: memory::raw::Receive>: Cleanup<Context = Self::Context<L>, Producer = Self::Producer<L>>;
    type Producer<L: memory::raw::Receive>: Producer<
            Layout = L,
            Context = Self::Context<L>,
            Pending = Self::Pending<L::Extent>,
            Index = Self::Index,
            Entry = Self::Entry,
        >;
    type Pending<E: memory::raw::Extent>: buffer::raw::Token<Index = Self::Index, Length = E::Capacity>
        + buffer::raw::Pending<Extent = E, Complete = Self::Complete<E>>;
    type Complete<E: memory::raw::Extent>: buffer::raw::Token<Index = Self::Index, Length = E::Length>;
}

/// Payload ownership and exact completed-prefix projection for one receive ring.
pub trait Context: Close + memory::Context + buffer::raw::Context<Self::Complete> {
    type Error: Failure<Cleanup = Self::Cleanup>;
    type Cleanup: Retry + memory::Failure;
    type Complete: buffer::raw::Token;

    fn new(layout: Self::Layout) -> Result<Self, Self::Error>;
    fn as_ptr(&self) -> *const u8;
}

/// Native ring entries and deferred publication, paired with one payload owner.
pub trait Producer: Close + buffer::raw::Select + Sized {
    type Layout: memory::raw::Receive;
    type Context: Context;
    type Entry: Copy;
    type Error: Open<Context = Self::Context, Cleanup = <Self::Context as Context>::Cleanup>;

    fn new(context: &Self::Context) -> Result<Self, Self::Error>;
    fn flush(&mut self);
    fn layout(&self) -> Self::Layout;
    fn registration(&self) -> (ptr::NonNull<Self::Entry>, num::NonZero<u16>);
}

/// The original mapping failure and any storage still requiring cleanup.
pub trait Failure {
    type Cleanup: Retry + memory::Failure;

    fn into_parts(self) -> (memory::Error, Option<Self::Cleanup>);
}

/// A failed producer allocation that can retire its paired payload owner.
pub trait Open: Failure {
    type Context: Context;

    fn close(self, context: Self::Context) -> <Self::Context as Context>::Error;
}

/// Exact native storage states retained until all mappings are released.
pub trait Cleanup: Retry + memory::Failure {
    type Context: Context;
    type Producer: Producer<Context = Self::Context>;

    fn entries(current: <Self::Producer as Close>::State, context: Self::Context) -> Self;
    fn payload(current: <Self::Context as Close>::State) -> Self;
    fn producer(current: <Self::Producer as Close>::State) -> Self;
}
