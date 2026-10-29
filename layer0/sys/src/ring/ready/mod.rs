use crate::{memory, ring::map};
mod error;
pub mod raw;
use std::{os::fd, time};

pub use error::Error;
use o3::base::num::bounded;

use crate::{
    io::{
        Close,
        reactor::{complete, submit},
    },
    memory::{buffer, buffer::provided},
    ring::{
        enter,
        enter::{Enter, Poll},
    },
};

type Completion<R> = (
    <R as Complete>::Entry,
    Option<Result<<R as Complete>::Pending, <R as Complete>::Index>>,
);
type Failure<R> = Poll<<R as Ready>::Error, <R as Ready>::CancelError>;

/// Acquires completions and publishes returned storage without submitting native work.
pub trait Complete: Sized {
    type Token: complete::raw::Token;
    type Layout: memory::raw::Receive;
    type Pending: buffer::raw::Token<Index = Self::Index>
        + buffer::raw::Pending<
            Extent = <Self::Layout as memory::raw::Receive>::Extent,
            Complete: buffer::raw::Token<
                Index = Self::Index,
                Length = <<Self::Layout as memory::raw::Receive>::Extent as memory::raw::Extent>::Length,
            >,
        >;
    type Index: Copy;
    type Entry: complete::Entry<Token = Self::Token>;

    fn acquire(&mut self);
    /// Publishes consumed completion ownership without acquiring another batch.
    fn publish(&mut self);
    fn pop(&mut self) -> Option<Completion<Self>>;
    fn refresh(&mut self) -> bounded::U32<0, { u32::MAX }>;
    fn layout(&self) -> Self::Layout;
    fn flush(&mut self);
}

/// A registered reactor and its provided-buffer entries share one owner.
pub trait Ready: Complete + Close<State: map::Unmap> + Sized {
    type Ring;
    type Context: provided::Context;
    type Producer: provided::Producer<Layout = Self::Layout, Context = Self::Context>;
    type Registration;
    type Registered;
    type BufferError;
    type Error;
    type InstallError;
    type CancelError;
    type Target;
    type Queue<'q>: submit::Queue<'q, Self::Token>
    where
        Self: 'q;
    type Enter<'q>: Enter<Error = Self::Error>
    where
        Self: 'q;

    fn submission(&mut self) -> Self::Queue<'_>;
    fn enter(&mut self) -> Self::Enter<'_>;

    fn submit(&mut self) -> Result<(), Failure<Self>>;
    fn commit(&mut self) -> Result<(), Failure<Self>>;
    fn advance(&mut self) -> Result<enter::Progress, Failure<Self>>;
    fn try_wait(&mut self, timeout: Option<time::Duration>) -> Result<(), Failure<Self>>;

    fn install(
        &mut self,
        target: Self::Target,
        file: fd::BorrowedFd<'_>,
    ) -> Result<(), Self::InstallError>;
    fn cancel_all(&mut self) -> Result<(), Failure<Self>>;
}
