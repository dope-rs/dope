use std::{ops, os::fd};

use o3::buffer::bytes::{self, bounded};

use crate::{
    Errno,
    io::{
        reactor::{
            complete,
            retry::{Attempt, Complete, Context, Entry, State},
        },
        socket::{addr, msg},
    },
    table,
    table::change,
};

/// A pending send whose borrowed inputs remain owned by its driver flight.
pub trait Send<C: Context>: Sized {
    fn bytes(input: bytes::Prefix<'_, { bounded::MAX }>) -> Self;
    fn message(header: &msg::Header) -> Self;

    /// # Safety
    /// The original input and every message pointee remain initialized, immutable, and live.
    /// Message iovecs and total bytes satisfy the original admitted ABI bounds.
    unsafe fn attempt(&self, file: fd::BorrowedFd<'_>) -> Attempt<C::Outcome>;

    fn connected(file: fd::BorrowedFd<'_>) -> Result<Attempt<C::Unit>, Errno>;
}

/// Drives retained writes while borrowing their descriptor and retry owners.
pub trait Write<'a, C: Context, E: Complete<C>, P>: Sized {
    type Files: table::Files<C::Slots>;
    type Entry: Entry;
    type State: State<Self::Entry>;
    type Send: Send<C>;
    type Failure;
    type Prepared: complete::Cancellation<Event = E>;

    fn new(files: &'a mut Self::Files, state: &'a mut Self::State, poll: &'a mut P) -> Self;
    fn prepare(self, key: C::Key) -> Result<Option<Self::Prepared>, Self::Failure>;
    fn take_fd(&mut self, fd: fd::RawFd) -> ops::ControlFlow<(), Option<C::Key>>;
    fn connect(
        &mut self,
        key: C::Key,
        slot: <C::Slots as change::raw::Slots>::Physical,
        addr: addr::Addr<'_>,
        vacant: impl complete::Vacancy<Event = E>,
    );
    fn shutdown(
        &mut self,
        key: C::Key,
        slot: <C::Slots as change::raw::Slots>::Physical,
        vacant: impl complete::Vacancy<Event = E>,
    );

    /// # Safety
    /// The retained input and every message pointee remain live with their admitted ABI bounds.
    unsafe fn dispatch(
        &mut self,
        key: C::Key,
        error: Option<Errno>,
        vacant: impl complete::Vacancy<Event = E>,
    );

    /// # Safety
    /// The original write input retains its admitted bounds and pointees through completion.
    unsafe fn send(
        &mut self,
        key: C::Key,
        slot: <C::Slots as change::raw::Slots>::Physical,
        make: impl FnOnce() -> Self::Send,
        vacant: impl complete::Vacancy<Event = E>,
    );
}
