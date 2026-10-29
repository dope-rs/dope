use std::{ops, os::fd, task, time};

use crate::{
    engine::{Buffer, Source, life, register},
    io::reactor::ready::{delivery, store::Quota},
};

type Created<S, C, G> = (
    <S as Source<C>>::Backend,
    <<S as Source<C>>::Backend as Buffer>::Context,
    <S as Create<C, G>>::Receive,
);

/// Arms a shutdown source owned outside the native engine.
pub trait Register {
    /// # Safety
    /// Retain the source's exact descriptor until engine quiescence or destruction,
    /// including when a submission is published before this method returns an error.
    unsafe fn register(&mut self, source: fd::BorrowedFd<'_>) -> Result<(), register::Error>;
}

/// Finishes native retirement and rearms the retained generation source.
pub trait Resume<'a, R>
where
    R: life::Resume + 'a,
    <R::Backend as Poll>::Context: 'a,
{
    /// # Safety
    /// Retain the exact source descriptor continuously until the next native
    /// quiescence or destruction, including when rearming publishes before an error.
    unsafe fn try_resume(resolver: R) -> life::Retirement<R, R>;
}

/// Dispatches completions while the host retains their registered storage.
pub trait Dispatch<'a, C, H, Q: delivery::Quota<H>>: Sized
where
    C: delivery::Context<H>,
{
    /// # Safety
    /// Registered storage and kernel identities remain valid for this driver's flights.
    unsafe fn new(host: &'a mut H, quota: Q) -> Self;

    fn dispatch(
        self,
        emit: impl FnMut(C::Event, &mut H) -> ops::ControlFlow<C::Event>,
    ) -> C::Dispatch;
}

/// Commits and waits while the host retains the registered operation owners.
pub trait Poll {
    type Context;
    type Error: Copy + Eq;

    /// # Safety
    /// Registered storage and kernel identities remain valid for this driver's flights.
    unsafe fn commit<'a, Q: Quota>(
        &'a mut self,
        quota: Q,
        deferred: impl FnOnce() -> bool,
    ) -> Result<task::Poll<()>, Self::Error>
    where
        Self::Context: 'a;

    /// # Safety
    /// Registered storage and kernel identities remain valid for this driver's flights.
    unsafe fn wait<'a, Q: Quota>(
        &'a mut self,
        quota: Q,
        timeout: Option<time::Duration>,
        busy: impl FnOnce() -> bool,
    ) -> Result<(), Self::Error>
    where
        Self::Context: 'a;
}

/// Creates the native runtime and its external receive owners in backend order.
pub trait Create<C, G>: Source<C>
where
    Self::Backend: Buffer,
{
    type Receive;
    type Error;

    /// # Safety
    /// Retain the returned buffer context alongside the backend until native
    /// quiescence and destruction, including every returned partial cleanup owner.
    unsafe fn try_new(config: &G) -> Result<Created<Self, C, G>, Self::Error>;
}
