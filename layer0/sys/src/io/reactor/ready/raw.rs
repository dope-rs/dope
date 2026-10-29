use std::{task, time};

use crate::{
    Budget,
    io::reactor::ready::{
        Backend, Driver,
        delivery::Decoded,
        store::{Exec, Quota},
    },
};

pub trait Decode: Exec {
    /// # Safety
    /// The value was returned by this driver's registered kernel queue.
    unsafe fn decode(raw: usize) -> Decoded<Self::Key>;
    const SHUTDOWN: Self::Event;
}

pub trait Poll<C: Driver>: Backend<C> {
    /// # Safety
    /// Registered operations retain their submitted storage and admitted layouts.
    unsafe fn resume<'a>(&'a mut self, budget: &mut impl Budget)
    where
        C::Context: 'a;

    /// # Safety
    /// Registered operations retain their submitted storage and admitted layouts.
    /// Kernel identities belong to this driver's live or retired flights.
    unsafe fn wait<'a>(
        &'a mut self,
        timeout: Option<time::Duration>,
        changes: &mut impl Budget,
        done: &mut impl Budget,
    ) -> Result<(), Self::Failure>
    where
        C::Context: 'a;
}

pub trait Work<C: Driver, Q: Quota>: Sized {
    type Backend: Poll<C>;

    /// # Safety
    /// Registered operations retain their submitted storage and admitted layouts.
    unsafe fn new<'a>(backend: &'a mut Self::Backend, quota: Q) -> Self
    where
        C::Context: 'a;

    /// # Safety
    /// Registered storage and kernel identities remain valid for this driver's flights.
    unsafe fn commit<'a>(
        self,
        backend: &'a mut Self::Backend,
        dirty: bool,
    ) -> Result<task::Poll<()>, <Self::Backend as Backend<C>>::Failure>
    where
        C::Context: 'a;

    /// # Safety
    /// Registered storage and kernel identities remain valid for this driver's flights.
    unsafe fn wait<'a>(
        self,
        backend: &'a mut Self::Backend,
        span: Option<time::Duration>,
        busy: bool,
    ) -> Result<(), <Self::Backend as Backend<C>>::Failure>
    where
        C::Context: 'a;
}
