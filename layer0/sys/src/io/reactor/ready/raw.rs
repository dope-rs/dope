use std::{ops::ControlFlow, time};

use crate::{
    Budget,
    io::reactor::ready::{Backend, Driver, delivery::Decoded, store::Exec},
};

pub trait Decode: Exec {
    /// # Safety
    /// The value was returned by this driver's registered kernel queue.
    unsafe fn decode(raw: usize) -> Decoded<Self::Key>;
    const SHUTDOWN: Self::Event;
}

pub trait Poll<C: Driver>: Backend<C> {
    /// A blocked receive consumes its admission and ends this pass without being removed.
    ///
    /// # Safety
    /// Registered operations retain their submitted storage and admitted layouts.
    unsafe fn resume<'a>(
        &'a mut self,
        budget: &mut impl Budget,
    ) -> Result<ControlFlow<()>, Self::Failure>
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
