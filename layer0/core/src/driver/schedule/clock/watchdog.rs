use std::mem;

use dope_sys::thread::{Thread, timer};
use o3::base::{marker::ThreadBound, time::Positive};

use crate::{
    Backend, driver,
    driver::{
        life::{finish, recovery},
        schedule::clock,
    },
};

/// Worker-local kernel deadline branded by its owning driver generation.
#[repr(transparent)]
pub struct Timer<'d>(clock::Bound<'d, <Backend as Thread>::Timer>);

impl<'d, T: dope_sys::Timer> Timer<'d>
where
    Backend: Thread<Timer = T>,
{
    pub fn try_new(deadline: Positive, owner: driver::Reference<'d>) -> Result<Self, timer::Error> {
        let raw = T::try_new(deadline)?;
        Ok(Self(clock::Bound::new(
            raw,
            (owner.brand, ThreadBound::NEW),
        )))
    }

    pub fn refresh(&self, context: driver::Context<'_, 'd>) {
        if let Err(cause) = self.0.inner.try_refresh() {
            recovery::Recovery::new(context, recovery::Cause::TimerRefresh(cause)).terminate();
        }
    }

    /// Disarms the timer after driver quiescence and returns the exact native error.
    /// Failure retains this timer; it may remain armed until dropped.
    pub fn try_disarm(&mut self, proof: &mut finish::Finish<'_, 'd>) -> Result<(), crate::Errno> {
        self.0.driver.0 = proof.brand();
        self.0.inner.try_disarm()
    }
}

const _: () = {
    assert!(mem::size_of::<Timer<'static>>() == mem::size_of::<<Backend as Thread>::Timer>());
    assert!(mem::align_of::<Timer<'static>>() == mem::align_of::<<Backend as Thread>::Timer>());
};
