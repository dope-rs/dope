//! Provided-buffer exhaustion contracts.

use std::{marker, task::Poll};

pub use attempt::Attempt;
use dope_sys::{
    engine,
    memory::buffer,
    table::{permit, permit::Queue},
};
use o3::base::num::bounded;

use crate::{
    Backend, Engine, driver,
    driver::{schedule::ready, settings},
};

pub(crate) mod access;
mod attempt;
pub mod credit;
mod raw;
pub(crate) mod tokens;

/// Receive-message frame capacity retained by a failed driver probe.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub struct Capacity(bounded::U64<0, { u64::MAX }>);

/// Exact receive-message frame length retained by a failed driver probe.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub struct Length(bounded::U64<0, { u64::MAX }>);

impl From<bounded::U64<0, { u64::MAX }>> for Capacity {
    fn from(value: bounded::U64<0, { u64::MAX }>) -> Self {
        Self(value)
    }
}

impl From<bounded::U64<0, { u64::MAX }>> for Length {
    fn from(value: bounded::U64<0, { u64::MAX }>) -> Self {
        Self(value)
    }
}

/// Exact out-of-range provided-buffer selection observed from a backend.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub struct Selection(<Backend as buffer::Selection<settings::Buffers>>::Error);

impl Selection {
    pub(crate) fn from_native<E>(value: E) -> Self
    where
        Backend: buffer::Selection<settings::Buffers, Error = E>,
    {
        Self(value)
    }
}

/// Exact ready key owned by provided-buffer receive authority.
#[repr(transparent)]
pub(crate) struct Proof(ready::Stored);

impl Proof {
    pub(in crate::driver) const fn new(key: ready::Stored) -> Self {
        Self(key)
    }

    pub(in crate::driver) const fn key(&self) -> ready::Stored {
        self.0
    }

    pub(crate) fn release(
        permit: &<crate::Permits as Queue>::Capability,
        store: &crate::Permits,
        driver: driver::Reference<'_>,
    ) {
        while let Some((index, key)) = store.grant_one(permit) {
            let stale = match driver.ready().arena().stored().try_receive(Self::new(key)) {
                Ok(()) => continue,
                Err(stale) => stale.key(),
            };
            match store.take(permit, index, stale) {
                permit::State::Granted(returned) => {
                    if returned != stale {
                        return;
                    }
                }
                permit::State::Gone | permit::State::Waiting | permit::State::Stale => return,
            }
        }
    }
}

/// Driver-branded proof of one terminal provided-buffer exhaustion.
/// Retains the driver's invariant lifetime and thread confinement without storage.
#[repr(transparent)]
pub struct Exhausted<'d> {
    driver: marker::PhantomData<driver::Reference<'d>>,
}

/// Exact ready-target registration for provided-buffer exhaustion.
#[must_use = "a receive registration must be polled, cancelled, or retired"]
#[repr(transparent)]
pub struct Registration<'d> {
    target: ready::Target<'d>,
    permit: <crate::Permits as Queue>::Capability,
}

/// A registration no longer names the live receive target that created it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stale;

/// The selected backend does not provide receive registrations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Absent;

impl<'d> Exhausted<'d> {
    pub(crate) const fn from_driver(_driver: driver::Reference<'d>) -> Self {
        Self {
            driver: marker::PhantomData,
        }
    }

    /// Consumes terminal exhaustion for this generative driver scope.
    pub(in crate::driver) fn retire(self) -> dope_sys::Errno {
        dope_sys::Errno::NO_BUFFER_SPACE
    }
}

impl<'d, P> Registration<'d>
where
    crate::Permits: Queue<Capability = P>,
{
    /// Acquires provided-buffer receive authority for this target.
    pub fn try_acquire(target: ready::Target<'d>) -> Result<Self, Absent> {
        let permit = crate::Permits::try_acquire().map_err(|permit::Absent| Absent)?;
        Ok(Self { target, permit })
    }

    /// Retires a registration when the selected backend provides one.
    pub fn clear(target: ready::Target<'d>) {
        let reg = match Self::try_acquire(target) {
            Ok(reg) => reg,
            Err(Absent) => return,
        };
        reg.retire();
    }

    /// Registers exhaustion before permitting one optimistic retry.
    pub fn exhausted(
        self,
        proof: Exhausted<'d>,
    ) -> Result<Poll<Attempt<'d>>, (Exhausted<'d>, Stale)> {
        let Self { target, permit } = self;
        let (driver, key) = target.receive();
        if !Self::is_live(driver, key) {
            return Err((proof, Stale));
        }
        let store = driver.receive().exhausted();
        match store.try_arm(&permit, Index::from_key(key), key) {
            Ok(()) => Ok(Poll::Ready(Attempt::new(permit, driver))),
            Err(key) => Self::take(permit, store, driver, key).map_err(|stale| (proof, stale)),
        }
    }

    /// Observes the exact central registration after its target was woken.
    /// Stale targets discard any leftover grant before reporting staleness.
    pub fn resume(self) -> Result<Poll<Attempt<'d>>, Stale> {
        let Self { target, permit } = self;
        let (driver, key) = target.receive();
        let store = driver.receive().exhausted();
        if !Self::is_live(driver, key) {
            return match Self::cancel_inner(&permit, store, key) {
                Ok(()) | Err(Stale) => Err(Stale),
            };
        }
        Self::take(permit, store, driver, key)
    }

    /// Cancels this exact registration and discards an unconsumed grant.
    pub fn cancel(self) -> Result<(), Stale> {
        let Self { target, permit } = self;
        let (driver, key) = target.receive();
        Self::cancel_inner(&permit, driver.receive().exhausted(), key)
    }

    /// Retires this registration, discarding a matching unconsumed grant.
    /// A stale registration has no matching grant to retire.
    pub fn retire(self) {
        match self.cancel() {
            Ok(()) | Err(Stale) => {}
        }
    }

    fn is_live(driver: driver::Reference<'_>, key: ready::Stored) -> bool {
        driver.ready().arena().stored().is_dispatch(Proof::new(key))
    }

    fn take(
        permit: P,
        store: &'d crate::Permits,
        driver: driver::Reference<'d>,
        key: ready::Stored,
    ) -> Result<Poll<Attempt<'d>>, Stale> {
        match store.take(&permit, Index::from_key(key), key) {
            permit::State::Waiting => Ok(Poll::Pending),
            permit::State::Granted(_) => Ok(Poll::Ready(Attempt::new(permit, driver))),
            permit::State::Gone | permit::State::Stale => Err(Stale),
        }
    }

    fn cancel_inner(permit: &P, store: &crate::Permits, key: ready::Stored) -> Result<(), Stale> {
        match store.cancel(permit, Index::from_key(key), key) {
            permit::State::Gone | permit::State::Waiting | permit::State::Granted(_) => Ok(()),
            permit::State::Stale => Err(Stale),
        }
    }
}

const _: () = {
    assert!(
        std::mem::size_of::<Selection>()
            == std::mem::size_of::<<Engine as engine::Buffer>::Selection>()
    );
    assert!(
        std::mem::align_of::<Selection>()
            == std::mem::align_of::<<Engine as engine::Buffer>::Selection>()
    );
    assert!(std::mem::size_of::<Proof>() == std::mem::size_of::<ready::Stored>());
    assert!(std::mem::align_of::<Proof>() == std::mem::align_of::<ready::Stored>());
    assert!(std::mem::size_of::<Exhausted<'static>>() == 0);
    assert!(std::mem::align_of::<Exhausted<'static>>() == 1);
    assert!(std::mem::size_of::<<crate::Permits as Queue>::Capability>() == 0);
    assert!(std::mem::size_of::<permit::Absent>() == 0);
    assert!(std::mem::size_of::<Absent>() == 0);
    assert!(std::mem::size_of::<Registration<'static>>() == 2 * std::mem::size_of::<usize>());
    assert!(
        std::mem::size_of::<Registration<'static>>()
            == std::mem::size_of::<ready::Target<'static>>()
    );
    assert!(
        std::mem::align_of::<Registration<'static>>()
            == std::mem::align_of::<ready::Target<'static>>()
    );
};

/// Compact receive-permit position retaining its ready-arena domain.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Index(ready::DynamicIndex);

impl Index {
    const fn from_key(key: ready::Stored) -> Self {
        Self(key.dense())
    }
}
