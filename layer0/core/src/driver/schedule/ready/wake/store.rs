use std::{cell, marker, mem};

use o3::base::marker::ThreadBound;

use crate::{
    driver,
    driver::schedule::{
        ready,
        ready::wake::{Waker, resident::slot},
    },
};

/// One standalone wake registration that carries its driver.
#[repr(transparent)]
pub struct Slot<'d> {
    wake: cell::Cell<Option<ready::Address<'d>>>,
}

/// Driver ownership shared by a resident collection of wake entries.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Store<'d> {
    driver: driver::Reference<'d>,
}

/// One wake target bound to its collection's exact driver.
#[repr(transparent)]
pub struct Entry<'d> {
    raw: slot::Slot,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
    thread: ThreadBound,
}

impl<'d> Store<'d> {
    pub const fn new(driver: driver::Reference<'d>) -> Self {
        Self { driver }
    }

    pub fn set(&self, entry: &Entry<'d>, wake: Waker<'d>) {
        entry.raw.set(*wake.key());
    }

    pub fn wake_once(&self, entry: &Entry<'d>) {
        entry.raw.wake_once(self.driver);
    }

    pub fn wake(&self, entry: &Entry<'d>) {
        entry.raw.wake(self.driver);
    }
}

impl<'d> Slot<'d> {
    pub const fn empty() -> Self {
        Self {
            wake: cell::Cell::new(None),
        }
    }

    pub fn set(&self, wake: Waker<'d>) {
        self.wake.set(Some(wake.0));
    }

    pub fn clear(&self) {
        self.wake.set(None);
    }

    /// Takes and wakes the target after publishing vacancy, so a repoll may
    /// install its next wake without this call clearing that new registration.
    pub fn wake_once(&self) {
        let Some(wake) = self.wake.take() else {
            return;
        };
        Waker(wake).wake();
    }
}

impl Entry<'_> {
    pub fn empty() -> Self {
        Self {
            raw: slot::Slot::default(),
            driver: marker::PhantomData,
            thread: ThreadBound::NEW,
        }
    }

    pub fn clear(&self) {
        self.raw.clear();
    }
}

const _: () = {
    assert!(mem::size_of::<Slot<'static>>() == mem::size_of::<Waker<'static>>());
    assert!(mem::size_of::<Store<'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Entry<'static>>() == mem::size_of::<u64>());
};
