pub mod raw;
pub mod storage;

use std::{marker, mem};

use o3::{depot::queue::resident, mem::quota};

use crate::{
    driver,
    driver::schedule::{
        app,
        ready::{
            self, wake,
            wake::{Drain, Once, Waker, resident::one},
        },
    },
};

/// One wake per exact identity with an active-only FIFO.
pub struct Table<'d> {
    driver: driver::Reference<'d>,
    inner: storage::Registry<ready::Retained<'d>>,
}

/// A fresh invariant identity shared by one source and its completion sink.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Scope<'scope>(marker::PhantomData<fn(&'scope ()) -> &'scope ()>);

/// The original table retained throughout one source operation.
pub struct Sink<'scope, 'a, 'd> {
    table: &'a Table<'d>,
    _scope: Scope<'scope>,
}

/// Linear ownership of one exact table slot.
#[repr(transparent)]
pub struct Registration<'target, 'd: 'target> {
    inner: storage::Registration<'target, ready::Retained<'d>>,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[must_use = "a rejected wake registration must be handled"]
pub enum Error {
    Slot(resident::Bounds<usize>),
    Active { position: wake::Position },
    Draining { position: wake::Position },
    Occupied { position: wake::Position },
}

impl<'d> Table<'d> {
    /// Runs a source whose construction established the table's exact extent.
    pub fn with_source<'a, 'target, S: raw::Source<'a, 'target, 'd>, R>(
        source: S,
        run: impl for<'scope> FnOnce(&mut S::State, Scope<'scope>, Sink<'scope, 'target, 'd>) -> R,
    ) -> R
    where
        'd: 'target,
        'target: 'a,
    {
        let (state, table) = source.parts();
        let scope = Scope(marker::PhantomData);
        run(
            state,
            scope,
            Sink {
                table,
                _scope: scope,
            },
        )
    }

    pub fn try_new(
        driver: driver::Reference<'d>,
        capacity: usize,
    ) -> Result<Self, resident::Error> {
        Ok(Self {
            driver,
            inner: storage::Registry::try_new(capacity)?,
        })
    }

    pub fn try_register<'target>(
        &'target self,
        position: usize,
        slot: &mut Registration<'target, 'd>,
        wake: &Waker<'d>,
    ) -> Result<(), Error> {
        let inner = self.inner.try_slot(position).map_err(Error::Slot)?;
        slot.try_keep(inner, wake)
    }

    pub fn try_wake_one(&self, position: usize) -> Result<Once, resident::Bounds<usize>> {
        let slot = self.inner.try_slot(position)?;
        Ok(one(self.driver, slot.take_one()))
    }

    pub fn has_waiters(&self) -> bool {
        self.inner.has_waiters()
    }

    pub fn begin_drain(&self) {
        self.inner.begin_drain();
    }

    pub fn is_draining(&self) -> bool {
        self.inner.is_draining()
    }

    pub fn drain_one(&self, work: app::Work<'_, 'd>) -> Drain {
        match self.inner.drain_next(work) {
            quota::Admission::Empty => Drain::Empty,
            quota::Admission::Exhausted => Drain::Exhausted,
            quota::Admission::Item(target) => {
                target.wake(self.driver);
                Drain::Woke
            }
        }
    }
}

impl Default for Registration<'_, '_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'target, 'd: 'target> Registration<'target, 'd> {
    pub const fn new() -> Self {
        Self {
            inner: storage::Registration::new(),
        }
    }

    pub fn clear(&mut self) {
        *self = Self::new();
    }

    fn try_keep(
        &mut self,
        slot: storage::Slot<'target, ready::Retained<'d>>,
        wake: &Waker<'d>,
    ) -> Result<(), Error> {
        slot.try_keep(&mut self.inner, *wake.key())
            .map_err(|error| match error {
                storage::Reject::Active { index } => Error::Active {
                    position: wake::Position::new(index),
                },
                storage::Reject::Draining { index } => Error::Draining {
                    position: wake::Position::new(index),
                },
                storage::Reject::Occupied { index } => Error::Occupied {
                    position: wake::Position::new(index),
                },
            })
    }
}

const _: () = {
    assert!(mem::size_of::<Registration<'static, 'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Registration<'static, 'static>>() == mem::align_of::<usize>());
};
