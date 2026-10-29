use std::{mem, num};

use o3::depot::queue::resident::{
    self, fifo,
    fifo::{Index, Scope},
};

use crate::{
    driver,
    driver::schedule::{
        app,
        ready::{
            self, wake,
            wake::{
                Drain, Once, Waker,
                fanout::storage::Step,
                resident::{drain, one},
            },
        },
    },
};

/// Bounded FIFO wakes with a constructor-proved lane extent.
pub struct Queue<'d, L = num::NonZero<usize>> {
    driver: driver::Reference<'d>,
    inner: fifo::Fifo<ready::Retained<'d>, L>,
}

/// Linear ownership of one resident queue slot.
#[repr(transparent)]
pub struct Registration<'target, 'd: 'target, L = num::NonZero<usize>> {
    pub(super) inner: fifo::Registration<'target, ready::Retained<'d>, L>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "a rejected wake registration must be handled"]
pub enum Error {
    Lane(resident::Bounds<num::NonZero<u32>>),
    Active { position: wake::Position },
    Draining { position: wake::Position },
    Full { position: wake::Position },
}

impl From<fifo::Reject> for Error {
    fn from(error: fifo::Reject) -> Self {
        match error {
            fifo::Reject::Active { index } => Self::Active {
                position: wake::Position::new(index),
            },
            fifo::Reject::Draining { index } => Self::Draining {
                position: wake::Position::new(index),
            },
            fifo::Reject::Full { index } => Self::Full {
                position: wake::Position::new(index),
            },
        }
    }
}

impl<'d, L> Queue<'d, L> {
    pub fn try_new(
        driver: driver::Reference<'d>,
        capacity: usize,
        lanes: L,
    ) -> Result<Self, resident::Error>
    where
        L: resident::Extent,
    {
        Ok(Self {
            driver,
            inner: fifo::Fifo::try_new(capacity, lanes)?,
        })
    }
}

impl<'d> Queue<'d> {
    /// Creates one queue and borrows its source under a fresh invariant scope.
    pub fn try_with<S, R>(
        driver: driver::Reference<'d>,
        capacity: usize,
        lanes: num::NonZero<usize>,
        source: S,
        run: impl for<'scope> FnOnce(&'scope S, Queue<'d, Scope<'scope>>) -> R,
    ) -> Result<R, resident::Error> {
        fifo::Fifo::try_with(capacity, lanes, source, |source, inner| {
            run(source, Queue { driver, inner })
        })
    }

    pub fn try_register<'target>(
        &'target self,
        position: usize,
        slot: &mut Registration<'target, 'd>,
        wake: &Waker<'d>,
    ) -> Result<(), Error> {
        let inner = self.inner.try_lane(position).map_err(Error::Lane)?;
        inner
            .try_keep(&mut slot.inner, *wake.key())
            .map_err(Error::from)
    }

    pub fn try_wake_one(
        &self,
        position: usize,
    ) -> Result<Once, resident::Bounds<num::NonZero<u32>>> {
        let lane = self.inner.try_lane(position)?;
        Ok(one(self.driver, lane.take_one()))
    }
}

impl<'scope, 'd> Queue<'d, Scope<'scope>> {
    pub fn try_index(
        &self,
        position: usize,
    ) -> Result<Index<'scope>, resident::Bounds<num::NonZero<u32>>> {
        self.inner.try_index(position)
    }

    pub fn try_register<'target>(
        &'target self,
        index: Index<'scope>,
        slot: &mut Registration<'target, 'd, Scope<'scope>>,
        wake: &Waker<'d>,
    ) -> Result<(), fifo::Reject> {
        self.inner
            .lane(index)
            .try_keep(&mut slot.inner, *wake.key())
    }

    pub fn wake_one(&self, index: Index<'scope>) -> Once {
        one(self.driver, self.inner.lane(index).take_one())
    }
}

impl<'d, const N: usize> Queue<'d, resident::Fixed<N>> {
    pub fn try_register<'target, const I: usize>(
        &'target self,
        slot: &mut Registration<'target, 'd, resident::Fixed<N>>,
        wake: &Waker<'d>,
    ) -> Result<(), fifo::Reject> {
        self.inner
            .lane::<I>()
            .try_keep(&mut slot.inner, *wake.key())
    }

    pub fn wake_one<const I: usize>(&self) -> Once {
        one(self.driver, self.inner.lane::<I>().take_one())
    }
}

impl<'d, L> Queue<'d, L> {
    /// Registers one wake in the constructor-proved first lane.
    pub fn try_first<'target>(
        &'target self,
        slot: &mut Registration<'target, 'd, L>,
        wake: &Waker<'d>,
    ) -> Result<(), fifo::Reject> {
        self.inner.first().try_keep(&mut slot.inner, *wake.key())
    }

    /// Wakes one registration from lane zero.
    pub fn wake_first(&self) -> Once {
        one(self.driver, self.inner.first().take_one())
    }

    pub fn wake_next(&self) -> Once {
        let Some(target) = self.inner.take_next() else {
            return Once::Empty;
        };
        target.wake(self.driver);
        Once::Woke
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
        drain::one(
            self.driver,
            work,
            || self.inner.is_draining() && self.inner.has_waiters(),
            || match self.inner.drain_next() {
                resident::Drain::Done => None,
                resident::Drain::Item(target) => Some(Step::Item(target)),
            },
        )
    }
}

impl<L> Default for Registration<'_, '_, L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L> Registration<'_, '_, L> {
    pub const fn new() -> Self {
        Self {
            inner: fifo::Registration::new(),
        }
    }

    pub fn is_active(&self) -> bool {
        self.inner.is_active()
    }

    pub fn clear(&mut self) {
        *self = Self::new();
    }
}

const _: () = {
    assert!(mem::size_of::<Registration<'static, 'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Registration<'static, 'static>>() == mem::align_of::<usize>());
    assert!(
        size_of::<Registration<'static, 'static, resident::Fixed<1>>>() == 2 * size_of::<usize>()
    );
    assert!(size_of::<Queue<'static, resident::Fixed<1>>>() == size_of::<Queue<'static>>());
    assert!(size_of::<Queue<'static, Scope<'static>>>() == size_of::<Queue<'static>>());
    assert!(size_of::<Index<'static>>() == size_of::<u32>());
    assert!(size_of::<Registration<'static, 'static, Scope<'static>>>() == 2 * size_of::<usize>());
};
