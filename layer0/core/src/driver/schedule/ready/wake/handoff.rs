use std::mem;

use o3::depot::queue::resident::{
    self, handoff,
    handoff::{Cancel, Grant},
};

use crate::{
    driver,
    driver::schedule::{
        app,
        ready::{
            self, wake,
            wake::{
                fanout::storage::Step,
                resident::{drain, one},
            },
        },
    },
};

/// Move-safe ownership of one affine handoff slot.
#[repr(transparent)]
pub struct Registration<'target, 'd: 'target, G, L> {
    pub(super) inner: handoff::Registration<'target, ready::Retained<'d>, G, L>,
}

/// Bounded FIFO for one-at-a-time affine handoff.
pub struct Queue<'d, G, L> {
    driver: driver::Reference<'d>,
    inner: handoff::Registry<ready::Retained<'d>, G, L>,
}

#[derive(Eq, PartialEq)]
#[must_use = "a rejected affine grant remains caller-owned"]
pub enum Offer<G> {
    Woke,
    Draining(G),
    Empty(G),
}

impl<'d, G, L> Queue<'d, G, L> {
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
            inner: handoff::Registry::try_new(capacity, lanes)?,
        })
    }
}

impl<'d, G, L> Queue<'d, G, L> {
    pub fn begin_drain(&self) {
        self.inner.begin_drain();
    }

    pub fn drain_one(&self, work: app::Work<'_, 'd>) -> wake::Drain {
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

impl<'d, G, const N: usize> Queue<'d, G, resident::Fixed<N>> {
    pub fn try_register<'target, const I: usize>(
        &'target self,
        slot: &mut Registration<'target, 'd, G, resident::Fixed<N>>,
        wake: &wake::Waker<'d>,
    ) -> Result<(), handoff::Reject> {
        self.inner
            .lane::<I>()
            .try_keep(&mut slot.inner, *wake.key())
    }

    pub fn offer<const I: usize>(&self, grant: G) -> Offer<G> {
        match self.inner.lane::<I>().offer_one(grant) {
            handoff::Offer::Value(target) => {
                target.wake(self.driver);
                Offer::Woke
            }
            handoff::Offer::Draining(grant) => Offer::Draining(grant),
            handoff::Offer::Empty(grant) => Offer::Empty(grant),
        }
    }

    pub fn wake_one<const I: usize>(&self) -> wake::Once {
        one(self.driver, self.inner.lane::<I>().take_one())
    }

    pub fn is_empty<const I: usize>(&self) -> bool {
        self.inner.lane::<I>().is_empty()
    }
}

impl<'target, 'd: 'target, G, L> Default for Registration<'target, 'd, G, L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'target, 'd: 'target, G, L> Registration<'target, 'd, G, L> {
    pub const fn new() -> Self {
        Self {
            inner: handoff::Registration::new(),
        }
    }

    pub fn take_grant(&mut self) -> Grant<G> {
        self.inner.take_grant()
    }

    pub fn cancel(&mut self) -> Cancel<G> {
        self.inner.cancel()
    }
}

const _: () = {
    assert!(
        mem::size_of::<Registration<'static, 'static, u64, resident::Fixed<2>>>()
            == 2 * mem::size_of::<usize>()
    );
    assert!(
        mem::align_of::<Registration<'static, 'static, u64, resident::Fixed<2>>>()
            == mem::align_of::<usize>()
    );
    assert!(
        mem::size_of::<Queue<'static, u64, resident::Fixed<2>>>()
            == mem::size_of::<(
                driver::Reference<'static>,
                handoff::Registry<ready::Retained<'static>, u64, resident::Fixed<2>>
            )>()
    );
};
