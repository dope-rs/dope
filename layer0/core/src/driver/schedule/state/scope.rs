use std::{marker, mem};

use o3::mem::quota;
use state::lanes;

use crate::driver::{
    schedule,
    schedule::{clock, ready, state, timers},
};

pub(in crate::driver) const REACTORS: usize = 2;

pub struct Scope<'scope, 'd> {
    state: &'scope mut state::Core<'d>,
}

#[must_use = "the active turn must remain live for the complete driver cycle"]
pub struct Guard<'turn, 'd> {
    state: &'turn mut state::Core<'d>,
}

#[must_use = "reactor authority must be consumed by Poll or Source"]
pub struct Reactor<'turn, 'd> {
    pub(in crate::driver::schedule) quota: lanes::Quota<'turn, 'd>,
    pub(in crate::driver::schedule) cursor: state::Cursor,
    turn: marker::PhantomData<&'turn mut ()>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

pub struct Coord<'turn, 'd> {
    state: &'turn state::Core<'d>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'scope, 'd> Scope<'scope, 'd> {
    pub(in crate::driver) fn new(state: &'scope mut state::Core<'d>) -> Self {
        Self { state }
    }

    pub fn begin_at(&mut self, sample: clock::Sample) -> Guard<'_, 'd> {
        self.sample(sample);
        self.begin()
    }

    /// Installs the clock sample for work that opens no driver cycle.
    pub fn sample(&mut self, sample: clock::Sample) {
        self.state
            .driver
            .scheduler()
            .set_turn_now(sample.into_raw());
    }

    pub fn begin(&mut self) -> Guard<'_, 'd> {
        self.state.reset();
        Guard {
            state: &mut *self.state,
        }
    }
}

impl<'turn, 'd> Guard<'turn, 'd> {
    pub fn turn(&self) -> schedule::Turn<'_, 'd> {
        schedule::Turn::new(&*self.state)
    }

    pub fn reactor(&mut self) -> Reactor<'_, 'd> {
        self.split().0
    }

    pub fn split(&mut self) -> (Reactor<'_, 'd>, schedule::Turn<'_, 'd>) {
        let state = &*self.state;
        let cursor = state.reactor.cursor.get();
        state.reactor.cursor.set(cursor.next());
        (
            Reactor {
                quota: lanes::Quota::reactor(&state.reactor.ledger),
                cursor,
                turn: marker::PhantomData,
                driver: marker::PhantomData,
            },
            schedule::Turn::new(state),
        )
    }

    /// Runs every timer that has expired at the current turn sample.
    pub fn expire(&mut self) {
        let state = &*self.state;
        timers::expire(state.driver, &state.lanes.timers);
    }

    pub fn is_exhausted(&self) -> bool {
        self.state.is_exhausted()
    }

    pub fn ready(&self) -> ready::Rounds<'_, 'd> {
        let state = &*self.state;
        ready::Rounds::new(state.driver, lanes::Budget::reserve_all(&state.lanes.ready))
    }
}

impl<'turn, 'd> Reactor<'turn, 'd> {
    pub(crate) fn refund(self) {
        self.quota.inner.refund();
    }

    pub(crate) fn remaining(&self) -> usize {
        self.quota.remaining()
    }

    pub(crate) const fn cursor(&self) -> usize {
        self.cursor.index()
    }

    pub(crate) fn budget<'budget, Lane>(
        &'budget self,
        limit: usize,
    ) -> lanes::Budget<'budget, 'd, Lane> {
        self.quota.lease(limit)
    }
}

impl<'turn, 'd> Coord<'turn, 'd> {
    pub(in crate::driver::schedule) fn new(state: &'turn state::Core<'d>) -> Self {
        Self {
            state,
            driver: marker::PhantomData,
        }
    }

    pub fn take(&mut self) -> quota::Take {
        match self.state.lanes.coord.take() {
            quota::Take::Taken => quota::Take::Taken,
            quota::Take::Exhausted => {
                self.state.deferred.set(true);
                quota::Take::Exhausted
            }
        }
    }

    pub fn remaining(&self) -> usize {
        self.state.lanes.coord.remaining()
    }

    pub fn share<const N: usize, T>(
        &self,
        rank: usize,
        weights: [usize; 2],
        run: impl FnOnce() -> T,
    ) -> T {
        let _held = lanes::Quota::weighted::<N, _>(&self.state.lanes.coord, rank, weights);
        run()
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Scope<'static, 'static>>() == word);
    assert!(mem::size_of::<Guard<'static, 'static>>() == word);
};
