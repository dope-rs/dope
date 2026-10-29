use std::{cell, marker, mem, time};

use o3::{alloc, base::cell::region, mem::quota};
use state::{lanes::Quota, scope};

use crate::{
    driver,
    driver::{
        receive::{credit, credit::storage},
        route::table::conn,
        settings,
    },
};

pub mod clock;

pub mod app;
pub mod maint;
pub mod ready;
pub mod sequence;
#[doc(hidden)]
pub mod state;
mod timers;

pub const LIMIT: usize = 256;

/// Exact result of acquiring scheduler capacity from a non-empty source.
#[must_use = "capacity reports whether the scheduler granted the operation"]
pub enum Capacity<T> {
    Available(T),
    Exhausted,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Turn<'turn, 'd> {
    state: &'turn state::Core<'d>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'turn, 'd> Turn<'turn, 'd> {
    pub(in crate::driver::schedule) const fn new(state: &'turn state::Core<'d>) -> Self {
        Self {
            state,
            driver: marker::PhantomData,
        }
    }

    pub fn reborrow(&self) -> Turn<'_, 'd> {
        Turn::new(self.state)
    }

    pub fn clock(self) -> clock::Clock<'turn, 'd> {
        self.state.driver.scheduler().clock()
    }

    pub fn app(self) -> app::Work<'turn, 'd> {
        app::Work { state: self.state }
    }

    pub fn maint(self) -> maint::Work<'turn, 'd> {
        maint::Work { state: self.state }
    }

    /// Runs one sibling with refundable application, maintenance and control
    /// shares. Ranks choose the remainder recipients when fewer credits than
    /// siblings remain; sufficiently funded shares keep ceiling division.
    #[doc(hidden)]
    pub fn share<const N: usize, T>(
        self,
        ranks: [usize; 3],
        weights: [usize; 2],
        run: impl for<'scope> FnOnce(Turn<'scope, 'd>) -> T,
    ) -> T {
        let _app = Quota::share::<N, _>(&self.state.lanes.app, ranks[0]);
        let _maint = Quota::share::<N, _>(&self.state.lanes.maint, ranks[1]);
        let _coord = Quota::weighted::<N, _>(&self.state.lanes.coord, ranks[2], weights);
        run(Turn::new(self.state))
    }

    #[doc(hidden)]
    pub fn coord(self) -> scope::Coord<'turn, 'd> {
        scope::Coord::new(self.state)
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Turn<'static, 'static>>() == word);
    assert!(mem::size_of::<quota::Admission<usize>>() == 2 * word);
    assert!(mem::size_of::<Capacity<&'static u8>>() == word);
};

/// Aggregate liveness of retained work owned by one driver.
#[must_use]
pub enum Progress<'d> {
    Runnable,
    Waiting(Wait<'d>),
    Quiescent,
}

impl<'d> Progress<'d> {
    pub fn waiting(region: &region::Token<region::Identity<'d>>) -> Self {
        Self::Waiting(Wait::event(region))
    }

    pub fn until(deadline: clock::Deadline<'d>) -> Self {
        Self::Waiting(Wait::until(deadline))
    }

    pub fn reduce(self, other: Self) -> Self {
        match self {
            runnable @ Self::Runnable => runnable,
            Self::Waiting(left) => match other {
                runnable @ Self::Runnable => runnable,
                Self::Waiting(right) => Self::Waiting(left.reduce(right)),
                Self::Quiescent => Self::Waiting(left),
            },
            Self::Quiescent => other,
        }
    }
}

const _: () = assert!(mem::size_of::<Progress<'static>>() <= 24);

pub(in crate::driver) struct State {
    pub(in crate::driver) owner: storage::Owner<ready::Stored, credit::Wake, ready::Arena>,
    clock: cell::Cell<time::Instant>,
}

impl State {
    pub(in crate::driver) fn try_new(plan: settings::Plan) -> Result<Self, alloc::Error> {
        Ok(Self {
            owner: storage::Owner::try_new(plan)?,
            clock: cell::Cell::new(time::Instant::now()),
        })
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Context<'d>(driver::Reference<'d>);

impl<'d> Context<'d> {
    pub(in crate::driver) const fn new(driver: driver::Reference<'d>) -> Self {
        Self(driver)
    }

    pub(in crate::driver) fn clock(self) -> clock::Clock<'d, 'd> {
        clock::Clock::new(&self.0.shared.context().schedule.clock, self.0.brand)
    }

    pub(in crate::driver) fn set_turn_now(self, now: time::Instant) {
        self.0.shared.context().schedule.clock.set(now);
    }

    /// Allocates fixed retained-credit storage in this driver generation.
    pub fn try_storage(
        self,
        capacity: conn::Capacity,
    ) -> Result<credit::Storage<'d>, alloc::Error> {
        let shared: &'d driver::Shared = self.0.shared.context();
        let owner = &shared.schedule.owner;
        owner.try_storage(capacity, self.0.brand)
    }
}

const _: () = assert!(
    std::mem::size_of::<Context<'static>>() == std::mem::size_of::<driver::Reference<'static>>()
);

enum Reason<'d> {
    Event(region::Proof<region::Identity<'d>>),
    Until(clock::Deadline<'d>),
}

/// Driver-scoped evidence that a retained owner is waiting for an external
/// wake or a monotonic deadline.
#[must_use]
#[repr(transparent)]
pub struct Wait<'d> {
    state: Reason<'d>,
}

impl<'d> Wait<'d> {
    pub fn event(region: &region::Token<region::Identity<'d>>) -> Self {
        Self {
            state: Reason::Event(region.proof()),
        }
    }

    pub fn until(deadline: clock::Deadline<'d>) -> Self {
        Self {
            state: Reason::Until(deadline),
        }
    }

    pub fn deadline(self) -> Option<clock::Deadline<'d>> {
        match self.state {
            Reason::Event(_proof) => None,
            Reason::Until(deadline) => Some(deadline),
        }
    }

    pub(super) fn reduce(self, other: Self) -> Self {
        match self.state {
            Reason::Event(_proof) => other,
            Reason::Until(left) => match other.state {
                Reason::Event(_proof) => Self {
                    state: Reason::Until(left),
                },
                Reason::Until(right) => Self {
                    state: Reason::Until(left.min(right)),
                },
            },
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Wait<'static>>() == mem::size_of::<Option<clock::Deadline<'static>>>());
    assert!(
        mem::align_of::<Wait<'static>>() == mem::align_of::<Option<clock::Deadline<'static>>>()
    );
    assert!(mem::size_of::<Option<Wait<'static>>>() == mem::size_of::<Wait<'static>>());
    assert!(!mem::needs_drop::<Wait<'static>>());
};
