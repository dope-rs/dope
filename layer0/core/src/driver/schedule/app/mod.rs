use std::{marker, mem::size_of};

use mem::{lease, quota};
use o3::{base::marker::ThreadBound, mem};
use state::lanes;

use crate::driver::{
    schedule,
    schedule::{clock, state},
};

pub mod batch;
pub mod step;

pub type Selection<I, P, C> = Option<Result<(I, P), C>>;

/// A stable batch whose front is consumed only after application admission.
/// A selected item carries the exact receipt returned by `take`; implementations
/// cannot create an admission independently of that callback.
pub trait Drain {
    type Candidate<'a>
    where
        Self: 'a;
    type Item<'a>
    where
        Self: 'a;

    fn next_when<P>(
        &mut self,
        take: impl FnOnce() -> Option<P>,
    ) -> Selection<Self::Item<'_>, P, Self::Candidate<'_>>;
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Work<'turn, 'd> {
    pub(in crate::driver::schedule) state: &'turn state::Core<'d>,
}

#[must_use = "an application permit represents one admitted transition"]
pub struct Permit<'turn, 'd> {
    proof: marker::PhantomData<(&'turn mut (), crate::driver::Invariant<'d>, ThreadBound)>,
}

#[must_use = "dropping an uncommitted application lease returns its turn"]
pub struct Lease<'turn, 'd> {
    inner: mem::Lease<quota::Return<'turn, state::AppLane>, lease::Open>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

#[must_use = "next reports whether work was ready or the application lane was exhausted"]
pub enum Next<'turn, 'd, T, C = T> {
    Item(T, Permit<'turn, 'd>),
    Empty,
    Exhausted(C),
}

#[must_use = "an application share scopes one participant's scheduler turn"]
#[repr(transparent)]
pub struct Share<'turn, 'd> {
    turn: schedule::Turn<'turn, 'd>,
}

impl<'turn, 'd> Work<'turn, 'd> {
    pub fn remaining(self) -> usize {
        self.state.lanes.app.remaining()
    }

    pub fn clock(self) -> clock::Clock<'turn, 'd> {
        self.state.driver.scheduler().clock()
    }

    pub fn run<T>(self, run: impl FnOnce() -> Option<T>) -> quota::Admission<T> {
        self.state.lanes.app.admit_with(run)
    }

    pub fn apply(self, run: impl FnOnce() -> quota::Acquire) {
        self.state.lanes.app.apply(run);
    }

    pub fn permit(self) -> Option<Permit<'turn, 'd>> {
        match self.state.lanes.app.take() {
            quota::Take::Taken => Some(Permit::new()),
            quota::Take::Exhausted => None,
        }
    }

    pub fn lease(self) -> Option<Lease<'turn, 'd>> {
        Some(Lease {
            inner: self.state.lanes.app.open()?,
            driver: marker::PhantomData,
        })
    }

    pub fn claim<T>(
        self,
        claim: impl FnOnce() -> Option<T>,
    ) -> quota::Admission<(T, Permit<'turn, 'd>)> {
        self.run(|| claim().map(|value| (value, Permit::new())))
    }

    pub fn next<'a, R: Drain>(
        self,
        ready: &'a mut R,
    ) -> Next<'turn, 'd, R::Item<'a>, R::Candidate<'a>> {
        match Drain::next_when(ready, || self.permit()) {
            None => Next::Empty,
            Some(Ok((value, permit))) => Next::Item(value, permit),
            Some(Err(value)) => Next::Exhausted(value),
        }
    }

    pub fn share<const N: usize, T>(
        self,
        run: impl for<'scope> FnOnce(Share<'scope, 'd>) -> T,
    ) -> T {
        let _reserve = lanes::Quota::share::<N, _>(&self.state.lanes.app, 0);
        run(Share {
            turn: schedule::Turn::new(self.state),
        })
    }
}

impl<'turn, 'd> Share<'turn, 'd> {
    pub fn turn(&self) -> schedule::Turn<'turn, 'd> {
        self.turn
    }

    pub fn work(&self) -> Work<'turn, 'd> {
        self.turn.app()
    }
}

impl<'turn, 'd> Permit<'turn, 'd> {
    fn new() -> Self {
        Self {
            proof: marker::PhantomData,
        }
    }

    pub fn run<T>(self, run: impl FnOnce() -> T) -> T {
        run()
    }
}

impl<'turn, 'd> Lease<'turn, 'd> {
    pub fn commit(self) -> Permit<'turn, 'd> {
        let Self {
            inner,
            driver: marker::PhantomData,
        } = self;
        let () = inner.into_parts().1;
        Permit::new()
    }
}

const _: () = {
    let word = size_of::<usize>();
    assert!(size_of::<Work<'static, 'static>>() == word);
    assert!(size_of::<Permit<'static, 'static>>() == 0);
    assert!(size_of::<Lease<'static, 'static>>() == word);
    assert!(size_of::<Next<'static, 'static, usize>>() == 2 * word);
    assert!(size_of::<Share<'static, 'static>>() == word);
};
