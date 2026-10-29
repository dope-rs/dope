use std::{marker, mem};

use o3::{base::marker::ThreadBound, mem::quota};
use state::lanes;

use crate::driver::schedule::state;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Work<'turn, 'd> {
    pub(in crate::driver::schedule) state: &'turn state::Core<'d>,
}

#[must_use = "a maintenance permit represents one admitted transition"]
pub struct Permit<'turn, 'd> {
    proof: marker::PhantomData<(&'turn mut (), crate::driver::Invariant<'d>, ThreadBound)>,
}

impl<'turn, 'd> Work<'turn, 'd> {
    pub fn remaining(self) -> usize {
        self.state.lanes.maint.remaining()
    }

    pub fn run<T>(self, run: impl FnOnce() -> Option<T>) -> quota::Admission<T> {
        self.state.lanes.maint.admit_with(run)
    }

    pub fn apply(self, run: impl FnOnce() -> quota::Acquire) {
        self.state.lanes.maint.apply(run);
    }

    pub fn permit(self) -> Option<Permit<'turn, 'd>> {
        match self.state.lanes.maint.take() {
            quota::Take::Taken => Some(Permit::new()),
            quota::Take::Exhausted => None,
        }
    }

    pub fn claim<T>(
        self,
        claim: impl FnOnce() -> Option<T>,
    ) -> quota::Admission<(T, Permit<'turn, 'd>)> {
        self.run(|| claim().map(|value| (value, Permit::new())))
    }

    pub fn share<const N: usize, T>(self, run: impl FnOnce() -> T) -> T {
        let _held = lanes::Quota::share::<N, _>(&self.state.lanes.maint, 0);
        run()
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

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Work<'static, 'static>>() == word);
    assert!(mem::size_of::<Permit<'static, 'static>>() == 0);
};
