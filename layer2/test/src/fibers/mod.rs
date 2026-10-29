use std::time::Duration;

use dope_fiber::{BlockOn, abi::Fiber, task::time::sleep};
use dope_runtime::executor::drive;

use crate::harness::{GUARD, signals::Progress};

mod delay;
mod once;
mod until;
mod within;
mod r#yield;
use delay::Delay;
use once::Once;
use until::Until;
pub use within::Within;
pub use r#yield::Yield;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Wait {
    Reached,
    Deadline,
}

#[derive(Debug)]
pub enum Run {
    Drive(drive::Error),
    Timer(sleep::Error),
    Deadline,
}

pub struct Harness {
    guard: Duration,
}
pub const TEST: Harness = Harness::new(GUARD);

impl Harness {
    pub const fn new(guard: Duration) -> Self {
        Self { guard }
    }

    pub fn within<'d, F: Fiber<'d>>(&self, fiber: F) -> Within<'d, F> {
        Within::new(fiber, self.guard)
    }

    pub fn poll_once<'d, F: Fiber<'d>>(
        &self,
        fiber: F,
    ) -> impl Fiber<'d, Output = Option<F::Output>> {
        Once::new(fiber)
    }

    pub fn try_run<'d, A: BlockOn<'d>, F: Fiber<'d>>(
        &self,
        app: &mut A,
        fiber: F,
    ) -> Result<F::Output, Run> {
        app.block_on(self.within(fiber))
            .map_err(Run::Drive)?
            .map_err(Run::Timer)?
            .ok_or(Run::Deadline)
    }

    pub fn try_wait<'d, A: BlockOn<'d>, G: Progress + ?Sized>(
        &self,
        app: &mut A,
        gate: &G,
        want: u32,
    ) -> Result<Wait, Run> {
        app.block_on(Until::new(gate, want, self.guard))
            .map_err(Run::Drive)?
            .map_err(Run::Timer)
    }

    pub fn try_pause<'d, A: BlockOn<'d>>(
        &self,
        app: &mut A,
        duration: Duration,
    ) -> Result<(), Run> {
        app.block_on(Delay::new(duration))
            .map_err(Run::Drive)?
            .map_err(Run::Timer)
    }
}
