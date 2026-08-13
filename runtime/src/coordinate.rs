use std::{marker, time};

use dope_core::driver::{self, schedule};
use o3::cell::region;

/// Whether a bounded application coordinator has another transition ready.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flow {
    Idle,
    More,
}

/// One linear, turn-bounded application coordination step.
/// It consumes one coordination credit and borrows the driver region only for
/// this step, preventing projected controls from escaping the callback.
#[must_use = "dropping a coordinate step consumes its admitted application work"]
pub struct Step<'step, 'turn, 'd> {
    region: &'step mut region::Token<'d>,
    now: time::Instant,
    _turn: marker::PhantomData<&'turn mut ()>,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'step, 'turn, 'd> Step<'step, 'turn, 'd> {
    #[doc(hidden)]
    pub fn try_new(
        budget: &mut schedule::Coordination<'turn, 'd>,
        driver: &'step mut driver::Context<'_, 'd>,
    ) -> Option<Self> {
        if !budget.take() {
            return None;
        }
        let now = driver.turn_now();
        let region = driver.region_token();
        Some(Self {
            region,
            now,
            _turn: marker::PhantomData,
            _driver: marker::PhantomData,
        })
    }

    /// Monotonic time captured once at the beginning of the driver turn.
    pub const fn now(&self) -> time::Instant {
        self.now
    }

    /// Region authority for this admitted step only.
    pub fn region(&mut self) -> &mut region::Token<'d> {
        self.region
    }
}
