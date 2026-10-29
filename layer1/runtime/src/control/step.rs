use std::mem;

use dope_core::driver::{self, schedule::clock};
use o3::base::cell::region;

/// One linear application control step borrowed from an exact pre-park call.
#[must_use = "dropping a control step consumes its admitted application work"]
pub struct Step<'step, 'd: 'step> {
    driver: driver::Context<'step, 'd>,
}

impl<'step, 'd: 'step> Step<'step, 'd> {
    pub(crate) fn new(driver: driver::Context<'step, 'd>) -> Self {
        Self { driver }
    }

    /// Monotonic time captured once at the beginning of the driver turn.
    pub fn now(&self) -> clock::Moment<'d> {
        self.driver.driver_ref().clock().now()
    }

    /// Read-only authority for the exact monotonic sample installed for this
    /// driver turn.
    pub fn clock(&self) -> clock::Clock<'_, 'd> {
        self.driver.driver_ref().clock()
    }

    /// Region authority for this admitted step only.
    pub fn region(&mut self) -> &mut region::Token<region::Identity<'d>> {
        self.driver.region_token()
    }
}

const fn assert_layout<'a, 'b: 'a>() {
    assert!(mem::size_of::<Step<'a, 'b>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Step<'a, 'b>>() == mem::align_of::<usize>());
}

const _: () = assert_layout();
