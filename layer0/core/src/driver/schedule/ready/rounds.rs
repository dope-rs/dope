use std::{mem, num::NonZero};

use crate::{
    dope_sys::Budget as _,
    driver,
    driver::{
        route,
        route::kind,
        schedule::{ready, state::lanes},
    },
};

/// Turn-scoped access to successive stable ready generations.
#[must_use = "ready quota is returned without driving while the rounds remain unused"]
pub struct Rounds<'turn, 'd> {
    driver: driver::Reference<'d>,
    budget: lanes::Budget<'turn, 'd, ready::Lane>,
}

impl<'turn, 'd> Rounds<'turn, 'd> {
    pub(in crate::driver::schedule) fn new(
        driver: driver::Reference<'d>,
        budget: lanes::Budget<'turn, 'd, ready::Lane>,
    ) -> Self {
        Self { driver, budget }
    }

    /// Drains exactly one non-empty stable generation.
    pub fn drain(
        &mut self,
        activate: impl FnMut(route::Erased<'d, kind::Any>),
    ) -> Option<NonZero<usize>> {
        let driver = self.driver;
        let access = ready::Access::new(&driver);
        let drained = driver
            .ready()
            .arena()
            .drain(access, &mut self.budget, activate);
        NonZero::<usize>::new(drained)
    }

    pub fn is_exhausted(&self) -> bool {
        self.budget.remaining() == 0
    }
}

const _: () = assert!(mem::size_of::<Rounds<'static, 'static>>() == 3 * mem::size_of::<usize>());
