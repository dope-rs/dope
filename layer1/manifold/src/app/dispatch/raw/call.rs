use std::{convert, mem, pin};

use dope_core::driver::schedule::clock;
use o3::base::cell::region;

/// A selected read-only liveness query for one exact pinned owner.
#[must_use]
pub struct Call<'a, 'region, 'turn, 'd, M, P> {
    owner: pin::Pin<&'a M>,
    plan: P,
    region: &'region (
        region::Token<region::Identity<'d>>,
        region::Token<region::Identity<'d, 1>>,
    ),
    clock: clock::Clock<'turn, 'd>,
}

impl<'a, 'region, 'turn, 'd, M, P> Call<'a, 'region, 'turn, 'd, M, P> {
    pub(super) fn new(
        owner: pin::Pin<&'a M>,
        plan: P,
        region: &'region (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
        clock: clock::Clock<'turn, 'd>,
    ) -> Self {
        Self {
            owner,
            plan,
            region,
            clock,
        }
    }

    pub fn owner(&self) -> pin::Pin<&'a M> {
        self.owner
    }

    pub fn region(&self) -> &'region region::Token<region::Identity<'d>> {
        &self.region.0
    }

    pub fn state(&self) -> &'region region::Token<region::Identity<'d, 1>> {
        &self.region.1
    }

    pub fn tokens(
        &self,
    ) -> &'region (
        region::Token<region::Identity<'d>>,
        region::Token<region::Identity<'d, 1>>,
    ) {
        self.region
    }

    pub fn clock(&self) -> clock::Clock<'turn, 'd> {
        self.clock
    }

    pub fn map_owner<U>(
        self,
        map: impl FnOnce(pin::Pin<&'a M>) -> pin::Pin<&'a U>,
    ) -> Call<'a, 'region, 'turn, 'd, U, P> {
        Call {
            owner: map(self.owner),
            plan: self.plan,
            region: self.region,
            clock: self.clock,
        }
    }
}

impl<'a, 'region, 'turn, 'd, M> Call<'a, 'region, 'turn, 'd, M, convert::Infallible> {
    pub(super) fn into_never(self) -> ! {
        match self {}
    }
}

const _: () = assert!(
    mem::size_of::<Call<'static, 'static, 'static, 'static, (), ()>>()
        == 3 * mem::size_of::<usize>()
);
