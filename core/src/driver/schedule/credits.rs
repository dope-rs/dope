use std::marker;

use o3::mem::quota;

use crate::driver::schedule::{self, ready, timer};

struct Domain<'d>(marker::PhantomData<fn(&'d ()) -> &'d ()>);
struct BudgetTag<'d, Lane> {
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
    lane: marker::PhantomData<fn(Lane) -> Lane>,
}

#[must_use = "unused scheduler quota is returned when the quota is dropped"]
#[repr(transparent)]
pub(crate) struct Quota<'turn, 'd> {
    inner: quota::Shared<'turn, Domain<'d>>,
}

#[must_use = "unused scheduler budget is returned when the budget is dropped"]
#[repr(transparent)]
pub(crate) struct Budget<'turn, 'd, Lane> {
    inner: quota::Lease<'turn, BudgetTag<'d, Lane>>,
}

impl<'turn, 'd> Quota<'turn, 'd> {
    pub(super) fn from_reactor(ledger: &'turn quota::Ledger<super::ReactorLane>) -> Self {
        Self {
            inner: quota::Shared::reserve_all(ledger),
        }
    }

    pub(super) fn from_application(
        work: super::Application<'turn, 'd>,
        count: usize,
    ) -> Option<Self> {
        Some(Self {
            inner: quota::Shared::reserve_exact(work.remaining, count)?,
        })
    }

    pub(super) fn from_maintenance(work: super::Maintenance<'turn, 'd>, limit: usize) -> Self {
        Self {
            inner: quota::Shared::reserve_up_to(&work.work.maintenance, limit),
        }
    }

    pub(super) fn reserve_budget<'quota, Lane>(
        &'quota self,
        limit: usize,
    ) -> Budget<'quota, 'd, Lane> {
        Budget {
            inner: self.inner.lease_up_to(limit),
        }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.inner.remaining()
    }

    pub(crate) fn spend(&mut self, count: usize) {
        self.inner.spend(count);
    }
}

impl<'turn, 'd> Budget<'turn, 'd, ready::Lane> {
    pub(super) fn from_ready(work: super::Turn<'turn, 'd>, limit: usize) -> Self {
        Self {
            inner: quota::Lease::reserve_up_to(&work.work.ready, limit),
        }
    }
}

impl<'turn, 'd> Budget<'turn, 'd, timer::Lane> {
    pub(super) fn from_timers(work: super::Timers<'turn, 'd>) -> Self {
        Self {
            inner: quota::Lease::reserve_all(work.remaining),
        }
    }
}

impl<Lane> Budget<'_, '_, Lane> {
    pub(crate) const fn remaining(&self) -> usize {
        self.inner.remaining()
    }

    pub(crate) fn take(&mut self) -> bool {
        if self.inner.remaining() == 0 {
            return false;
        }
        self.spend(1);
        true
    }

    pub(crate) fn spend(&mut self, count: usize) {
        self.inner.spend(count);
    }

    pub(crate) fn admit_with<T>(
        &mut self,
        acquire: impl FnOnce() -> Option<T>,
    ) -> schedule::Admission<T> {
        match self.inner.admit_with(acquire) {
            quota::Admission::Item(value) => schedule::Admission::Item(value),
            quota::Admission::Empty => schedule::Admission::Empty,
            quota::Admission::Exhausted => schedule::Admission::Exhausted,
        }
    }
}

const _: () =
    assert!(std::mem::size_of::<Quota<'static, 'static>>() == 2 * std::mem::size_of::<usize>());
const _: () = assert!(
    std::mem::size_of::<Budget<'static, 'static, ()>>() == 2 * std::mem::size_of::<usize>()
);
const _: () =
    assert!(std::mem::size_of::<schedule::Admission<usize>>() == 2 * std::mem::size_of::<usize>());
