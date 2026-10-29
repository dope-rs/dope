use std::{marker, mem, num::NonZero};

use o3::mem::{lease, quota};

use crate::driver::{
    schedule,
    schedule::{app, state},
};

type Invariant<'d, Lane> = marker::PhantomData<fn(&'d (), Lane) -> (&'d (), Lane)>;

pub(in crate::driver::schedule) struct Domain<'d, Lane = ()>(Invariant<'d, Lane>);

#[must_use = "unused scheduler quota is returned when the quota is dropped"]
#[repr(transparent)]
pub(in crate::driver::schedule) struct Quota<'turn, 'd, M = lease::Sealed> {
    pub(in crate::driver::schedule) inner: quota::Shared<'turn, Domain<'d>, M>,
}

#[must_use = "unused scheduler budget is returned when the budget is dropped"]
#[repr(transparent)]
pub(crate) struct Budget<'turn, 'd, Lane> {
    inner: quota::Lease<'turn, Domain<'d, Lane>>,
}

impl<'turn, 'd> Quota<'turn, 'd> {
    pub(in crate::driver::schedule) fn share<const N: usize, Lane>(
        ledger: &'turn quota::Ledger<Lane>,
        rank: usize,
    ) -> Self {
        const { assert!(N != 0) };
        let total = ledger.remaining();
        let allowed = if total < N {
            usize::from(rank < total)
        } else {
            total.div_ceil(N)
        };
        Self {
            inner: quota::Shared::reserve_up(ledger, total - allowed),
        }
    }

    pub(in crate::driver::schedule) fn weighted<const N: usize, Lane>(
        ledger: &'turn quota::Ledger<Lane>,
        rank: usize,
        weights: [usize; 2],
    ) -> Option<Self> {
        let total = NonZero::new(weights[0].saturating_add(weights[1]))?;
        let credits = ledger.remaining();
        if credits < total.get() {
            return Some(Self::share::<N, _>(ledger, rank));
        }
        let own = weights[0];
        let allowed = credits / total.get() * own + (credits % total.get()).min(own);
        Some(Self {
            inner: quota::Shared::reserve_up(ledger, credits - allowed),
        })
    }

    pub(in crate::driver::schedule) fn reactor(
        ledger: &'turn quota::Ledger<state::ReactorLane>,
    ) -> Self {
        Self {
            inner: quota::Shared::reserve_all(ledger),
        }
    }

    pub(in crate::driver::schedule) fn reserve_app(
        work: app::Work<'turn, 'd>,
        count: usize,
    ) -> schedule::Capacity<Quota<'turn, 'd, lease::Open>> {
        let ledger = &work.state.lanes.app;
        match quota::Shared::reserve(ledger, count) {
            Some(inner) => schedule::Capacity::Available(Quota { inner }),
            None => schedule::Capacity::Exhausted,
        }
    }

    pub(in crate::driver::schedule) fn lease<'quota, Lane>(
        &'quota self,
        limit: usize,
    ) -> Budget<'quota, 'd, Lane> {
        Budget {
            inner: self.inner.lease_up_to(limit),
        }
    }

    pub(in crate::driver::schedule) fn remaining(&self) -> usize {
        self.inner.remaining()
    }
}

impl<'turn, 'd, Lane> Budget<'turn, 'd, Lane> {
    pub(in crate::driver::schedule) fn reserve_all(ledger: &'turn quota::Ledger<Lane>) -> Self {
        Self {
            inner: quota::Lease::reserve_all(ledger),
        }
    }
}

impl<Lane> dope_sys::Budget for Budget<'_, '_, Lane> {
    fn remaining(&self) -> usize {
        self.inner.remaining()
    }

    fn admit<T>(&mut self, acquire: impl FnOnce() -> Option<T>) -> quota::Admission<T> {
        self.inner.admit_with(acquire)
    }
}

const _: () = {
    let words = 2 * mem::size_of::<usize>();
    assert!(mem::size_of::<Quota<'static, 'static>>() == words);
    assert!(mem::size_of::<Option<Quota<'static, 'static>>>() == words);
    assert!(mem::size_of::<Quota<'static, 'static, lease::Open>>() == words);
    assert!(mem::size_of::<Budget<'static, 'static, ()>>() == words);
};
