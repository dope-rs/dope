use std::{marker, mem, num::NonZero};

use o3::mem::quota;

type Invariant<'d, Lane> = marker::PhantomData<fn(&'d (), Lane) -> (&'d (), Lane)>;

pub(in crate::driver::schedule) struct Domain<'d, Lane = ()>(Invariant<'d, Lane>);

#[must_use = "unused scheduler budget is returned when the budget is dropped"]
#[repr(transparent)]
pub(crate) struct Budget<'turn, 'd, Lane> {
    inner: quota::Lease<'turn, Domain<'d, Lane>>,
}

pub(in crate::driver::schedule) fn share<'turn, 'd, const N: usize, Lane>(
    ledger: &'turn quota::Ledger<Lane>,
    rank: usize,
) -> quota::Shared<'turn, Domain<'d>> {
    const { assert!(N != 0) };
    let total = ledger.remaining();
    let allowed = if total < N {
        usize::from(rank < total)
    } else {
        total.div_ceil(N)
    };
    quota::Shared::reserve_up(ledger, total - allowed)
}

pub(in crate::driver::schedule) fn weighted<'turn, 'd, const N: usize, Lane>(
    ledger: &'turn quota::Ledger<Lane>,
    rank: usize,
    weights: [usize; 2],
) -> Option<quota::Shared<'turn, Domain<'d>>> {
    let total = NonZero::new(weights[0].saturating_add(weights[1]))?;
    let credits = ledger.remaining();
    if credits < total.get() {
        return Some(share::<N, _>(ledger, rank));
    }
    let own = weights[0];
    let allowed = credits / total.get() * own + (credits % total.get()).min(own);
    Some(quota::Shared::reserve_up(ledger, credits - allowed))
}

impl<'turn, 'd, Lane> Budget<'turn, 'd, Lane> {
    pub(in crate::driver::schedule) fn lease(
        quota: &'turn quota::Shared<'_, Domain<'d>>,
        limit: usize,
    ) -> Self {
        Self {
            inner: quota.lease_up_to(limit),
        }
    }

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
    assert!(mem::size_of::<quota::Shared<'static, Domain<'static>>>() == words);
    assert!(mem::size_of::<Option<quota::Shared<'static, Domain<'static>>>>() == words);
    assert!(mem::size_of::<Budget<'static, 'static, ()>>() == words);
};
