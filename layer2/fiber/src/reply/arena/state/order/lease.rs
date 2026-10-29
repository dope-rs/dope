use std::{marker, mem::size_of};

use o3::mem::{self, lease};

use crate::reply::{
    arena::state::order,
    order::Depth,
    retain::{self, grant::payload},
    usage,
};

pub(in crate::reply) struct Lease<
    'a,
    'arena,
    'credits,
    'region,
    A: mem::raw::Amount<3, usize> + Into<payload::Returned> + Into<payload::Requested>,
    K = (),
> {
    inner: mem::Lease<Refund<'a, 'arena, 'credits, 'region, A, K>, lease::Open>,
}

struct Refund<'a, 'arena, 'credits, 'region, A, K> {
    slot: order::Slot<'a, 'arena, 'credits, 'region, K>,
    amount: marker::PhantomData<A>,
}

impl<'a, 'arena, 'credits, 'region, A, K> Lease<'a, 'arena, 'credits, 'region, A, K>
where
    A: mem::raw::Amount<3, usize> + Into<payload::Returned> + Into<payload::Requested>,
{
    pub(super) fn new(slot: order::Slot<'a, 'arena, 'credits, 'region, K>, amount: A) -> Self {
        Self {
            inner: mem::Lease::open(
                Refund {
                    slot,
                    amount: marker::PhantomData,
                },
                amount,
            ),
        }
    }

    pub(in crate::reply) const fn amount(&self) -> A {
        *self.inner.item()
    }

    pub(in crate::reply) fn keep(self) {
        self.inner.into_parts();
    }

    pub(in crate::reply::arena::state) fn discard(self) -> Depth {
        let (refund, _) = self.inner.into_parts();
        refund.slot.cancel_pop()
    }
}

impl<A: mem::raw::Amount<3, usize> + Into<payload::Returned> + Into<payload::Requested>, K>
    mem::Return for Refund<'_, '_, '_, '_, A, K>
{
    type Item = A;

    fn return_item(self, amount: A) {
        // SAFETY: the receipt retains the original exclusive slot loan and the
        // exact new admission; neither commit nor another removal has consumed it.
        unsafe { retain::raw::Remove::remove(&mut self.slot.owner.payload, amount) }.release();
    }
}

impl<K> Lease<'_, '_, '_, '_, payload::Value, K> {
    pub(in crate::reply) fn usage(&self) -> usage::Usage {
        self.amount().usage()
    }
}

const _: () = {
    assert!(
        size_of::<Lease<'static, 'static, 'static, 'static, payload::Value>>()
            == size_of::<(
                order::Slot<'static, 'static, 'static, 'static>,
                payload::Value
            )>()
    );
    assert!(
        size_of::<Lease<'static, 'static, 'static, 'static, payload::Failure>>()
            == size_of::<(
                order::Slot<'static, 'static, 'static, 'static>,
                payload::Failure
            )>()
    );
};
