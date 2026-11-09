use std::{marker, mem::size_of};

use o3::mem::{self, lease};

use crate::reply::{
    recv::Acceptance,
    retain::{
        self,
        grant::payload,
        pool::{Acquire, Owner},
    },
    usage::amount::Amount,
};

pub(in crate::reply) struct Lease<
    'a,
    'arena,
    'credits,
    'region,
    A: mem::raw::Amount<3, usize> + Into<payload::Requested>,
> {
    inner: mem::Lease<Refund<'a, 'arena, 'credits, 'region, A>, lease::Open>,
}

struct Refund<'a, 'arena, 'credits, 'region, A> {
    owner: &'a mut Owner<'arena, 'credits, 'region>,
    amount: marker::PhantomData<A>,
}

impl<'a, 'arena, 'credits, 'region, A> Lease<'a, 'arena, 'credits, 'region, A>
where
    A: mem::raw::Amount<3, usize> + Into<payload::Requested>,
{
    pub(in crate::reply) fn try_new(
        owner: &'a mut Owner<'arena, 'credits, 'region>,
        amount: A,
    ) -> Result<Self, Acquire> {
        let acceptance = owner.acceptance.unwrap_or(Acceptance::Collected);
        owner.payload.try_acquire(amount, acceptance)?;
        Ok(Self {
            inner: mem::Lease::open(
                Refund {
                    owner,
                    amount: marker::PhantomData,
                },
                amount,
            ),
        })
    }

    pub(in crate::reply) fn amount(&self) -> A {
        *self.inner.item()
    }

    pub(in crate::reply) fn into_owner(self) -> &'a mut Owner<'arena, 'credits, 'region> {
        let (refund, _) = self.inner.into_parts();
        refund.owner
    }
}

impl<A: mem::raw::Amount<3, usize> + Into<payload::Requested>> mem::Return
    for Refund<'_, '_, '_, '_, A>
{
    type Item = A;

    fn return_item(self, amount: A) {
        // SAFETY: the receipt retains the original exclusive slot loan and the
        // exact new admission; neither commit nor another removal has consumed it.
        unsafe { retain::raw::Remove::remove(&mut self.owner.payload, amount) }.release();
    }
}

impl<A: Amount> Lease<'_, '_, '_, '_, payload::Value<A>> {
    pub(in crate::reply) fn keep(self) {
        let owner = self.into_owner();
        if owner.acceptance == Some(Acceptance::First) {
            owner.acceptance = None;
        }
    }
}

const _: () = {
    assert!(
        size_of::<Lease<'static, 'static, 'static, 'static, payload::Value>>()
            == size_of::<(&mut Owner<'static, 'static, 'static>, payload::Value)>()
    );
    assert!(
        size_of::<Lease<'static, 'static, 'static, 'static, payload::Failure>>()
            == size_of::<(&mut Owner<'static, 'static, 'static>, payload::Failure)>()
    );
};
