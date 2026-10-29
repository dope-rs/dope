use std::mem;

use metadata::pool::{self, store};
use o3::base::cell::region;
use slots::Slots;

use crate::link::egress::{
    self,
    load::{Value, credit},
    metadata,
    reserve::{
        Bounds, Publish,
        bounds::Shape,
        demand,
        error::Error,
        guard::{Guard, Rollback},
    },
};

mod slots;

fn storage(lane: metadata::Lane, bounds: Bounds, free: egress::Entries) -> egress::Admission {
    egress::Admission::Storage {
        lane,
        requested: egress::Credit::new(bounds.entries().into_credit(), bounds.resident()),
        available: free,
    }
}

/// Reserved final-link nodes whose state owns their capacity charge.
#[must_use = "dropping vacant final-link nodes rolls the reservation back"]
pub struct Vacant<'queue, 'token, 'p, 'd, B, D: demand::Mode = demand::Fallible> {
    bounds: Bounds,
    state: Guard<'token, 'd, Slots<'queue, 'p, 'd, B, D>>,
}

impl<'queue, 'token, 'p, 'd, B, D: demand::Mode> Vacant<'queue, 'token, 'p, 'd, B, D> {
    pub(super) const fn empty(
        token: &'token mut region::Token<region::Identity<'d>>,
        bounds: Bounds,
    ) -> Self {
        Self {
            bounds,
            state: Guard::new(token, Slots::Empty),
        }
    }

    pub(super) fn try_new(
        bounds: Bounds,
        shape: Shape,
        token: &'token mut region::Token<region::Identity<'d>>,
        mut credit: credit::Reserve<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D>,
    ) -> Result<Self, egress::Admission> {
        let queue = credit.queue();
        let slots = match queue.pool.store.reservation(token) {
            Ok(slots) => slots,
            Err(token) => {
                credit.rollback(token);
                return Err(storage(queue.lane, bounds, egress::Entries::new(0)));
            }
        };
        let (token, slots) = match shape {
            Shape::First => {
                let claim = credit::Claim::from(credit);
                let (token, index, claim, ()) = slots.reserve(claim, ());
                (token, Slots::First(pool::Vacant::new(claim, index, ())))
            }
            Shape::Second => {
                let claim = credit::Claim::from(credit);
                let (token, index, claim, ()) = slots.reserve(claim, ());
                (token, Slots::Second(pool::Vacant::new(claim, index, ())))
            }
            Shape::Pair(pair) => {
                let slots = match slots.pair() {
                    store::Pairing::Two(slots) => slots,
                    store::Pairing::One(token) => {
                        credit.rollback(token);
                        return Err(storage(queue.lane, bounds, egress::Entries::new(1)));
                    }
                };
                let first = credit::Claim::from(credit.split_pair(pair));
                let second = credit::Claim::from(credit);
                let (token, (idx, first, ()), (next, second, ())) =
                    slots.reserve((first, ()), (second, ()));
                (
                    token,
                    Slots::Pair(
                        pool::Vacant::new(first, idx, ()),
                        pool::Vacant::new(second, next, ()),
                    ),
                )
            }
        };
        Ok(Self {
            bounds,
            state: Guard::new(token, slots),
        })
    }

    pub fn try_publish(
        self,
        first: Option<Value<B>>,
        second: Option<Value<B>>,
    ) -> Result<Publish, Error<B>> {
        let (token, slots) = self.state.into_parts();
        slots.try_publish(self.bounds, first, second, token)
    }

    pub fn cancel(self) {
        drop(self);
    }
}

const _: () = {
    assert!(
        mem::size_of::<Vacant<'static, 'static, 'static, 'static, ()>>()
            == mem::size_of::<(
                Bounds,
                Guard<'static, 'static, Slots<'static, 'static, 'static, (), demand::Fallible>>,
            )>()
    );
    assert!(
        mem::align_of::<Vacant<'static, 'static, 'static, 'static, ()>>()
            == mem::align_of::<(
                Bounds,
                Guard<'static, 'static, Slots<'static, 'static, 'static, (), demand::Fallible>>,
            )>()
    );
};
