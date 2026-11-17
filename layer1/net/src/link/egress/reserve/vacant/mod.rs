use std::mem;

use metadata::pool::store;
use o3::base::cell::region;
use slots::Slots;

use crate::link::egress::{
    self, data,
    load::{Value, credit},
    metadata,
    reserve::{
        Bounds, Geometry, Publish,
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
pub struct Vacant<
    'queue,
    'token,
    'p,
    'd,
    B: data::raw::Payload<'d>,
    D: demand::Mode = demand::Fallible,
    G: Geometry = Bounds,
> {
    pub(super) bounds: G,
    pub(super) state: Guard<'token, 'd, Slots<'queue, 'p, 'd, B, D, G>>,
}

impl<'queue, 'token, 'p, 'd, B: data::raw::Payload<'d>, D: demand::Mode, G: Geometry>
    Vacant<'queue, 'token, 'p, 'd, B, D, G>
{
    pub(super) const fn empty(
        token: &'token mut region::Token<region::Identity<'d>>,
        bounds: G,
    ) -> Self {
        Self {
            bounds,
            state: Guard::new(token, Slots::Empty),
        }
    }

    pub(super) fn acquire(
        &mut self,
        shape: G::Selection,
        credit: credit::Reserve<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D, B::Position>,
    ) -> Result<(), egress::Admission> {
        let (token, state) = self.state.parts_mut();
        let queue = credit.queue();
        let slots = match queue.pool.store.reservation(token) {
            Ok(slots) => slots,
            Err(token) => {
                credit.rollback(token);
                return Err(storage(queue.lane, self.bounds.bounds(), egress::Entries::new(0)));
            }
        };
        let (first, second) = match G::kind(shape) {
            Shape::First | Shape::Second => {
                let (_, first) = slots.reserve();
                (first, None)
            }
            Shape::Pair => {
                let slots = match slots.pair() {
                    store::Pairing::Two(slots) => slots,
                    store::Pairing::One(token) => {
                        credit.rollback(token);
                        return Err(storage(queue.lane, self.bounds.bounds(), egress::Entries::new(1)));
                    }
                };
                let (_, first, second) = slots.reserve();
                (first, Some(second))
            }
        };
        *state = Slots::Reserved(credit, G::store(shape, first, second));
        Ok(())
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
        mem::size_of::<Vacant<'static, 'static, 'static, 'static, &'static [u8]>>()
            == mem::size_of::<(
                Bounds,
                Guard<
                    'static,
                    'static,
                    Slots<'static, 'static, 'static, &'static [u8], demand::Fallible, Bounds>,
                >,
            )>()
    );
    assert!(
        mem::align_of::<Vacant<'static, 'static, 'static, 'static, &'static [u8]>>()
            == mem::align_of::<(
                Bounds,
                Guard<
                    'static,
                    'static,
                    Slots<'static, 'static, 'static, &'static [u8], demand::Fallible, Bounds>,
                >,
            )>()
    );
};
