use std::{mem, task};

use dope_core::driver::schedule::ready;
use o3::{base::cell::region, mem::Rejected};

use crate::link::egress::{
    self, Geometry, NonEmpty,
    data::{self, compose},
    load::{credit, lease},
    metadata::{self, pool},
    reserve::{
        demand,
        guard::{Guard, Rollback},
    },
};

/// One owned payload paired with the exact queue geometry derived from it.
#[must_use]
pub struct Offer<'queue, 'p, 'd, B, D: demand::Mode = demand::Fallible, S = B> {
    queue: metadata::Queue<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D>,
    value: NonEmpty<S>,
}

/// A proved source retained with the exact failed reservation.
#[must_use]
#[repr(transparent)]
pub struct Unavailable<S>(Rejected<NonEmpty<S>, Failure>);

/// Exact failure to reserve one already-proved payload geometry.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Failure {
    Tickets {
        lane: metadata::Lane,
        cause: egress::Tickets,
    },
    Lane {
        lane: metadata::Lane,
        geometry: Geometry,
    },
    Storage {
        lane: metadata::Lane,
        geometry: Geometry,
    },
}

/// One attempt to reserve capacity for an owned exact payload.
#[must_use]
pub enum Outcome<'queue, 'token, 'p, 'd, B, D: demand::Mode = demand::Fallible, S = B> {
    Ready(Prepared<'queue, 'token, 'p, 'd, B, D, S>),
    Pending(NonEmpty<S>),
    Rejected(Unavailable<S>),
}

/// A non-empty payload validated against its exact reserved geometry.
#[must_use = "dropping a prepared payload rolls its reservation back"]
pub struct Prepared<'queue, 'token, 'p, 'd, B, D: demand::Mode = demand::Fallible, S = B> {
    state: Guard<'token, 'd, pool::Vacant<'queue, 'p, 'd, B, (), credit::Ready<'p, 'd>, D>>,
    value: NonEmpty<S>,
}

impl<'queue, 'p, 'd, B, D: demand::Mode, S> Offer<'queue, 'p, 'd, B, D, S> {
    pub(in crate::link::egress) fn new(
        queue: metadata::Queue<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D>,
        value: NonEmpty<S>,
    ) -> Result<Self, Unavailable<S>> {
        let expected = value.geometry();
        let amount = credit::Amount::new(egress::Entries::new(1), expected.resident());
        if !queue.fits(amount) {
            return Err(Unavailable::new(
                value,
                Failure::Lane {
                    lane: queue.lane,
                    geometry: expected,
                },
            ));
        }
        Ok(Self { queue, value })
    }

    pub fn into_value(self) -> NonEmpty<S> {
        self.value
    }

    pub fn poll_reserve<'token>(
        self,
        token: &'token mut region::Token<region::Identity<'d>>,
        target: ready::Target<'d>,
    ) -> Outcome<'queue, 'token, 'p, 'd, B, D, S> {
        let Self { queue, value } = self;
        let expected = value.geometry();
        match queue.poll_acquire(token, credit::Queued::one(expected.resident()), target) {
            task::Poll::Ready(Err(cause)) => Outcome::Rejected(Unavailable::new(
                value,
                Failure::Tickets {
                    lane: queue.lane,
                    cause,
                },
            )),
            task::Poll::Ready(Ok(credit)) => {
                let slot = match queue.pool.store.reservation(token) {
                    Ok(slot) => slot,
                    Err(token) => {
                        credit.rollback(token);
                        return Outcome::Rejected(Unavailable::new(
                            value,
                            Failure::Storage {
                                lane: queue.lane,
                                geometry: expected,
                            },
                        ));
                    }
                };
                let claim = credit::Claim::from(credit);
                let (token, index, claim, ()) = slot.reserve(claim, ());
                let node = pool::Vacant::new(claim, index, ());
                Outcome::Ready(Prepared {
                    state: Guard::new(token, node),
                    value,
                })
            }
            task::Poll::Pending => Outcome::Pending(value),
        }
    }
}

impl<S> Unavailable<S> {
    const fn new(value: NonEmpty<S>, cause: Failure) -> Self {
        Self(Rejected::new(value, cause))
    }

    pub const fn cause(&self) -> Failure {
        *self.0.error()
    }

    pub fn into_parts(self) -> (NonEmpty<S>, Failure) {
        self.0.into_parts()
    }
}

impl<'queue, 'token, 'p, 'd, L, R, D: demand::Mode>
    Prepared<'queue, 'token, 'p, 'd, compose::Either<L, R>, D, R>
{
    pub fn right(self) {
        let Self { state, value } = self;
        let (token, slot) = state.into_parts();
        slot.replace(token, value.into_right());
    }
}

impl<'queue, 'token, 'p, 'd, H, L, R, D: demand::Mode>
    Prepared<'queue, 'token, 'p, 'd, compose::Either<H, compose::Either<L, R>>, D, L>
{
    pub fn right_left(self) {
        let Self { state, value } = self;
        let (token, slot) = state.into_parts();
        slot.replace(token, value.into_left().into_right());
    }
}

impl<'queue, 'token, 'p, 'd, H, L, R, D: demand::Mode>
    Prepared<'queue, 'token, 'p, 'd, compose::Either<H, compose::Either<L, R>>, D, R>
{
    pub fn right_right(self) {
        let Self { state, value } = self;
        let (token, slot) = state.into_parts();
        slot.replace(token, value.into_right().into_right());
    }
}

impl<'queue, 'token, 'p, 'd, T, K, D: demand::Mode>
    Prepared<'queue, 'token, 'p, 'd, T, D, compose::Tagged<T, K>>
{
    pub(in crate::link::egress) fn untag(self) {
        let Self { state, value } = self;
        let (token, slot) = state.into_parts();
        slot.replace(token, value.untag());
    }
}

impl<'queue, 'token, 'p, 'd, H, L, R, G, D: demand::Mode>
    Prepared<
        'queue,
        'token,
        'p,
        'd,
        compose::Either<H, data::Bound<compose::Either<L, R>, G>>,
        D,
        data::Bound<R, G>,
    >
{
    pub fn bound_right(self) {
        let Self { state, value } = self;
        let (token, slot) = state.into_parts();
        slot.replace(token, value.bound_right().into_right());
    }
}

impl<'queue, 'token, 'p, 'd, L, R, D: demand::Mode, U: lease::Commit<Item = L>>
    Prepared<'queue, 'token, 'p, 'd, compose::Either<L, R>, D, lease::Lease<U>>
{
    pub fn commit_left(self) {
        let Self { state, value } = self;
        let (token, slot) = state.into_parts();
        let (owner, value) = value.settle();
        owner.commit();
        slot.replace(token, value.into_left());
    }
}

impl<'queue, 'token, 'p, 'd, L, R, D: demand::Mode, U: lease::Commit<Item = R>>
    Prepared<'queue, 'token, 'p, 'd, compose::Either<L, R>, D, lease::Lease<U>>
{
    pub fn commit_right(self) {
        let Self { state, value } = self;
        let (token, slot) = state.into_parts();
        let (owner, value) = value.settle();
        owner.commit();
        slot.replace(token, value.into_right());
    }
}

impl<'queue, 'token, 'p, 'd, B, D: demand::Mode> Prepared<'queue, 'token, 'p, 'd, B, D> {
    pub fn publish(self) {
        let Self { state, value } = self;
        let (token, slot) = state.into_parts();
        slot.replace(token, value);
    }

    pub fn publish_with(self, commit: impl FnOnce(&mut region::Token<region::Identity<'d>>)) {
        let Self { state, value } = self;
        let (token, slot) = state.into_parts();
        commit(token);
        slot.replace(token, value);
    }
}

const _: () = {
    assert!(mem::size_of::<Failure>() == 2 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Offer<'static, 'static, 'static, ()>>()
            == mem::size_of::<(
                metadata::Queue<
                    'static,
                    'static,
                    'static,
                    (),
                    credit::Ready<'static, 'static>,
                    demand::Fallible,
                >,
                NonEmpty<()>,
            )>()
    );
    assert!(
        mem::size_of::<Prepared<'static, 'static, 'static, 'static, ()>>()
            == mem::size_of::<(
                Guard<
                    'static,
                    'static,
                    pool::Vacant<
                        'static,
                        'static,
                        'static,
                        (),
                        (),
                        credit::Ready<'static, 'static>,
                        demand::Fallible,
                    >,
                >,
                NonEmpty<()>,
            )>()
    );
};
