use std::task;

use data::compose;
use dope_core::{driver::schedule::ready, io::transfer};
use o3::base::cell::region;
use vacant::Vacant;

use crate::link::egress::{
    self, data,
    load::credit,
    metadata::{self, head, lane, pool::store::index::Index},
    reserve::{demand, exact, guard::Rollback},
};

mod vacant;

/// One detached queue head together with its exact payload and source credit.
#[must_use = "dropping a detached front discards its payload and releases its source credit"]
pub struct Front<'queue, 'token, 'p, 'd, T, D: demand::Mode = demand::Fallible> {
    value: egress::NonEmpty<T>,
    vacant: Vacant<'queue, 'token, 'p, 'd, T, metadata::Local, D>,
}

pub(in crate::link::egress) enum Consumed<T> {
    Whole {
        value: egress::NonEmpty<T>,
        remaining: transfer::Len,
    },
    Partial,
}

impl<T> Consumed<T> {
    /// Leaves an unfinished front linked or transfers its fully consumed owner.
    pub(in crate::link::egress) fn complete(self, release: &mut impl FnMut(T)) -> credit::Prefix {
        match self {
            Self::Whole {
                value,
                remaining: rest,
            } => {
                release(value.into_inner());
                credit::Prefix::Whole { remaining: rest }
            }
            Self::Partial => credit::Prefix::Partial,
        }
    }
}

enum Move<T> {
    Published,
    Pending(egress::NonEmpty<T>),
    Rejected(exact::Unavailable<T>),
}

/// Charge-preserving move from a detached source front to a final-link queue.
pub trait Transfer<'d, B: data::raw::Payload<'d>> {
    fn transfer_to<'q, const IOV: usize>(
        self,
        queue: &egress::Queue<'_, 'q, 'd, IOV, B>,
        target: ready::Target<'d>,
    ) -> task::Poll<Result<(), exact::Failure>>;
}

impl<'queue, 'token, 'p, 'd, T, D: demand::Mode> Front<'queue, 'token, 'p, 'd, T, D> {
    pub(super) fn new(
        queue: metadata::Queue<'queue, 'p, 'd, T, metadata::Local, D>,
        value: egress::NonEmpty<T>,
        slot: Index<'d>,
        stored: credit::Stored<'p>,
        token: &'token mut region::Token<region::Identity<'d>>,
    ) -> Self {
        Self {
            value,
            vacant: Vacant::new(queue, slot, token, stored),
        }
    }

    pub fn get(&self) -> &T {
        self.value.as_inner()
    }

    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }
    pub fn restore(self) {
        let Self { value, vacant } = self;
        vacant.restore(value);
    }

    fn move_to<'a, 'q, B: data::raw::Payload<'d>, const IOV: usize>(
        self,
        queue: &egress::Queue<'a, 'q, 'd, IOV, B>,
        target: ready::Target<'d>,
        publish: impl for<'t> FnOnce(exact::Prepared<'a, 't, 'q, 'd, B, demand::Fallible, T>),
    ) -> task::Poll<Result<(), exact::Failure>> {
        let Self { value, mut vacant } = self;
        let offer = match queue.offer(value) {
            Ok(offer) => offer,
            Err(miss) => {
                let (value, reason) = miss.into_parts();
                vacant.restore(value);
                return task::Poll::Ready(Err(reason));
            }
        };
        let moved = vacant.with_region(|token| match offer.poll_reserve(token, target) {
            exact::Outcome::Ready(prepared) => {
                publish(prepared);
                Move::Published
            }
            exact::Outcome::Pending(load) => Move::Pending(load),
            exact::Outcome::Rejected(rejected) => Move::Rejected(rejected),
        });
        match moved {
            Move::Published => task::Poll::Ready(Ok(())),
            Move::Pending(value) => {
                vacant.restore(value);
                task::Poll::Pending
            }
            Move::Rejected(rejected) => {
                let (value, reason) = rejected.into_parts();
                vacant.restore(value);
                task::Poll::Ready(Err(reason))
            }
        }
    }
}

pub(in crate::link::egress) fn take<'p, 'd, T, D: demand::Mode>(
    queue: metadata::Queue<'_, 'p, 'd, T, credit::Ready<'p, 'd>, D>,
    token: &mut region::Token<region::Identity<'d>>,
) -> Option<egress::NonEmpty<T>> {
    let (value, slot, stored) = head::detach(queue, token)?;
    vacant::Parts {
        queue,
        slot,
        stored,
    }
    .rollback(token);
    Some(value)
}

pub(in crate::link::egress) fn advance<'p, 'd, T, D: demand::Mode>(
    queue: metadata::Queue<'_, 'p, 'd, T, credit::Ready<'p, 'd>, D>,
    prefix: lane::Prefix<'p, 'd>,
    token: &mut region::Token<region::Identity<'d>>,
    bytes: transfer::NonZero,
) -> (Consumed<T>, Option<lane::Prefix<'p, 'd>>) {
    let credit::Prefix::Whole { remaining: rest } =
        queue.pool.store.consume(token, prefix.index(), bytes)
    else {
        return (Consumed::Partial, None);
    };
    let (((value, slot), stored), next) = prefix.advance(|slot| {
        let (next, value, entry) = queue.pool.store.detach(token, slot);
        (next, (value, slot), entry)
    });
    vacant::Parts {
        queue,
        slot,
        stored,
    }
    .rollback(token);
    (
        Consumed::Whole {
            value,
            remaining: rest,
        },
        next,
    )
}

impl<'queue, 'token, 'p, 'd, T, D: demand::Mode> Transfer<'d, T>
    for Front<'queue, 'token, 'p, 'd, T, D>
where
    T: data::raw::Payload<'d>,
{
    fn transfer_to<'q, const IOV: usize>(
        self,
        queue: &egress::Queue<'_, 'q, 'd, IOV, T>,
        target: ready::Target<'d>,
    ) -> task::Poll<Result<(), exact::Failure>> {
        self.move_to(queue, target, |prepared| prepared.publish())
    }
}

impl<'queue, 'token, 'p, 'd, T, K, D: demand::Mode> Transfer<'d, T>
    for Front<'queue, 'token, 'p, 'd, compose::Tagged<T, K>, D>
where
    T: data::raw::Payload<'d>,
{
    fn transfer_to<'q, const IOV: usize>(
        self,
        queue: &egress::Queue<'_, 'q, 'd, IOV, T>,
        target: ready::Target<'d>,
    ) -> task::Poll<Result<(), exact::Failure>> {
        self.move_to(queue, target, |prepared| prepared.untag())
    }
}
