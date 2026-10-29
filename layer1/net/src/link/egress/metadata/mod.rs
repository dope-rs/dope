use std::{
    num::{self, NonZero},
    ops, task,
};

use dope_core::driver::{
    route::{indices, table::conn},
    schedule::ready,
};
use o3::{
    base::{cell::region, num::bounded},
    depot::capacity,
};
use pool::store;

use crate::link::egress::{
    self, data,
    data::compose,
    load::{Value, credit},
    reserve::{demand, error},
};

pub mod arena;
pub mod batch;
mod branches;
pub mod front;
mod head;
pub(in crate::link::egress) mod lane;
pub(in crate::link::egress) mod pool;
pub mod reserve;

pub(in crate::link::egress) use branches::Prepared;
use reserve::Reserve;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(transparent)]
pub struct Lane(NonZero<u32>);

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(transparent)]
pub struct LaneCount(NonZero<u32>);

/// Exact lane count rejected by the lane position encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Count {
    actual: num::NonZero<usize>,
}

impl Count {
    pub const MAXIMUM: usize = u32::MAX as usize - 1;

    pub(super) const fn new(actual: num::NonZero<usize>) -> Self {
        Self { actual }
    }

    pub const fn actual(self) -> num::NonZero<usize> {
        self.actual
    }
}

impl Lane {
    /// Seals a nonzero lane position for collections reserving zero as empty.
    pub const fn at(position: NonZero<u32>) -> Self {
        Self(position)
    }

    /// Reuses a route slot proof as the corresponding metadata lane.
    pub fn from_slot(index: indices::SlotIndex) -> Self {
        Self(index.into())
    }

    pub const fn index(self) -> bounded::U32<0, { u32::MAX - 1 }> {
        bounded::U32::from_one(self.0)
    }

    pub(in crate::link::egress) const fn position(self) -> NonZero<u32> {
        self.0
    }
}

impl LaneCount {
    pub const fn try_new(count: num::NonZero<usize>) -> Result<Self, Count> {
        if count.get() >= u32::MAX as usize {
            return Err(Count::new(count));
        }
        Ok(Self(
            NonZero::<u32>::MIN.saturating_add(count.get() as u32 - 1),
        ))
    }

    pub const fn fixed<const N: usize>() -> Self {
        const {
            assert!(N != 0);
            assert!(N <= Count::MAXIMUM);
        }
        Self(NonZero::<u32>::MIN.saturating_add(N as u32 - 1))
    }

    pub const fn count(self) -> capacity::Count {
        capacity::Count::new(self.0.get())
    }

    pub(in crate::link::egress) const fn position(self) -> NonZero<u32> {
        self.0
    }
}

impl From<conn::Capacity> for LaneCount {
    fn from(count: conn::Capacity) -> Self {
        Self(count.count())
    }
}

const _: () = {
    assert!(std::mem::size_of::<Lane>() == std::mem::size_of::<NonZero<u32>>());
    assert!(std::mem::size_of::<Option<Lane>>() == std::mem::size_of::<Lane>());
    assert!(std::mem::size_of::<LaneCount>() == std::mem::size_of::<NonZero<u32>>());
    assert!(std::mem::size_of::<Option<LaneCount>>() == std::mem::size_of::<LaneCount>());
};

pub enum Reservation<'queue, 'token, 'p, 'd, T, D: demand::Mode = demand::Fallible> {
    Ready(Reserve<'queue, 'token, 'p, 'd, T, D>),
    Rejected(error::Capacity<T>),
}

pub enum Admission<T> {
    Committed,
    Rejected(egress::Rejected<Value<T>>),
}

pub struct Local;

impl<'p, 'd> credit::Access<'p, 'd> for Local {
    fn try_acquire(
        &self,
        _: &mut region::Token<region::Identity<'d>>,
        lane: credit::Lane<'_, 'p, 'd, Self>,
        amount: credit::Amount,
    ) -> Result<credit::Grant<'p>, egress::Admission> {
        let Local = *self;
        lane.try_charge(amount)
    }

    fn release(
        &self,
        _: &mut region::Token<region::Identity<'d>>,
        _: credit::Lane<'_, 'p, 'd, Self>,
        grant: credit::Grant<'p>,
    ) {
        let Local = *self;
        grant.release();
    }
}

pub struct Queue<'a, 'p, 'd, T, M = Local, D: demand::Mode = demand::Fallible> {
    pub(in crate::link::egress) pool: &'a pool::Pool<'p, 'd, T, M, D>,
    pub(in crate::link::egress) lane: Lane,
}

/// One queue paired with its own retained nonempty lane prefix.
pub(in crate::link::egress) struct Prefix<'queue, 'p, 'd, T, M, D: demand::Mode> {
    queue: Queue<'queue, 'p, 'd, T, M, D>,
    prefix: lane::Prefix<'p, 'd>,
}

impl<'p, T, M, D: demand::Mode> Clone for Queue<'_, 'p, '_, T, M, D> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'p, T, M, D: demand::Mode> Copy for Queue<'_, 'p, '_, T, M, D> {}

impl<'queue, 'p, 'd, T> Queue<'queue, 'p, 'd, T> {
    /// Inspects the current head without detaching it.
    pub fn inspect<K>(
        self,
        token: &region::Token<region::Identity<'d>>,
        f: impl FnOnce(&T) -> K,
    ) -> Option<K> {
        let index = self.state().head()?;
        let value = self.pool.store.value(token, index);
        Some(f(value.as_inner()))
    }

    /// Detaches the head while exclusively retaining the exact region token.
    pub fn take_front<'token>(
        self,
        token: &'token mut region::Token<region::Identity<'d>>,
    ) -> Option<front::Front<'queue, 'token, 'p, 'd, T>> {
        let (value, slot, stored) = head::detach(self, token)?;
        Some(front::Front::new(self, value, slot, stored, token))
    }

    /// Owns one exact non-empty payload before invoking protocol state.
    pub fn try_reserve<'token>(
        self,
        token: &'token mut region::Token<region::Identity<'d>>,
        load: egress::NonEmpty<T>,
    ) -> Reservation<'queue, 'token, 'p, 'd, T>
    where
        T: data::raw::Resident<'d>,
    {
        match Prepared::prepare(self, token, load) {
            Ok(ready) => Reservation::Ready(ready.reserve()),
            Err(cause) => Reservation::Rejected(cause),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.state().is_empty()
    }

    /// Returns whether the lane can reserve the exact batch.
    pub fn can_acquire(&self, plan: batch::Plan) -> bool {
        credit::Lane::new(*self).account().can_charge(plan.amount())
    }

    pub fn try_batch<'token, R, E>(
        self,
        token: &'token mut region::Token<region::Identity<'d>>,
        plan: batch::Plan,
        build: impl for<'batch> FnOnce(batch::Sink<'batch, 'queue, 'token, 'p, 'd, T>) -> Result<R, E>,
    ) -> Result<R, batch::Error<E>> {
        batch::Draft::try_new(self, token, plan)
            .map_err(batch::Error::Admission)?
            .try_build(|draft| build(batch::Sink::new(draft)))
    }
}

impl<'queue, 'p, 'd, T, M, D: demand::Mode> Queue<'queue, 'p, 'd, T, M, D> {
    pub(super) fn try_acquire(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        amount: credit::Amount,
    ) -> Result<credit::Reserve<'queue, 'p, 'd, T, M, D>, egress::Admission>
    where
        M: credit::Access<'p, 'd>,
    {
        credit::Lane::new(*self)
            .try_acquire(token, amount)
            .map(|grant| credit::Reserve::new(*self, grant))
    }

    pub(in crate::link::egress) fn fits(&self, amount: credit::Amount) -> bool
    where
        M: credit::Access<'p, 'd>,
    {
        credit::Lane::new(*self).account().fits(amount)
    }

    pub(in crate::link::egress) fn state(&self) -> &'p lane::Lane<'d>
    where
        M: credit::Access<'p, 'd>,
    {
        credit::Lane::new(*self).account().state()
    }

    pub(in crate::link::egress) fn prefix(self) -> Option<Prefix<'queue, 'p, 'd, T, M, D>>
    where
        M: credit::Access<'p, 'd>,
    {
        let prefix = lane::Prefix::try_new(self.state())?;
        Some(Prefix {
            queue: self,
            prefix,
        })
    }
}

impl<'queue, 'p, 'd, T, M, D: demand::Mode> Prefix<'queue, 'p, 'd, T, M, D> {
    pub(in crate::link::egress) fn visit(
        self,
        token: &region::Token<region::Identity<'d>>,
        mut f: impl FnMut(store::View<'_, T>) -> ops::ControlFlow<()>,
    ) -> (Queue<'queue, 'p, 'd, T, M, D>, lane::Prefix<'p, 'd>) {
        let Self { queue, prefix } = self;
        let mut slot = Some(prefix.index());
        while let Some(index) = slot {
            let next = queue.pool.store.next(token, index);
            let view = queue.pool.store.get(token, index);
            if f(view).is_break() {
                break;
            }
            slot = next;
        }
        (queue, prefix)
    }
}

impl<'queue, 'p, 'd, T, D: demand::Mode> Queue<'queue, 'p, 'd, T, credit::Ready<'p, 'd>, D> {
    pub(in crate::link::egress) fn poll_acquire(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        amount: credit::Queued,
        target: ready::Target<'d>,
    ) -> task::Poll<
        Result<credit::Reserve<'queue, 'p, 'd, T, credit::Ready<'p, 'd>, D>, egress::Tickets>,
    > {
        credit::Lane::new(*self)
            .poll(token, amount, target)
            .map(|grant| grant.map(|grant| credit::Reserve::new(*self, grant)))
    }
}

impl<'p, 'd, L: data::raw::Payload<'d>, R: data::raw::Payload<'d>, D: demand::Mode>
    Queue<'_, 'p, 'd, compose::Either<L, R>, Local, D>
{
    pub fn try_left(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        load: egress::NonEmpty<L>,
    ) -> Result<(), error::Capacity<L>> {
        Prepared::prepare(*self, token, load)?.left();
        Ok(())
    }

    pub fn try_right(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        load: egress::NonEmpty<R>,
    ) -> Result<(), error::Capacity<R>> {
        Prepared::prepare(*self, token, load)?.right();
        Ok(())
    }
}

impl<'p, 'd, T> Queue<'_, 'p, 'd, T> {
    pub fn admit(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        load: egress::Load<T>,
    ) -> Admission<T> {
        match load.try_nonempty() {
            Ok(load) => self.admit_load(token, load),
            Err(rejected) => Admission::Rejected(rejected.into()),
        }
    }
}

impl<'p, 'd, T, M, D: demand::Mode> Queue<'_, 'p, 'd, T, M, D> {
    pub(in crate::link::egress) fn admit_load(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        load: egress::NonEmpty<T>,
    ) -> Admission<T>
    where
        M: credit::Access<'p, 'd>,
    {
        let charge = load.charge();
        let resident = load.geometry().resident();
        let amount = credit::Amount::new(egress::Entries::new(1), resident);
        if !self.fits(amount) {
            return Admission::Rejected(egress::Rejected::new(
                Value::Checked(load),
                egress::Rejection::LaneCapacity { charge },
            ));
        }
        match Prepared::prepare(*self, token, load) {
            Ok(ready) => {
                ready.retain();
                Admission::Committed
            }
            Err(error::Capacity { load, cause }) => Admission::Rejected(egress::Rejected::new(
                Value::Checked(load),
                egress::Rejection::Capacity { charge, cause },
            )),
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Lane>() == std::mem::size_of::<u32>());
    assert!(
        std::mem::size_of::<Queue<'static, 'static, 'static, ()>>()
            == 2 * std::mem::size_of::<usize>()
    );
};
