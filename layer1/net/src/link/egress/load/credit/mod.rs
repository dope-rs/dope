use std::{mem::size_of, task};

pub(in crate::link::egress) use amount::Amount;
pub(in crate::link::egress) use claim::Claim;
use demand::Demand;
use dope_core::{
    driver,
    driver::schedule::{maint, ready::Target},
};
use o3::{base::cell::region, mem, mem::raw::Source as _};
pub(in crate::link::egress) use ready::Ready;
pub(in crate::link::egress) use reserve::Reserve;
pub(in crate::link::egress) use stored::{Bound, Part, Prefix, Split, Stored};

use crate::link::{
    egress,
    egress::{
        create,
        load::credit,
        metadata,
        metadata::{arena, lane},
        reserve::{Bounds, Entries, demand::Mode},
    },
};

mod amount;
mod claim;
mod demand;
mod ready;
mod reserve;
mod stored;
mod wait;

pub(in crate::link::egress) enum Tag {}

pub(in crate::link::egress) trait Access<'p, 'd>: Sized {
    fn try_acquire(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        lane: Lane<'_, 'p, 'd, Self>,
        amount: credit::Amount,
    ) -> Result<Grant<'p>, egress::Admission>;

    fn release(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        lane: Lane<'_, 'p, 'd, Self>,
        grant: Grant<'p>,
    );
}

pub(in crate::link::egress) struct Lane<'a, 'p, 'd, M> {
    pool: &'a credit::Pool<'p, 'd, M>,
    index: metadata::Lane,
}

pub(in crate::link::egress) struct Account<'p, 'd> {
    pool: &'p mem::raw::Pool<2, credit::Tag, u32>,
    state: &'p lane::Lane<'d>,
}

impl Clone for Account<'_, '_> {
    fn clone(&self) -> Self {
        *self
    }
}

impl Copy for Account<'_, '_> {}

impl<'p, 'd, M> Clone for Lane<'_, 'p, 'd, M> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'p, 'd, M> Copy for Lane<'_, 'p, 'd, M> {}

pub(in crate::link::egress) struct Grant<'p>(
    mem::raw::Grant<mem::raw::Lane<'p, 'p, 2, credit::Tag, u32>, 2, credit::Amount>,
);

pub(in crate::link::egress) struct Pair {
    first: egress::Resident,
}

impl Pair {
    pub(in crate::link::egress) const fn new(first: egress::Resident) -> Self {
        Self { first }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::link::egress) struct Queued {
    entries: Entries,
    resident: egress::Resident,
}

impl Queued {
    pub(in crate::link::egress) const fn one(resident: egress::Resident) -> Self {
        Self {
            entries: Entries::One,
            resident,
        }
    }

    pub(in crate::link::egress) const fn from_bounds(bounds: Bounds) -> Self {
        Self {
            entries: bounds.entries(),
            resident: bounds.resident(),
        }
    }

    const fn amount(self) -> credit::Amount {
        credit::Amount::new(self.entries.into_credit(), self.resident)
    }
}

// SAFETY: the caller invokes this marker only for the bounds that minted it.
unsafe impl mem::raw::Exact<2, u32, credit::Amount> for Pair {
    type Input = ();
    type Left = credit::Amount;
    type Right = credit::Amount;

    fn parts(self, source: credit::Amount, (): ()) -> (Self::Left, Self::Right) {
        let first = credit::Amount::new(egress::Entries::new(1), self.first);
        (source.sub(first), first)
    }
}

impl<'p> Grant<'p> {
    pub(in crate::link::egress) fn release(self) {
        self.0.release();
    }

    fn from(
        charge: mem::raw::Charge<mem::raw::Lane<'p, 'p, 2, credit::Tag, u32>, 2, credit::Amount>,
    ) -> Self {
        Self(mem::raw::Grant::from(charge))
    }

    fn amount(&self) -> credit::Amount {
        self.0.amount()
    }

    fn split_pair(&mut self, pair: Pair) -> Self {
        // SAFETY: Pair is the two active bounds that minted this aggregate grant.
        Self(unsafe { self.0.bind_mut(pair) }.split(()))
    }
}

impl<'a, 'p, 'd, M> Lane<'a, 'p, 'd, M> {
    /// Binds the lane already proved by its metadata queue owner.
    pub(in crate::link::egress) fn new<T, D: Mode>(
        queue: metadata::Queue<'a, 'p, 'd, T, M, D>,
    ) -> Self {
        Self {
            pool: &queue.pool.credit,
            index: queue.lane,
        }
    }

    pub(in crate::link::egress) fn account(self) -> credit::Account<'p, 'd> {
        // SAFETY: Lane is minted only from its pool's metadata queue or a
        // candidate selected from that same pool's wait table.
        let state = unsafe {
            self.pool
                .lanes
                .get_unchecked(self.index.index().into_usize())
        };
        credit::Account {
            pool: self.pool.shared,
            state,
        }
    }

    pub(in crate::link::egress) fn try_charge(
        self,
        amount: credit::Amount,
    ) -> Result<credit::Grant<'p>, egress::Admission> {
        match self.account().credit().try_acquire(amount) {
            Ok(grant) => Ok(Grant::from(grant)),
            Err(rejected) => {
                let (amount, cause) = rejected.into_parts();
                Err(egress::Admission::Credit {
                    lane: self.index,
                    requested: egress::Credit::new(amount.entries(), amount.resident()),
                    cause,
                })
            }
        }
    }

    const fn index(self) -> metadata::Lane {
        self.index
    }

    fn is_covered(self, demand: credit::Demand<2>) -> bool {
        demand.fits(credit::Amount::from_raw(self.pool.shared.shared()))
    }
}

impl<'p, 'd> Account<'p, 'd> {
    fn credit(self) -> mem::raw::Lane<'p, 'p, 2, credit::Tag, u32> {
        // SAFETY: Account retains the state and pool selected from one exact owner.
        unsafe { self.pool.lane(&self.state.credit) }
    }

    pub(in crate::link::egress) fn state(self) -> &'p lane::Lane<'d> {
        self.state
    }

    fn demand(self, amount: credit::Amount) -> credit::Demand {
        credit::Demand::from_raw(self.credit().demand(mem::raw::Amount::get(amount)))
    }

    fn acquire(self, amount: credit::Amount) -> credit::Grant<'p> {
        // SAFETY: Ready's zero demand, Vacant's zero/covered branch, and the
        // eligible Selected capability prove current shared coverage. The
        // unchanged lane and conserved pool bound held plus amount by the
        // admitted protected-plus-shared capacity.
        Grant::from(unsafe { self.credit().acquire_raw(amount) })
    }

    pub(in crate::link::egress) fn fits(self, amount: credit::Amount) -> bool {
        self.credit().fits(mem::raw::Amount::get(amount))
    }

    pub(in crate::link::egress) fn can_charge(self, amount: credit::Amount) -> bool {
        self.credit().can_acquire(mem::raw::Amount::get(amount))
    }
}

impl<'p, 'd, M: Access<'p, 'd>> Lane<'_, 'p, 'd, M> {
    pub(in crate::link::egress) fn try_acquire(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        amount: credit::Amount,
    ) -> Result<credit::Grant<'p>, egress::Admission> {
        self.pool.mode.try_acquire(token, self, amount)
    }

    pub(in crate::link::egress) fn release(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        grant: credit::Grant<'p>,
    ) {
        self.pool.mode.release(token, self, grant);
    }
}

impl<'a, 'p, 'd> Lane<'a, 'p, 'd, credit::Ready<'p, 'd>> {
    pub(in crate::link::egress) fn cancel(self, token: &mut region::Token<region::Identity<'d>>) {
        self.pool.mode.cancel(token, self)
    }

    pub(in crate::link::egress) fn poll(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        amount: credit::Queued,
        target: Target<'d>,
    ) -> task::Poll<Result<credit::Grant<'p>, egress::Tickets>> {
        self.pool.mode.poll(token, self, amount, target)
    }
}

const _: () = {
    assert!(size_of::<Grant>() == 3 * size_of::<u64>());
    assert!(size_of::<Pair>() == size_of::<egress::Resident>());
    assert!(size_of::<Queued>() == size_of::<credit::Amount>());
    assert!(size_of::<Account<'static, 'static>>() == 2 * size_of::<usize>());
    assert!(
        size_of::<Lane<'static, 'static, 'static, metadata::Local>>() == 2 * size_of::<usize>()
    );
};

pub(in crate::link::egress) struct Pool<'p, 'd, M = metadata::Local> {
    shared: &'p mem::raw::Pool<2, credit::Tag, u32>,
    lanes: &'p [lane::Lane<'d>],
    mode: M,
}

impl<'p, 'd> Pool<'p, 'd, metadata::Local> {
    pub(in crate::link::egress) fn new(slots: &'p arena::Store<'d>) -> Self {
        Self {
            shared: &slots.shared,
            lanes: &slots.lanes,
            mode: metadata::Local,
        }
    }
}

impl<'p, 'd> Pool<'p, 'd, credit::Ready<'p, 'd>> {
    pub(in crate::link::egress) fn has_work(&self) -> bool {
        self.mode.pending.get()
    }

    /// Drains queued selections with one maintenance permit per attempt,
    /// including stale activations. The installed pool owner observes pending
    /// work before parking, independently of lanes.
    pub(in crate::link::egress) fn maintain(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        work: maint::Work<'_, 'd>,
    ) {
        while self.has_work() {
            let Some(permit) = work.permit() else {
                return;
            };
            permit.run(|| {
                self.mode.pending.set(false);
                let wait = token.borrow_mut(&self.mode.wait);
                let shared = credit::Amount::from_raw(self.shared.shared());
                let selected = match wait.select(self, shared) {
                    wait::Selection::Selected(selected) => selected,
                    wait::Selection::NoWaiting
                    | wait::Selection::Insufficient
                    | wait::Selection::BypassExhausted => return,
                };
                let result = selected.activate(self.mode.owner);
                self.mode.pending.set(wait.has_waiting());
                if let wait::Activation::Stale(grant) = result {
                    grant.release();
                }
            });
        }
    }
    pub(in crate::link::egress) fn try_driver(
        token: &region::Token<region::Identity<'d>>,
        slots: &'p arena::Store<'d>,
        owner: driver::Reference<'d>,
    ) -> Result<Self, create::Error> {
        Ok(Self {
            shared: &slots.shared,
            lanes: &slots.lanes,
            mode: credit::Ready::try_new(token, owner, slots.count)?,
        })
    }
}

const _: () = {
    assert!(size_of::<metadata::Local>() == 0);
    assert!(
        size_of::<Pool<'static, 'static>>()
            == size_of::<(&mem::raw::Pool<2, credit::Tag, u32>, &[lane::Lane<'static>])>()
    );
    assert!(
        size_of::<Pool<'static, 'static, credit::Ready<'static, 'static>>>()
            == size_of::<Pool<'static, 'static>>() + size_of::<credit::Ready<'static, 'static>>()
    );
};
