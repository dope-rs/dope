use std::mem::size_of;

pub(in crate::link::egress) use amount::Amount;
pub(in crate::link::egress) use claim::Claim;
use demand::Demand;
use dope_core::{driver, driver::schedule::maint};
use o3::{
    base::cell::region,
    mem::{credit::fair, raw::Balance},
};
pub(in crate::link::egress) use ready::Ready;
pub(in crate::link::egress) use reserve::Reserve;
pub(in crate::link::egress) use stored::{Part, Prefix, Split, Stored};

use crate::link::{
    egress,
    egress::{
        create,
        data::position,
        load::credit,
        metadata,
        metadata::{arena, lane},
        reserve::{Entries, demand::Mode},
    },
};

mod amount;
mod claim;
mod demand;
mod ready;
mod reserve;
mod stored;
mod wait;

pub(in crate::link::egress) trait Access<'p, 'd>: Sized {
    fn try_acquire<'queue, T, D: Mode, P: position::State>(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        queue: metadata::Queue<'queue, 'p, 'd, T, Self, D, P>,
        amount: credit::Amount,
    ) -> Result<Reserve<'queue, 'p, 'd, T, Self, D, P>, egress::Admission>;

    fn released(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        lane: Lane<'_, 'p, 'd, Self>,
    );
}

impl<'p, 'd> Access<'p, 'd> for metadata::Local {
    fn try_acquire<'queue, T, D: Mode, P: position::State>(
        &self,
        _: &mut region::Token<region::Identity<'d>>,
        queue: metadata::Queue<'queue, 'p, 'd, T, Self, D, P>,
        amount: credit::Amount,
    ) -> Result<Reserve<'queue, 'p, 'd, T, Self, D, P>, egress::Admission> {
        Reserve::try_charge(queue, amount)
    }

    fn released(
        &self,
        _: &mut region::Token<region::Identity<'d>>,
        _: Lane<'_, 'p, 'd, Self>,
    ) {}
}

pub(in crate::link::egress) struct Lane<'a, 'p, 'd, M> {
    pool: &'a credit::Pool<'p, 'd, M>,
    index: metadata::Lane,
}

pub(in crate::link::egress) struct Account<'p, 'd> {
    store: &'p arena::Store<'d>,
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
    pub(in crate::link::egress) const fn new(entries: Entries, resident: egress::Resident) -> Self {
        Self { entries, resident }
    }

    pub(in crate::link::egress) const fn one(resident: egress::Resident) -> Self {
        Self {
            entries: Entries::One,
            resident,
        }
    }

    pub(in crate::link::egress) const fn amount(self) -> credit::Amount {
        credit::Amount::new(self.entries.into_credit(), self.resident)
    }
}

impl<'a, 'p, 'd, M> Lane<'a, 'p, 'd, M> {
    /// Binds the lane already proved by its metadata queue owner.
    pub(in crate::link::egress) fn new<T, D: Mode, P: egress::data::position::State>(
        queue: metadata::Queue<'a, 'p, 'd, T, M, D, P>,
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
            store: self.pool.store,
            state,
        }
    }

    const fn index(self) -> metadata::Lane {
        self.index
    }

    fn is_covered(self, demand: credit::Demand<2>) -> bool {
        demand.fits(credit::Amount::from_raw(self.pool.store.shared.get()))
    }
}

impl<'p, 'd> Account<'p, 'd> {
    pub(in crate::link::egress) fn state(self) -> &'p lane::Lane<'d> {
        self.state
    }

    fn balance(self) -> Balance<2, u32> {
        Balance::new(self.state.held.get(), self.store.shared.get())
    }

    fn commit(self, balance: Balance<2, u32>) {
        let (held, shared) = balance.into_parts();
        self.state.held.set(held);
        self.store.shared.set(shared);
    }

    fn try_acquire(self, amount: credit::Amount) -> Result<(), fair::Cause<u32, 2>> {
        let balance = self.balance().try_acquire(self.state.reserve, amount.raw())?;
        self.commit(balance);
        Ok(())
    }

    fn demand(self, amount: credit::Amount) -> credit::Demand {
        credit::Demand::from_raw(self.balance().demand(self.state.reserve, amount.raw()))
    }

    /// # Safety
    /// The caller retains current shared coverage and the admitted total.
    unsafe fn acquire(self, amount: credit::Amount) {
        let balance = unsafe { self.balance().acquire_raw(self.state.reserve, amount.raw()) };
        self.commit(balance);
    }

    /// # Safety
    /// The consuming owner returns a disjoint acquisition from this exact epoch.
    pub(in crate::link::egress) unsafe fn release(self, amount: credit::Amount) {
        let balance = unsafe { self.balance().release_raw(self.state.reserve, amount.raw()) };
        self.commit(balance);
    }

    pub(in crate::link::egress) fn fits(self, amount: credit::Amount) -> bool {
        let amount = amount.raw();
        let shared = self.store.config.shared_limit();
        (0..2).all(|axis| amount[axis] <= shared[axis] + self.state.reserve[axis])
    }

    pub(in crate::link::egress) fn can_charge(self, amount: credit::Amount) -> bool {
        self.balance()
            .try_acquire(self.state.reserve, amount.raw())
            .is_ok()
    }
}

impl<'p, 'd, M: Access<'p, 'd>> Lane<'_, 'p, 'd, M> {
    pub(in crate::link::egress) fn released(self, token: &mut region::Token<region::Identity<'d>>) {
        self.pool.mode.released(token, self);
    }
}

impl<'p, 'd> Lane<'_, 'p, 'd, credit::Ready<'p, 'd>> {
    pub(in crate::link::egress) fn cancel(self, token: &mut region::Token<region::Identity<'d>>) {
        self.pool.mode.cancel(token, self)
    }
}

const _: () = {
    assert!(size_of::<Pair>() == size_of::<egress::Resident>());
    assert!(size_of::<Queued>() == size_of::<credit::Amount>());
    assert!(size_of::<Account<'static, 'static>>() == 2 * size_of::<usize>());
    assert!(
        size_of::<Lane<'static, 'static, 'static, metadata::Local>>() == 2 * size_of::<usize>()
    );
};

pub(in crate::link::egress) struct Pool<'p, 'd, M = metadata::Local> {
    store: &'p arena::Store<'d>,
    lanes: &'p [lane::Lane<'d>],
    pub(in crate::link::egress) mode: M,
}

impl<'p, 'd> Pool<'p, 'd, metadata::Local> {
    pub(in crate::link::egress) fn new(slots: &'p arena::Store<'d>) -> Self {
        Self {
            store: slots,
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
                let shared = credit::Amount::from_raw(self.store.shared.get());
                let Some(selected) = wait.select(self, shared) else {
                    return;
                };
                selected.activate(self.mode.owner);
                self.mode.pending.set(wait.has_waiting());
            });
        }
    }
    pub(in crate::link::egress) fn try_driver(
        token: &region::Token<region::Identity<'d>>,
        slots: &'p arena::Store<'d>,
        owner: driver::Reference<'d>,
    ) -> Result<Self, create::Error> {
        Ok(Self {
            store: slots,
            lanes: &slots.lanes,
            mode: credit::Ready::try_new(token, owner, slots.count)?,
        })
    }
}

const _: () = {
    assert!(size_of::<metadata::Local>() == 0);
    assert!(
        size_of::<Pool<'static, 'static>>()
            == size_of::<(&arena::Store<'static>, &[lane::Lane<'static>])>()
    );
    assert!(
        size_of::<Pool<'static, 'static, credit::Ready<'static, 'static>>>()
            == size_of::<Pool<'static, 'static>>() + size_of::<credit::Ready<'static, 'static>>()
    );
};
