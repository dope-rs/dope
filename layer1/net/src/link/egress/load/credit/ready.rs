use std::{cell, mem, task};

use dope_core::driver::{self, schedule::ready};
use o3::base::cell::region;

use crate::link::egress::{
    self, create,
    load::credit::{self, wait},
    metadata,
};

pub struct Ready<'p, 'd> {
    pub(super) owner: driver::Reference<'d>,
    pub(super) wait: region::Value<region::Identity<'d>, wait::Queue<'p, 'd>>,
    pub(super) pending: cell::Cell<bool>,
}

impl<'p, 'd> Ready<'p, 'd> {
    pub(super) fn try_new(
        token: &region::Token<region::Identity<'d>>,
        owner: driver::Reference<'d>,
        lanes: metadata::LaneCount,
    ) -> Result<Self, create::Error> {
        Ok(Self {
            owner,
            wait: token.bind(wait::Queue::try_new(lanes)?),
            pending: cell::Cell::new(false),
        })
    }

    pub(super) fn cancel(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        lane: credit::Lane<'_, 'p, 'd, Self>,
    ) {
        let wait = token.borrow_mut(&self.wait);
        if let wait::Cancelled::Busy = wait.cancel(lane) {
            self.pending.set(wait.has_waiting());
        }
    }
}

impl<'p, 'd> credit::Access<'p, 'd> for Ready<'p, 'd> {
    fn try_acquire(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        lane: credit::Lane<'_, 'p, 'd, Self>,
        amount: credit::Amount,
    ) -> Result<credit::Grant<'p>, egress::Admission> {
        let index = lane.index();
        let wait = token.borrow(&self.wait);
        if wait.is_busy(lane) {
            return Err(egress::Admission::Busy {
                lane: index,
                requested: egress::Credit::new(amount.entries(), amount.resident()),
            });
        }
        if wait.has_waiting() {
            let account = lane.account();
            let demand = account.demand(amount);
            if demand != credit::Demand::ZERO {
                return Err(egress::Admission::Queued {
                    lane: index,
                    requested: egress::Credit::new(amount.entries(), amount.resident()),
                    demand: egress::Credit::new(demand.entries(), demand.resident()),
                });
            }
            return Ok(account.acquire(amount));
        }
        lane.try_charge(amount)
    }

    fn release(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        lane: credit::Lane<'_, 'p, 'd, Self>,
        grant: credit::Grant<'p>,
    ) {
        grant.release();
        let wait = token.borrow_mut(&self.wait);
        wait.refresh(lane);
        self.pending.set(wait.has_waiting());
    }
}

impl<'p, 'd> Ready<'p, 'd> {
    /// Publishes refunded credit even when replacing a grant exhausts tickets.
    pub(super) fn poll(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        lane: credit::Lane<'_, 'p, 'd, Self>,
        queued: credit::Queued,
        target: ready::Target<'d>,
    ) -> task::Poll<Result<credit::Grant<'p>, egress::Tickets>> {
        let key = target.key();
        let wait = token.borrow_mut(&self.wait);
        let state = wait.resume(lane, key, queued);
        if !matches!(state, Ok(wait::Resume::Waiting)) {
            self.pending.set(wait.has_waiting());
        }
        match state? {
            wait::Resume::Waiting | wait::Resume::Queued => task::Poll::Pending,
            wait::Resume::Granted(grant) => task::Poll::Ready(Ok(grant)),
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<Ready<'static, 'static>>()
            == mem::size_of::<driver::Reference<'static>>()
                + mem::size_of::<
                    region::Value<region::Identity<'static>, wait::Queue<'static, 'static>>,
                >()
                + mem::size_of::<usize>()
    );
};
