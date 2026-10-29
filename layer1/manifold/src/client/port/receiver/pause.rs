use dope_net::link::egress::{data, metadata::arena};
use o3::base::cell::region;

use crate::{
    app::dispatch::identity,
    client::port::{receiver::head::Head, state},
};

/// One exact paused source lane and connection generation.
pub(super) struct Pause<'entry, 'e, 'd, S, I: identity::Identity> {
    slot: arena::Slot<'entry, 'e, 'd, S, state::Entry<'d, I>>,
    transaction: state::Transaction<I>,
}

impl<'entry, 'e, 'd, S: data::raw::Payload<'d>, I: identity::Identity> Pause<'entry, 'e, 'd, S, I> {
    pub(super) fn begin(
        slot: arena::Slot<'entry, 'e, 'd, S, state::Entry<'d, I>>,
        txn: state::Transaction<I>,
    ) -> Option<Self> {
        match slot.state().suspend(txn) {
            state::Suspension::Suspended => Some(Self {
                slot,
                transaction: txn,
            }),
            state::Suspension::Stale => None,
        }
    }

    pub(super) fn head<'source, K>(
        &'source self,
        token: &region::Token<region::Identity<'d>>,
        inspect: impl FnOnce(&S) -> K,
    ) -> Option<Head<'source, 'entry, 'e, 'd, S, K, I>> {
        let kind = self.slot.queue().inspect(token, inspect)?;
        Some(Head::new(self, kind))
    }

    pub(super) fn front(&self) -> Option<Head<'_, 'entry, 'e, 'd, S, (), I>> {
        if self.slot.queue().is_empty() {
            return None;
        }
        Some(Head::new(self, ()))
    }

    pub(super) fn is_current(&self) -> bool {
        self.slot.state().is_suspended(self.transaction)
    }

    pub(super) fn slot(&self) -> arena::Slot<'entry, 'e, 'd, S, state::Entry<'d, I>> {
        self.slot
    }
}

impl<'e, S, I: identity::Identity> Drop for Pause<'_, 'e, '_, S, I> {
    fn drop(&mut self) {
        self.slot.state().restore(self.transaction);
    }
}
