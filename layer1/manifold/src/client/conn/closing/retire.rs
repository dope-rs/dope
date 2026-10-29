use std::{marker, mem};

use dope_core::driver::route::{Identity, KeyTag};

use crate::client::{
    close,
    conn::{
        self,
        closing::{Closing, Entry, Quarantine, RetireState, SettledState},
        state,
    },
};

/// Proof to advance one branded connection through retirement.
#[repr(transparent)]
#[must_use = "retirement proof must reach a terminal close phase"]
pub(in crate::client) struct Retire<'a, 'd, ID: Identity> {
    closing: &'a mut Closing,
    proof: state::Enter<'d, ID>,
}

impl<'a, 'd, ID: Identity> Retire<'a, 'd, ID> {
    pub(in crate::client::conn) fn new(
        closing: &'a mut Closing,
        proof: state::Enter<'d, ID>,
    ) -> Self {
        Self { closing, proof }
    }

    pub(in crate::client) fn enter(self, reason: close::Reason) -> Stage<'a, 'd, ID> {
        match self.closing.enter(reason, self.proof) {
            Entry::Started(state, reason, proof) => {
                Stage::Started(Starting::new(state, reason, proof))
            }
            Entry::Retiring(state, proof) => Stage::Retiring(Retiring::new(state, proof)),
            Entry::Settled(state) => Stage::Settled(Settled::new(state)),
            Entry::Finalized => Stage::Finalized,
            Entry::Claimed => Stage::Claimed,
        }
    }
}

#[must_use = "restored close phase must be handled"]
pub(in crate::client) enum Stage<'a, 'd, ID: Identity> {
    Started(Starting<'a, 'd, ID>),
    Retiring(Retiring<'a, 'd, ID>),
    Settled(Settled<'a, 'd, ID>),
    Finalized,
    Claimed,
}

#[must_use = "starting close proof must run quarantine"]
pub(in crate::client) struct Starting<'a, 'd, ID: Identity> {
    state: Quarantine<'a>,
    reason: close::Reason,
    proof: state::Enter<'d, ID>,
}

impl<'a, 'd, ID: Identity> Starting<'a, 'd, ID> {
    fn new(state: Quarantine<'a>, reason: close::Reason, proof: state::Enter<'d, ID>) -> Self {
        Self {
            state,
            reason,
            proof,
        }
    }

    pub(in crate::client) fn quarantine(
        self,
        run: impl FnOnce(close::Reason),
    ) -> Retiring<'a, 'd, ID> {
        let reason = self.reason;
        run(reason);
        Retiring::new(self.state.complete(), self.proof)
    }
}

#[repr(transparent)]
#[must_use = "retiring close proof must be settled"]
pub(in crate::client) struct Retiring<'a, 'd, ID: Identity> {
    state: RetireState<'a>,
    brand: marker::PhantomData<fn(conn::Id<'d, ID>) -> conn::Id<'d, ID>>,
}

impl<'a, 'd, ID: Identity> Retiring<'a, 'd, ID> {
    fn new(state: RetireState<'a>, _: state::Enter<'d, ID>) -> Self {
        Self {
            state,
            brand: marker::PhantomData,
        }
    }

    pub(in crate::client) fn reborrow(&mut self) -> conn::CloseRequest<'_, 'd, ID> {
        conn::CloseRequest {
            target: conn::CloseTarget::Retiring(self.state.reborrow()),
            brand: marker::PhantomData,
        }
    }

    pub(in crate::client) fn reason(&self) -> close::Reason {
        self.state.reason()
    }

    pub(in crate::client) fn settle(self, reason: close::Reason) -> Settled<'a, 'd, ID> {
        Settled::new(self.state.settle(reason))
    }
}

#[repr(transparent)]
pub(in crate::client) struct Settled<'a, 'd, ID: Identity> {
    state: SettledState<'a>,
    brand: marker::PhantomData<fn(conn::Id<'d, ID>) -> conn::Id<'d, ID>>,
}

impl<'a, 'd, ID: Identity> Settled<'a, 'd, ID> {
    fn new(state: SettledState<'a>) -> Self {
        Self {
            state,
            brand: marker::PhantomData,
        }
    }

    pub(in crate::client) fn reason(&self) -> close::Reason {
        self.state.reason()
    }
}

const _: () = assert!(
    mem::size_of::<Retire<'static, 'static, KeyTag<0>>>() == mem::size_of::<&'static mut Closing>()
);
