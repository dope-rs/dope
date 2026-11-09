use std::{marker, mem};

use dope_core::driver::route::{Identity, KeyTag};

use crate::client::{
    close,
    conn::{
        self, ClosePhase,
        closing::{Closing, Packed},
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
        self.closing.enter(reason, self.proof)
    }

    pub(in crate::client) fn transmit_failed(
        &mut self,
        report: impl FnOnce(conn::Transmit) -> Option<conn::Transmit>,
    ) {
        if mem::take(&mut self.closing.transmit_pending)
            && let Some(failure) = self.closing.transmit.take()
        {
            self.closing.transmit = report(failure);
        }
    }
}

#[must_use = "restored close phase must be handled"]
pub(in crate::client) enum Stage<'a, 'd, ID: Identity> {
    Started(Starting<'a, 'd, ID>),
    Retiring(Retiring<'a, 'd, ID>),
    Settled(Settled<'a, 'd, ID>),
}

#[must_use = "starting close proof must run quarantine"]
pub(in crate::client) struct Starting<'a, 'd, ID: Identity> {
    state: &'a mut Packed,
    proof: state::Enter<'d, ID>,
}

impl<'a, 'd, ID: Identity> Starting<'a, 'd, ID> {
    pub(super) fn new(state: &'a mut Packed, proof: state::Enter<'d, ID>) -> Self {
        Self { state, proof }
    }

    pub(in crate::client) fn quarantine(
        self,
        run: impl FnOnce(close::Reason),
    ) -> Retiring<'a, 'd, ID> {
        run(self.state.rule.reason());
        *self.state = self.state.with_phase(ClosePhase::Retiring);
        Retiring::new(self.state, self.proof)
    }
}

#[repr(transparent)]
#[must_use = "retiring close proof must be settled"]
pub(in crate::client) struct Retiring<'a, 'd, ID: Identity> {
    state: &'a mut Packed,
    brand: marker::PhantomData<fn(conn::Id<'d, ID>) -> conn::Id<'d, ID>>,
}

impl<'a, 'd, ID: Identity> Retiring<'a, 'd, ID> {
    pub(super) fn new(state: &'a mut Packed, _: state::Enter<'d, ID>) -> Self {
        Self {
            state,
            brand: marker::PhantomData,
        }
    }

    pub(in crate::client) fn reborrow(&mut self) -> conn::CloseRequest<'_, 'd, ID> {
        conn::CloseRequest {
            target: conn::CloseTarget::Retiring(&mut *self.state),
            brand: marker::PhantomData,
        }
    }

    pub(in crate::client) fn reason(&self) -> close::Reason {
        self.state.effective().reason()
    }

    pub(in crate::client) fn settle(self) -> Settled<'a, 'd, ID> {
        *self.state = self.state.with_phase(ClosePhase::Settled);
        Settled::new(self.state)
    }
}

#[repr(transparent)]
pub(in crate::client) struct Settled<'a, 'd, ID: Identity> {
    state: &'a Packed,
    brand: marker::PhantomData<fn(conn::Id<'d, ID>) -> conn::Id<'d, ID>>,
}

impl<'a, 'd, ID: Identity> Settled<'a, 'd, ID> {
    pub(super) fn new(state: &'a Packed) -> Self {
        Self {
            state,
            brand: marker::PhantomData,
        }
    }

    pub(in crate::client) fn reason(&self) -> close::Reason {
        self.state.effective().reason()
    }
}

const _: () = assert!(
    mem::size_of::<Retire<'static, 'static, KeyTag<0>>>() == mem::size_of::<&'static mut Closing>()
);
