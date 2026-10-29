pub mod raw;

use dope_core::driver::route::Identity;
use send::Store as _;

use crate::{
    link::slot::{transmit::outcome, types},
    wire::{self, send},
};

pub struct Status<'a, 'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w, S> {
    slot: &'a types::Connection<'w, 'd, ID, W, S>,
}

impl<'a, 'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w, S> Status<'a, 'w, 'd, ID, W, S> {
    pub(in crate::link::slot) fn new(slot: &'a types::Connection<'w, 'd, ID, W, S>) -> Self {
        Self { slot }
    }

    pub fn is_inflight(&self) -> bool {
        self.slot.engine.operations.sending.is_inflight()
    }

    pub fn is_waiting(&self) -> bool {
        self.slot.engine.operations.lifecycle.is_waiting()
    }

    pub fn retention(&self) -> outcome::Retention {
        if !self.slot.send.as_slice().is_empty() {
            outcome::Retention::Held
        } else {
            outcome::Retention::Clear
        }
    }

    pub fn is_closing(&self) -> bool {
        self.slot.engine.operations.lifecycle.close.is_shutting()
    }

    pub fn is_closed(&self) -> bool {
        self.slot.engine.operations.lifecycle.close.has_shutdown()
    }
}
