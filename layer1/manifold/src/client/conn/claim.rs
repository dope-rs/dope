use dope_core::driver::route::Identity;
use dope_net::wire;

use crate::client::{
    close,
    conn::{self, closing, drain},
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::client) enum Transport {
    Preserved,
    Discarded,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Urgency {
    Deferred,
    Immediate,
}

impl Urgency {
    pub(super) fn finalize<'d, ID: Identity, C, O, W>(
        self,
        slot: &mut conn::Slot<'_, 'd, ID, W, C, O>,
        claim: closing::Claim,
    ) -> drain::Drain<'d, ID>
    where
        W: wire::Spec + 'd,
    {
        let close = drain::Drain::new(conn::Id::from_key(slot.key()), claim);
        if matches!(self, Self::Immediate) {
            slot.discard();
        }
        close
    }
}

#[must_use = "forced close claim must distinguish an active drain owner"]
pub(in crate::client) enum CutClaim<'d, ID: Identity> {
    Ready(drain::Drain<'d, ID>),
    Owned,
}

impl<'d, ID: Identity> CutClaim<'d, ID> {
    pub(in crate::client) fn request<C, O, W>(
        slot: &mut conn::Slot<'_, 'd, ID, W, C, O>,
        rule: close::Directive,
    ) -> Self
    where
        W: wire::Spec + 'd,
    {
        match slot.state.closing.cut(rule) {
            closing::CutState::Issued(claim, urgency) => Self::Ready(urgency.finalize(slot, claim)),
            closing::CutState::Owned(urgency) => {
                if matches!(urgency, Urgency::Immediate) {
                    slot.discard();
                }
                Self::Owned
            }
        }
    }
}
