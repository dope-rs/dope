use dope_core::driver::{route::Identity, schedule::ready};
use dope_net::wire;

use crate::client::{
    attempt, close,
    conn::{self, closing, drain, state},
    side,
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
    fn finalize<'d, ID: Identity, C, O, W>(
        self,
        slot: &mut conn::Slot<'_, 'd, ID, W, C, O>,
        claim: closing::Claim,
    ) -> state::Finalized<'d, ID>
    where
        W: wire::Spec + 'd,
    {
        let close = state::Finalized::new(conn::Id::from_key(slot.key()), claim);
        if matches!(self, Self::Immediate) {
            slot.discard();
        }
        close
    }
}

#[must_use = "settled close claim must preserve a waiting owner"]
pub(in crate::client) enum SettledClaim<'d, ID: Identity> {
    Ready(state::Finalized<'d, ID>),
    Active,
    Waiting,
    Missing,
}

#[must_use = "owned settled close claim must preserve its exact attempt"]
pub(in crate::client) struct Settlement<'d, ID: Identity>(Owned<'d, ID>);

enum Owned<'d, ID: Identity> {
    Ready(drain::OwnedDrain<'d, ID>),
    Active,
    Waiting(ready::Target<'d>),
    Missing(attempt::Id<'d, ID>),
    Absent,
}

#[must_use = "forced close claim must distinguish an active drain owner"]
pub(in crate::client) enum CutClaim<'d, ID: Identity> {
    Ready(state::Finalized<'d, ID>),
    Owned,
}

#[must_use = "application close probe must preserve every close phase"]
pub(in crate::client) enum ProbeClaim<'d, ID: Identity> {
    Ready(state::Finalized<'d, ID>),
    Active,
    InProgress,
    Missing,
}

impl<'d, ID: Identity> ProbeClaim<'d, ID> {
    pub(in crate::client) fn claim<C, O, W>(slot: &mut conn::Slot<'_, 'd, ID, W, C, O>) -> Self
    where
        W: wire::Spec + 'd,
    {
        let claim = slot.state.closing.claim_final();
        match claim {
            closing::ClaimState::Issued(claim) => {
                Self::Ready(state::Finalized::new(conn::Id::from_key(slot.key()), claim))
            }
            closing::ClaimState::Active => Self::Active,
            closing::ClaimState::InProgress => Self::InProgress,
            closing::ClaimState::Missing => Self::Missing,
        }
    }
}

impl<'d, ID: Identity> SettledClaim<'d, ID> {
    pub(in crate::client) fn claim<C, O, W>(slot: &mut conn::Slot<'_, 'd, ID, W, C, O>) -> Self
    where
        W: wire::Spec + 'd,
    {
        match slot.state.closing.settle() {
            closing::SettleState::Issued(claim, urgency) => {
                Self::Ready(urgency.finalize(slot, claim))
            }
            closing::SettleState::Waiting => Self::Waiting,
            closing::SettleState::Active => Self::Active,
            closing::SettleState::Missing => Self::Missing,
        }
    }
}

impl<'d, ID: Identity> Settlement<'d, ID> {
    pub(in crate::client) fn claim<C, W>(
        slot: Option<&mut conn::Slot<'_, 'd, ID, W, C, side::Primary<'d, ID>>>,
    ) -> Self
    where
        W: wire::Spec + 'd,
    {
        let Some(slot) = slot else {
            return Self(Owned::Absent);
        };
        let owner = slot.state.owner.attempt();
        let target = slot.io().wake_target();
        Self(match SettledClaim::claim(slot) {
            SettledClaim::Ready(close) => {
                Owned::Ready(drain::OwnedDrain::new(owner, close.into_drain()))
            }
            SettledClaim::Active => Owned::Active,
            SettledClaim::Waiting => Owned::Waiting(target),
            SettledClaim::Missing => Owned::Missing(owner),
        })
    }

    pub(in crate::client) fn drain(
        self,
        missing: impl FnOnce(attempt::Id<'d, ID>),
    ) -> Option<drain::OwnedDrain<'d, ID>> {
        match self.0 {
            Owned::Ready(drain) => Some(drain),
            Owned::Active | Owned::Absent => None,
            Owned::Waiting(target) => {
                target.wake();
                None
            }
            Owned::Missing(owner) => {
                missing(owner);
                None
            }
        }
    }
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
