use std::mem;

use dope_core::driver::{route::Identity, schedule::ready};

mod accesses;
pub(in crate::client) mod retire;
pub(super) use accesses::RetireRef;
use retire::{Retiring, Settled, Stage, Starting};

use crate::client::{
    close,
    conn::{self, ClosePhase, CloseStatus, CloseTarget, claim, drain, state},
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Packed {
    phase: ClosePhase,
    rule: close::Directive,
    transport: claim::Transport,
    deferred: bool,
}

impl Packed {
    const fn new(phase: ClosePhase, rule: close::Directive) -> Self {
        Self {
            phase,
            rule,
            transport: claim::Transport::Preserved,
            deferred: false,
        }
    }

    const fn with(self, rule: close::Directive) -> Self {
        Self { rule, ..self }
    }

    const fn with_phase(self, phase: ClosePhase) -> Self {
        Self { phase, ..self }
    }

    pub(super) const fn effective(&self) -> close::Directive {
        self.rule
    }

    fn merged(self, rule: close::Directive) -> Self {
        self.with(self.effective().merge(rule))
    }

    fn defer(&mut self, rule: close::Directive) {
        self.rule = self.rule.merge(rule);
        self.deferred = true;
    }

    fn observe(&mut self, reason: close::Reason) {
        self.rule = close::Directive::with_policy(reason, self.rule.policy());
        self.deferred = true;
    }

    fn issue(&mut self) -> Claim {
        self.phase = ClosePhase::Claimed;
        self.deferred = false;
        Claim(())
    }

    pub(super) fn claim_final(&mut self) -> ClaimState {
        match self.phase {
            ClosePhase::Claimed => ClaimState::Active,
            ClosePhase::Finalized => ClaimState::Issued(self.issue()),
            _ => ClaimState::InProgress,
        }
    }

    fn restore(&mut self) -> drain::Resume {
        if mem::take(&mut self.deferred) {
            // Claims follow completed retirement or an unattached opening.
            // Reevaluate deferred policy without restarting retirement.
            self.phase = ClosePhase::Settled;
            drain::Resume::Pending
        } else {
            self.phase = ClosePhase::Finalized;
            drain::Resume::Restored
        }
    }

    fn take_urgency(&mut self) -> claim::Urgency {
        if matches!(self.transport, claim::Transport::Discarded) {
            return claim::Urgency::Deferred;
        }
        let rule = self.effective();
        match rule.policy() {
            close::Policy::Reconnect | close::Policy::Permanent => {
                return claim::Urgency::Deferred;
            }
            close::Policy::ImmediateReconnect | close::Policy::ImmediatePermanent => {}
        }
        self.rule = rule.resume();
        self.deferred |= matches!(self.phase, ClosePhase::Claimed);
        self.transport = claim::Transport::Discarded;
        claim::Urgency::Immediate
    }

    fn finalize(&mut self) -> (Claim, claim::Urgency) {
        let urgency = match self.rule.policy() {
            close::Policy::ImmediateReconnect | close::Policy::ImmediatePermanent => {
                match self.phase {
                    ClosePhase::Withdrawing
                    | ClosePhase::Requested
                    | ClosePhase::Quarantining
                    | ClosePhase::Retiring
                    | ClosePhase::Settled => {
                        self.rule = self.rule.resume();
                        self.transport = claim::Transport::Discarded;
                        claim::Urgency::Immediate
                    }
                    ClosePhase::Finalized | ClosePhase::Claimed => claim::Urgency::Deferred,
                }
            }
            close::Policy::Reconnect | close::Policy::Permanent => claim::Urgency::Deferred,
        };
        (self.issue(), urgency)
    }

    fn apply(&mut self, rule: close::Directive) -> &mut Self {
        match self.phase {
            ClosePhase::Withdrawing => *self = self.with(rule).with_phase(ClosePhase::Requested),
            ClosePhase::Requested
            | ClosePhase::Quarantining
            | ClosePhase::Retiring
            | ClosePhase::Settled
            | ClosePhase::Finalized => *self = self.merged(rule),
            ClosePhase::Claimed => self.defer(rule),
        }
        self
    }

    pub(super) const fn is_claimed(&self) -> bool {
        matches!(self.phase, ClosePhase::Claimed)
    }

    pub(super) const fn status(self) -> CloseStatus {
        CloseStatus::Closing(self.phase, self.effective())
    }
}

#[derive(Default)]
pub(super) struct Closing {
    state: Option<Packed>,
    pub(super) transmit: Option<conn::Transmit>,
    pub(super) transmit_pending: bool,
}

pub(super) enum Callback {
    Ready,
    Sealed,
}

impl Callback {
    pub(super) fn new(closing: &Closing) -> Self {
        match closing.state {
            None => Self::Ready,
            Some(state) => match state.phase {
                ClosePhase::Withdrawing
                | ClosePhase::Requested
                | ClosePhase::Quarantining
                | ClosePhase::Retiring
                | ClosePhase::Settled => Self::Ready,
                ClosePhase::Finalized | ClosePhase::Claimed => Self::Sealed,
            },
        }
    }
}

pub(in crate::client::conn) struct Claim(());

pub(super) enum CutState {
    Issued(Claim, claim::Urgency),
    Owned(claim::Urgency),
}

pub(super) enum ClaimState {
    Issued(Claim),
    Active,
    InProgress,
}

impl Closing {
    pub(super) fn withdraw(&mut self) -> bool {
        if self.state.is_some() {
            return false;
        }
        self.state = Some(Packed::new(
            ClosePhase::Withdrawing,
            close::Directive::reconnect(close::Reason::EndpointRetired),
        ));
        true
    }

    pub(super) fn is_withdrawing(&self) -> bool {
        matches!(
            self.state,
            Some(Packed {
                phase: ClosePhase::Withdrawing,
                ..
            })
        )
    }

    pub(in crate::client::conn) fn complete(
        &mut self,
        rule: close::Directive,
    ) -> (Claim, claim::Urgency, claim::Urgency) {
        let state = self.apply(rule);
        // The admitted active callback cannot claim or finalize this exclusive close state.
        *state = state.with_phase(ClosePhase::Settled);
        // Preserve discard activation on each side of the caller's deadline update.
        let before = state.take_urgency();
        let (claim, after) = state.finalize();
        (claim, before, after)
    }

    pub(super) fn apply(&mut self, rule: close::Directive) -> &mut Packed {
        match &mut self.state {
            Some(state) => state.apply(rule),
            slot @ None => slot.insert(Packed::new(ClosePhase::Requested, rule)),
        }
    }

    pub(super) fn failed(&mut self, reason: close::Reason) {
        match &mut self.state {
            None => {
                self.state = Some(Packed::new(
                    ClosePhase::Requested,
                    close::Directive::reconnect(reason),
                ));
            }
            Some(state) => match state.phase {
                ClosePhase::Withdrawing => {
                    *state = state
                        .with(close::Directive::reconnect(reason))
                        .with_phase(ClosePhase::Requested);
                }
                ClosePhase::Requested
                | ClosePhase::Quarantining
                | ClosePhase::Retiring
                | ClosePhase::Settled
                | ClosePhase::Finalized => {
                    *state = state.with(close::Directive::with_policy(
                        reason,
                        state.effective().policy(),
                    ));
                }
                ClosePhase::Claimed => {
                    state.observe(reason);
                }
            },
        }
    }

    pub(super) fn take_urgency(&mut self) -> claim::Urgency {
        self.state
            .as_mut()
            .map_or(claim::Urgency::Deferred, Packed::take_urgency)
    }

    pub(super) fn terminal<T>(
        &mut self,
        ready: ready::Target<'_>,
        callback: impl FnOnce(CloseTarget<'_>) -> Option<T>,
    ) -> Option<(Option<T>, claim::Urgency)> {
        match &mut self.state {
            Some(state) if matches!(state.phase, ClosePhase::Claimed) => None,
            Some(state) if matches!(state.phase, ClosePhase::Finalized) => {
                state.issue();
                let output = callback(CloseTarget::Retiring(RetireRef(state)));
                let urgency = match output {
                    Some(_) => state.take_urgency(),
                    None => claim::Urgency::Deferred,
                };
                match state.restore() {
                    drain::Resume::Restored => {}
                    drain::Resume::Pending => ready.wake(),
                }
                Some((output, urgency))
            }
            Some(_) | None => {
                let output = callback(CloseTarget::Open(self));
                let urgency = match output {
                    Some(_) => self.take_urgency(),
                    None => claim::Urgency::Deferred,
                };
                Some((output, urgency))
            }
        }
    }

    pub(super) fn enter<'d, ID: Identity>(
        &mut self,
        reason: close::Reason,
        proof: state::Enter<'d, ID>,
    ) -> Stage<'_, 'd, ID> {
        let rule = close::Directive::reconnect(reason);
        let state = self
            .state
            .get_or_insert_with(|| Packed::new(ClosePhase::Quarantining, rule));
        match state.phase {
            // Enter comes only from an admitted ActiveMut, which excludes these phases.
            ClosePhase::Claimed | ClosePhase::Finalized => unreachable!("close"),
            ClosePhase::Settled => return Stage::Settled(Settled::new(state)),
            ClosePhase::Retiring => {
                return Stage::Retiring(Retiring::new(state, proof));
            }
            ClosePhase::Quarantining => {}
            ClosePhase::Withdrawing | ClosePhase::Requested => {
                *state = state.merged(rule).with_phase(ClosePhase::Quarantining);
            }
        }
        Stage::Started(Starting::new(state, proof))
    }

    pub(super) fn cut(&mut self, rule: close::Directive) -> CutState {
        let state = self.apply(rule);
        if matches!(state.phase, ClosePhase::Claimed) {
            CutState::Owned(self.take_urgency())
        } else {
            let (claim, urgency) = state.finalize();
            CutState::Issued(claim, urgency)
        }
    }

    pub(super) const fn is_closing(&self) -> bool {
        !self.status().is_receiving()
    }

    pub(super) const fn status(&self) -> CloseStatus {
        match self.state {
            Some(state) => state.status(),
            None => CloseStatus::Open,
        }
    }

    pub(super) fn reason(&self) -> Option<close::Reason> {
        self.state.map(|state| state.effective().reason())
    }

    pub(super) fn restore(&mut self) -> drain::Restore {
        match &mut self.state {
            Some(state) if matches!(state.phase, ClosePhase::Claimed) => {
                drain::Restore::Current(state.restore())
            }
            Some(_) | None => drain::Restore::Stale,
        }
    }

    pub(super) fn authorize(&self) -> Option<claim::Transport> {
        match self.state {
            Some(state) if matches!(state.phase, ClosePhase::Claimed) => Some(state.transport),
            Some(_) | None => None,
        }
    }

    pub(super) fn commit(&self) -> Option<close::Directive> {
        match self.state {
            Some(state) if matches!(state.phase, ClosePhase::Claimed) => Some(state.rule),
            Some(_) | None => None,
        }
    }
}

const _: () = {
    assert!(mem::align_of::<Option<Packed>>() == mem::align_of::<Option<conn::Transmit>>());
    assert!(
        mem::size_of::<Closing>()
            == mem::size_of::<Option<Packed>>()
                + mem::size_of::<Option<conn::Transmit>>()
                + mem::align_of::<Closing>()
    );
};
