use std::mem;

use dope_core::driver::{route::Identity, schedule::ready};
use dope_net::wire::terminal;

mod accesses;
pub(in crate::client) mod retire;
pub(super) use accesses::{Quarantine, RetireRef, RetireState, SettledState};

use crate::client::{
    close,
    conn::{self, ClosePhase, CloseStatus, CloseTarget, CloseUpdate, claim, drain, state},
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Packed {
    phase: ClosePhase,
    rule: close::Directive,
    transport: claim::Transport,
    deferred: Option<close::Directive>,
}

impl Packed {
    const fn new(phase: ClosePhase, rule: close::Directive) -> Self {
        Self {
            phase,
            rule,
            transport: claim::Transport::Preserved,
            deferred: None,
        }
    }

    const fn with(self, rule: close::Directive) -> Self {
        Self { rule, ..self }
    }

    const fn with_phase(self, phase: ClosePhase) -> Self {
        Self { phase, ..self }
    }

    pub(super) const fn effective(self) -> close::Directive {
        match self.deferred {
            Some(rule) => rule,
            None => self.rule,
        }
    }

    fn merged(self, rule: close::Directive) -> Self {
        self.with(Self::merge(self.effective(), rule))
    }

    fn defer(&mut self, rule: close::Directive) {
        let current = self.effective();
        self.deferred = Some(Self::merge(current, rule));
    }

    fn observe(&mut self, reason: close::Reason) {
        let current = self.effective();
        self.deferred = Some(close::Directive::with_policy(reason, current.policy()));
    }

    fn resolve(mut self) -> Self {
        if let Some(rule) = self.deferred.take() {
            self = self.with(rule);
        }
        self
    }

    fn issue(&mut self) -> Claim {
        *self = self.resolve().with_phase(ClosePhase::Claimed);
        Claim { rule: self.rule }
    }

    fn restore(&mut self) -> drain::Resume {
        if self.deferred.is_some() {
            *self = self.resolve().with_phase(ClosePhase::Requested);
            drain::Resume::Pending
        } else {
            *self = self.with_phase(ClosePhase::Finalized);
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
        if matches!(self.phase, ClosePhase::Claimed) {
            self.deferred = Some(rule.resume());
        } else {
            *self = self.with(rule.resume());
        }
        self.transport = claim::Transport::Discarded;
        claim::Urgency::Immediate
    }

    fn finalize(&mut self) -> (Claim, claim::Urgency) {
        *self = self.resolve();
        let urgency = match self.rule.policy() {
            close::Policy::ImmediateReconnect | close::Policy::ImmediatePermanent => {
                match self.phase {
                    ClosePhase::Requested
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

    fn merge(left: close::Directive, right: close::Directive) -> close::Directive {
        let reason = left.reason().merge(right.reason());
        let policy = left.policy().merge(right.policy());
        close::Directive::with_policy(reason, policy)
    }

    fn apply(&mut self, rule: close::Directive) -> &mut Self {
        match self.phase {
            ClosePhase::Requested
            | ClosePhase::Quarantining
            | ClosePhase::Retiring
            | ClosePhase::Settled
            | ClosePhase::Finalized => *self = self.merged(rule),
            ClosePhase::Claimed => self.defer(rule),
        }
        self
    }

    pub(super) fn outcome(&self) -> CloseUpdate {
        match self.phase {
            ClosePhase::Requested
            | ClosePhase::Quarantining
            | ClosePhase::Retiring
            | ClosePhase::Settled
            | ClosePhase::Finalized => CloseUpdate::Applied(self.effective()),
            ClosePhase::Claimed => CloseUpdate::Sealed(self.effective()),
        }
    }

    pub(super) const fn status(self) -> CloseStatus {
        CloseStatus::Closing(self.phase, self.effective())
    }
}

#[derive(Default)]
pub(super) struct Closing {
    state: Option<Packed>,
    pub(super) transmit: Option<conn::Transmit>,
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
                ClosePhase::Requested
                | ClosePhase::Quarantining
                | ClosePhase::Retiring
                | ClosePhase::Settled => Self::Ready,
                ClosePhase::Finalized | ClosePhase::Claimed => Self::Sealed,
            },
        }
    }
}

pub(in crate::client::conn) struct Claim {
    rule: close::Directive,
}

impl Claim {
    pub(in crate::client::conn) const fn into_rule(self) -> close::Directive {
        self.rule
    }
}

pub(super) enum SettleState {
    Issued(Claim, claim::Urgency),
    Waiting,
    Active,
    Missing,
}

pub(super) enum CutState {
    Issued(Claim, claim::Urgency),
    Owned(claim::Urgency),
}

pub(super) enum ClaimState {
    Issued(Claim),
    Active,
    InProgress,
    Missing,
}

pub(super) enum Entry<'a, 'd, ID: Identity> {
    Started(Quarantine<'a>, close::Reason, state::Enter<'d, ID>),
    Retiring(RetireState<'a>, state::Enter<'d, ID>),
    Settled(SettledState<'a>),
    Finalized,
    Claimed,
}

impl Closing {
    pub(in crate::client::conn) fn complete(&mut self, rule: close::Directive) -> CloseUpdate {
        let state = self.apply(rule);
        if matches!(
            state.phase,
            ClosePhase::Requested
                | ClosePhase::Quarantining
                | ClosePhase::Retiring
                | ClosePhase::Settled
        ) {
            *state = state.with_phase(ClosePhase::Settled);
        }
        state.outcome()
    }

    pub(in crate::client::conn) fn settle(&mut self) -> SettleState {
        let Some(state) = &mut self.state else {
            return SettleState::Missing;
        };
        if matches!(state.phase, ClosePhase::Claimed) {
            return SettleState::Active;
        }
        if !matches!(state.phase, ClosePhase::Settled) {
            return SettleState::Waiting;
        }
        let (claim, urgency) = state.finalize();
        SettleState::Issued(claim, urgency)
    }

    pub(super) fn apply(&mut self, rule: close::Directive) -> &mut Packed {
        match &mut self.state {
            Some(state) => state.apply(rule),
            slot @ None => slot.insert(Packed::new(ClosePhase::Requested, rule)),
        }
    }

    pub(super) fn observe(&mut self, kind: terminal::Kind) {
        if matches!(kind, terminal::Kind::Clean) {
            self.apply(close::Directive::reconnect(close::Reason::Remote));
            return;
        }
        self.failed(close::Reason::Protocol)
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
    ) -> Entry<'_, 'd, ID> {
        let rule = close::Directive::reconnect(reason);
        let state = self
            .state
            .get_or_insert_with(|| Packed::new(ClosePhase::Quarantining, rule));
        let reason = match state.phase {
            ClosePhase::Claimed => return Entry::Claimed,
            ClosePhase::Finalized => return Entry::Finalized,
            ClosePhase::Settled => return Entry::Settled(SettledState(state)),
            ClosePhase::Retiring => return Entry::Retiring(RetireState(state), proof),
            ClosePhase::Quarantining => state.effective().reason(),
            ClosePhase::Requested => {
                *state = state.merged(rule).with_phase(ClosePhase::Quarantining);
                state.rule.reason()
            }
        };
        Entry::Started(Quarantine(state), reason, proof)
    }

    pub(super) fn claim_final(&mut self) -> ClaimState {
        match &mut self.state {
            None => ClaimState::Missing,
            Some(state) if matches!(state.phase, ClosePhase::Claimed) => ClaimState::Active,
            Some(state) if matches!(state.phase, ClosePhase::Finalized) => {
                ClaimState::Issued(state.issue())
            }
            Some(_) => ClaimState::InProgress,
        }
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
        self.state.is_some()
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

    pub(super) fn restore(&mut self, rule: close::Directive) -> drain::Restore {
        match &mut self.state {
            Some(state) if matches!(state.phase, ClosePhase::Claimed) && state.rule == rule => {
                drain::Restore::Current(state.restore())
            }
            Some(_) | None => drain::Restore::Stale,
        }
    }

    pub(super) fn authorize(&self, rule: close::Directive) -> Option<claim::Transport> {
        match self.state {
            Some(state) if matches!(state.phase, ClosePhase::Claimed) && state.rule == rule => {
                Some(state.transport)
            }
            Some(_) | None => None,
        }
    }

    pub(super) fn commit(&self, rule: close::Directive) -> Option<close::Directive> {
        match self.state {
            Some(state) if matches!(state.phase, ClosePhase::Claimed) && state.rule == rule => {
                Some(Packed::merge(state.effective(), rule))
            }
            Some(_) | None => None,
        }
    }
}

const _: () = {
    assert!(mem::align_of::<Option<Packed>>() == mem::align_of::<Option<conn::Transmit>>());
    assert!(
        mem::size_of::<Closing>()
            == mem::size_of::<Option<Packed>>() + mem::size_of::<Option<conn::Transmit>>()
    );
};
