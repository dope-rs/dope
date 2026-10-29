use dope_core::driver::result;
use dope_net::link::{egress::reserve::exact, event};

use crate::{app::receive, client::deadline};

/// Why the connector retired a transport connection, independent of redial policy.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Reason {
    Local(LocalOrigin),
    Capacity(Capacity),
    EndpointRetired,
    Transport,
    Timeout(deadline::Kind),
    Protocol,
    Remote,
    Receive(event::ParkError),
    Ingress(receive::Failure),
    Egress(exact::Failure),
}

impl Reason {
    pub(crate) const fn merge(self, next: Self) -> Self {
        match self {
            Self::Remote => next,
            reason => reason,
        }
    }
}

impl From<receive::Failure> for Reason {
    fn from(failure: receive::Failure) -> Self {
        Self::Ingress(failure)
    }
}

/// Exact bounded resource that forced transport retirement.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum Capacity {
    IngressStaging,
    IngressRetention,
    Transport,
    IdentityExhausted,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LocalOrigin {
    ApiRequest,
    SideComplete,
    OperationCancelled,
    Superseded,
    ScheduledRotation,
    RuntimeShutdown,
    ResourceFailure,
}

const FINAL: u8 = 1 << 0;
const IMMEDIATE: u8 = 1 << 1;
const BOTH: u8 = IMMEDIATE | FINAL;

/// Monotone transport disposition attached to one close request.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Policy {
    #[default]
    Reconnect,
    Permanent = FINAL,
    ImmediateReconnect = IMMEDIATE,
    ImmediatePermanent = BOTH,
}

impl Policy {
    pub(crate) const fn from_bits(bits: u8) -> Self {
        match (bits & FINAL != 0, bits & IMMEDIATE != 0) {
            (false, false) => Self::Reconnect,
            (true, false) => Self::Permanent,
            (false, true) => Self::ImmediateReconnect,
            (true, true) => Self::ImmediatePermanent,
        }
    }

    pub(crate) const fn bits(self) -> u8 {
        self as u8
    }

    pub(crate) const fn merge(self, other: Self) -> Self {
        Self::from_bits(self.bits() | other.bits())
    }

    pub(crate) const fn resume(self) -> Self {
        Self::from_bits(self.bits() & !IMMEDIATE)
    }
}

/// A transport-retirement decision retaining the exact causal payload.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Directive {
    reason: Reason,
    policy: Policy,
}

impl Directive {
    pub(crate) fn from_receive(error: event::ParkError) -> Self {
        let reason = Reason::Receive(error);
        match error {
            event::ParkError::Submit(error) => {
                Self::with_policy(reason, Self::from_submit(error).policy())
            }
            _ => Self::reconnect(reason),
        }
    }

    pub(crate) fn from_submit(error: result::SubmitError) -> Self {
        match error.cause() {
            result::SubmitCause::Full => Self::reconnect(Reason::Capacity(Capacity::Transport)),
            result::SubmitCause::IdentityExhausted => {
                Self::cut_final(Reason::Capacity(Capacity::IdentityExhausted))
            }
        }
    }

    pub fn reconnect(reason: Reason) -> Self {
        Self {
            reason,
            policy: Policy::Reconnect,
        }
    }
    pub fn permanent(reason: Reason) -> Self {
        Self {
            reason,
            policy: Policy::Permanent,
        }
    }
    pub fn cut_retry(reason: Reason) -> Self {
        Self {
            reason,
            policy: Policy::ImmediateReconnect,
        }
    }
    pub fn cut_final(reason: Reason) -> Self {
        Self {
            reason,
            policy: Policy::ImmediatePermanent,
        }
    }
    pub fn with_policy(reason: Reason, policy: Policy) -> Self {
        Self { reason, policy }
    }
    pub const fn reason(self) -> Reason {
        self.reason
    }
    pub const fn policy(self) -> Policy {
        self.policy
    }
    pub(crate) fn merge(self, policy: Policy) -> Self {
        Self {
            reason: self.reason,
            policy: self.policy.merge(policy),
        }
    }
    pub(crate) fn resume(self) -> Self {
        Self {
            reason: self.reason,
            policy: self.policy.resume(),
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Reason>() == 6 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<LocalOrigin>() == 1);
    assert!(std::mem::size_of::<Capacity>() == 1);
    assert!(std::mem::size_of::<Policy>() == 1);
    assert!(std::mem::size_of::<Directive>() == 7 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Option<Directive>>() == std::mem::size_of::<Directive>());
};
