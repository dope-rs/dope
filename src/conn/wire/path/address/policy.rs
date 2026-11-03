use crate::proto::path::Peer;
use crate::wire::stream::Initiator;

/// Exhaustive role and proto-parameter policy for peer address changes.
#[derive(Clone, Copy)]
#[repr(u8)]
pub(super) enum Policy {
    Fixed,
    NatRebinding,
    Active,
}

impl Policy {
    pub(super) const fn new(local: Initiator, disabled: bool) -> Self {
        match (local, disabled) {
            (Initiator::Client, _) => Self::Fixed,
            (Initiator::Server, true) => Self::NatRebinding,
            (Initiator::Server, false) => Self::Active,
        }
    }

    pub(super) fn allows(self, active: Peer, source: Peer) -> bool {
        match self {
            Self::Fixed => false,
            Self::NatRebinding => source.addr().ip().eq(&active.addr().ip()),
            Self::Active => true,
        }
    }
}

const _: () = assert!(std::mem::size_of::<Policy>() == 1);
