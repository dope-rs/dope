#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum TimeoutKind {
    Connect,
    Inbound,
    Send,
    Lifetime,
    Auxiliary,
}

impl TimeoutKind {
    pub(crate) const COUNT: usize = 5;
    pub(crate) const ALL: [Self; Self::COUNT] = [
        Self::Connect,
        Self::Inbound,
        Self::Send,
        Self::Lifetime,
        Self::Auxiliary,
    ];

    pub(crate) const fn index(self) -> usize {
        self as usize
    }
}

const _: () = assert!(std::mem::size_of::<TimeoutKind>() == 1);

/// Why the connector retired a transport connection, independent of redial policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseReason {
    Local,
    Capacity,
    EndpointRetired,
    Transport,
    Timeout(TimeoutKind),
    Protocol,
    Remote,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Close {
    Keep,
    Reconnect,
    Permanent,
}

impl Close {
    pub(crate) fn is_keep(self) -> bool {
        self == Self::Keep
    }
}

pub trait Lifecycle: Default {
    fn wants_close(&self) -> Close;

    fn defer_close(&self) -> bool;

    fn is_drained(&self) -> bool;
}

#[derive(Default)]
pub struct Stateless;

impl Lifecycle for Stateless {
    fn wants_close(&self) -> Close {
        Close::Keep
    }

    fn defer_close(&self) -> bool {
        false
    }

    fn is_drained(&self) -> bool {
        true
    }
}
