use std::net;

use o3::base::num::bounded;

use crate::{
    io::reactor::complete::message,
    probe::{self, cleanup, session},
};

/// The two multishot receive contracts checked on one canary session.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Mode {
    Data,
    Message,
}

/// Fixed-file installation failed before any receive was submitted.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Install<E> {
    pub mode: Mode,
    pub error: E,
}

/// Host address and payload checks borrow the native frame for one observation.
pub trait Context<V: Receive>: probe::Context {
    type Check;

    fn inspect(mode: Mode, bytes: &[u8], want: &[u8]) -> Result<(), Self::Check>;
}

/// Runs both multishot receive contracts while retaining every pending owner.
pub trait Validate<P: Copy, R: Copy>: session::Empty {
    type Update;
    type Cleanup;
    type Release;

    fn receive(
        self,
        release: impl Fn(Mode, Self::Update) -> R,
    ) -> Result<Self, cleanup::Rejection<Self::Cleanup, Self::Release>>;
}

/// A capability probe sent a different number of bytes than requested.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Partial {
    actual: usize,
    expected: usize,
}

impl Partial {
    pub const fn classify(actual: usize, expected: usize) -> Option<Self> {
        if actual == expected {
            None
        } else {
            Some(Self { actual, expected })
        }
    }
}

impl From<Partial> for (usize, usize) {
    fn from(partial: Partial) -> Self {
        (partial.actual, partial.expected)
    }
}

/// Exact failure from sending one probe payload.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Send {
    Syscall(crate::Errno),
    Partial(Partial),
}

/// A capability probe received a different payload or a non-loopback peer.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Corrupt;

/// Native framing and address failures retain their original evidence.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Failure<C, L, E> {
    Payload(Corrupt),
    Frame(message::Error<C, L>),
    Truncated,
    Address(E),
}

/// Verifies probe data while borrowing the native frame only for this call.
pub trait Receive {
    type Capacity: Copy + Eq + Into<bounded::U64<0, { u64::MAX }>>;
    type Length: Copy + Eq + Into<bounded::U64<0, { u64::MAX }>>;

    fn bytes(actual: &[u8], want: &[u8]) -> Result<(), Corrupt>;

    fn message<const NAME: usize, E>(
        bytes: &[u8],
        want: &[u8],
        parse: impl FnOnce(&[u8]) -> Result<net::SocketAddr, E>,
    ) -> Result<(), Failure<Self::Capacity, Self::Length, E>>;
}
