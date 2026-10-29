use std::{
    mem, net,
    time::{Duration, Instant},
};

use dope::{
    core::{
        Errno,
        driver::{result, schedule::clock},
        io::transfer,
    },
    manifold::{client::close, network::datagram::RejectCause, service},
    net::link::{egress::load, event, pool::change::opening},
    runtime::civil,
};
use o3::{
    base::time::{Nanos, Positive, Unix},
    buffer::write::consume,
};

use crate::{
    host,
    net::stream::codec,
    query::family,
    storage::{queries::state, range::Range},
};

#[derive(Debug)]
pub struct Completion<const N: usize> {
    answer: state::Answer<N>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Identity {
    Cookie { epoch: u64, servers: usize },
    Datagram { capacity: usize },
    Stream { id: u16 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Packet {
    Length {
        actual: usize,
        maximum: usize,
    },
    Truncated {
        offset: usize,
        required: usize,
        actual: usize,
    },
    Pointer {
        offset: usize,
        target: usize,
        length: usize,
    },
    Label {
        offset: usize,
        value: u8,
    },
    NameLength {
        actual: usize,
        maximum: usize,
    },
    PointerHops {
        actual: usize,
        maximum: usize,
    },
    RecordName {
        actual: u16,
        expected: u16,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cookie {
    Capacity(consume::Error),
    Packet(Packet),
    Duplicate {
        offset: u16,
    },
    ClientLength {
        actual: usize,
        required: usize,
    },
    Client {
        actual: u64,
        expected: u64,
    },
    ServerLength {
        actual: usize,
        minimum: usize,
        maximum: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Malformed {
    Packet(Packet),
    Flags { actual: u16 },
    QuestionCount { actual: u16, expected: u16 },
    QuestionName(host::Mismatch),
    QuestionKind { actual: u16, expected: u16 },
    QuestionClass { actual: u16, expected: u16 },
    OptDuplicate { offset: u16 },
    OptName(host::Mismatch),
    Cookie(Cookie),
    Address(family::Error),
    Trailing { offset: u16, actual: usize },
    AliasRecords { actual: usize, maximum: usize },
    AliasHops { actual: usize, maximum: usize },
    AliasConflict,
    Name(host::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Timing {
    Duration {
        started: Instant,
        completed: Instant,
    },
    Expiry {
        received: Instant,
        ttl: Duration,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Stream(codec::Error),
    Opening(opening::Exhaustion),
    Timeout,
    Transport { errno: Errno },
    Datagram(Datagram),
    Egress(load::Error),
    Close { reason: close::Reason },
    Name,
    Refused,
    Server { code: u16 },
    Truncated,
    Empty,
    Capacity { limit: usize, actual: usize },
    Clock(clock::Range),
    Deadline(clock::Range<Positive>),
    Timing(Timing),
    Identity(Identity),
    Malformed(Malformed),
}

impl Kind {
    pub(crate) fn is_terminal(self) -> bool {
        match self {
            Self::Datagram(
                Datagram::Submit(cause) | Datagram::Rejected(RejectCause::Submit(cause)),
            )
            | Self::Close {
                reason: close::Reason::Receive(event::ParkError::Submit(cause)),
            } => matches!(cause.cause(), result::SubmitCause::IdentityExhausted),
            Self::Opening(_)
            | Self::Clock(_)
            | Self::Datagram(
                Datagram::Rejected(RejectCause::Inactive | RejectCause::IdentityExhausted)
                | Datagram::Shutdown,
            )
            | Self::Close {
                reason: close::Reason::Capacity(close::Capacity::IdentityExhausted),
            }
            | Self::Identity(Identity::Cookie { .. }) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Datagram {
    Rejected(RejectCause),
    Submit(result::SubmitError),
    Partial {
        sent: transfer::Len,
        expected: transfer::Len,
    },
    Shutdown,
}

#[derive(Debug)]
pub enum Error<const N: usize> {
    Query {
        kind: Kind,
        started_unix_ns: Unix<Nanos>,
        completed_unix_ns: Unix<Nanos>,
    },
    Clock {
        source: civil::Error,
    },
    AnswerClock {
        completion: Completion<N>,
        source: civil::Error,
    },
    QueryClock {
        kind: Kind,
        started_unix_ns: Unix<Nanos>,
        source: civil::Error,
    },
    QueryRevision {
        source: service::Exhausted,
        started_unix_ns: Unix<Nanos>,
        completed_unix_ns: Unix<Nanos>,
    },
    Revision {
        source: service::Exhausted,
    },
}

impl<const N: usize> Error<N> {
    pub(super) fn is_terminal(&self) -> bool {
        match self {
            Self::Query { kind, .. } | Self::QueryClock { kind, .. } => kind.is_terminal(),
            Self::QueryRevision { .. } | Self::Revision { .. } => true,
            _ => false,
        }
    }

    pub(super) const fn query(kind: Kind, times: Range) -> Self {
        Self::Query {
            kind,
            started_unix_ns: times.start,
            completed_unix_ns: times.done,
        }
    }

    pub const fn times(&self) -> Option<(Unix<Nanos>, Unix<Nanos>)> {
        match self {
            Self::Query {
                started_unix_ns: start,
                completed_unix_ns: done,
                ..
            }
            | Self::QueryRevision {
                started_unix_ns: start,
                completed_unix_ns: done,
                ..
            } => Some((*start, *done)),
            Self::Clock { .. }
            | Self::AnswerClock { .. }
            | Self::QueryClock { .. }
            | Self::Revision { .. } => None,
        }
    }
}

impl<const N: usize> Completion<N> {
    pub(super) const fn new(answer: state::Answer<N>) -> Self {
        Self { answer }
    }

    pub fn addresses(&self) -> &[net::IpAddr] {
        self.answer.addresses.as_slice()
    }

    pub const fn started_at(&self) -> Instant {
        self.answer.started_at
    }

    pub const fn completed_at(&self) -> Instant {
        self.answer.completed_at
    }

    pub fn ttl(&self) -> Duration {
        self.answer
            .valid_until
            .saturating_duration_since(self.answer.completed_at)
    }
}

const _: () = assert!(mem::size_of::<Kind>() == mem::size_of::<close::Reason>());
const _: () = assert!(mem::size_of::<Error<1>>() == 14 * mem::size_of::<usize>());
