use std::mem;

use dope::net::link::{event, slot::transmit};

use crate::{
    core::{driver::result, io::transfer},
    manifold::{client::conn, network::listener},
};

/// Number of bytes retained across receive chunks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Buffer(u64);

impl Buffer {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Number of receive chunks retained for one connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Chunks(usize);

impl Chunks {
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Retention {
    Lane { index: usize, lanes: usize },
    Exhausted,
}

#[derive(Clone, Copy, Debug)]
pub enum Local {
    BufferedBytesTooLarge { actual: Buffer, maximum: Buffer },
    ChunkCountTooLarge { actual: Chunks, maximum: Chunks },
    RetentionLane { index: usize, lanes: usize },
    RetentionExhausted,
    Submission(result::SubmitError),
    Receive(event::ParkError),
}

impl From<event::ParkError> for Local {
    fn from(cause: event::ParkError) -> Self {
        match cause {
            event::ParkError::Submit(error) => Self::Submission(error),
            cause => Self::Receive(cause),
        }
    }
}

/// Exact transport send failure, including its operation and admitted payload bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Transmit {
    Io(event::SendFailure<crate::core::Errno>),
    Rejected(event::SendFailure<result::SubmitError>),
    Invalid(transmit::Invalid),
    Payload {
        offset: transfer::Len,
        length: transfer::Len,
        remaining: transfer::Len,
    },
}

impl From<&conn::Transmit> for Transmit {
    fn from(cause: &conn::Transmit) -> Self {
        use conn::Transmit;

        match cause {
            Transmit::Io(error) => Self::Io(*error),
            Transmit::Rejected(error) => Self::Rejected(*error),
            Transmit::Invalid(error) => Self::Invalid(*error),
        }
    }
}

const _: () = {
    use listener::conn;

    impl From<&conn::Transmit> for Transmit {
        fn from(cause: &conn::Transmit) -> Self {
            use conn::Transmit;

            match cause {
                Transmit::Io(error) => Self::Io(*error),
                Transmit::Rejected(error) => Self::Rejected(*error),
                Transmit::Invalid(error) => Self::Invalid(*error),
                Transmit::Payload {
                    offset,
                    length,
                    remaining: left,
                } => Self::Payload {
                    offset: *offset,
                    length: *length,
                    remaining: *left,
                },
            }
        }
    }
};

#[derive(Debug)]
pub enum Edge<E> {
    Wire(E),
    Transport(crate::core::Errno),
    Transmit(Transmit),
}

#[derive(Debug)]
pub enum Read<E> {
    Wire(E),
    Transport(crate::core::Errno),
    Transmit(Transmit),
    BufferedBytesTooLarge { actual: Buffer, maximum: Buffer },
    ChunkCountTooLarge { actual: Chunks, maximum: Chunks },
    RetentionCapacity(Retention),
    Submission(result::SubmitError),
    Receive(event::ParkError),
    LocalThenEdge { local: Local, edge: Edge<E> },
}

impl<E> Read<E> {
    /// Borrows the wire cause while preserving every receive failure and its metadata.
    pub fn map_ref<'a, F>(&'a self, map: impl FnOnce(&'a E) -> F) -> Read<F> {
        match self {
            Self::Wire(cause) => Read::Wire(map(cause)),
            Self::Transport(cause) => Read::Transport(*cause),
            Self::Transmit(cause) => Read::Transmit(*cause),
            Self::BufferedBytesTooLarge { actual, maximum } => Read::BufferedBytesTooLarge {
                actual: *actual,
                maximum: *maximum,
            },
            Self::ChunkCountTooLarge { actual, maximum } => Read::ChunkCountTooLarge {
                actual: *actual,
                maximum: *maximum,
            },
            Self::RetentionCapacity(cause) => Read::RetentionCapacity(*cause),
            Self::Submission(cause) => Read::Submission(*cause),
            Self::Receive(cause) => Read::Receive(*cause),
            Self::LocalThenEdge { local, edge } => Read::LocalThenEdge {
                local: *local,
                edge: match edge {
                    Edge::Wire(cause) => Edge::Wire(map(cause)),
                    Edge::Transport(cause) => Edge::Transport(*cause),
                    Edge::Transmit(cause) => Edge::Transmit(*cause),
                },
            },
        }
    }
}

const _: () = assert!(mem::size_of::<Buffer>() == mem::size_of::<u64>());
const _: () = assert!(mem::size_of::<Chunks>() == mem::size_of::<usize>());

impl<E> From<Local> for Read<E> {
    fn from(local: Local) -> Self {
        match local {
            Local::BufferedBytesTooLarge { actual, maximum } => {
                Self::BufferedBytesTooLarge { actual, maximum }
            }
            Local::ChunkCountTooLarge { actual, maximum } => {
                Self::ChunkCountTooLarge { actual, maximum }
            }
            Local::RetentionLane { index, lanes } => {
                Self::RetentionCapacity(Retention::Lane { index, lanes })
            }
            Local::RetentionExhausted => Self::RetentionCapacity(Retention::Exhausted),
            Local::Submission(cause) => Self::Submission(cause),
            Local::Receive(cause) => Self::Receive(cause),
        }
    }
}

impl<E> From<Edge<E>> for Read<E> {
    fn from(edge: Edge<E>) -> Self {
        match edge {
            Edge::Wire(error) => Self::Wire(error),
            Edge::Transport(error) => Self::Transport(error),
            Edge::Transmit(error) => Self::Transmit(error),
        }
    }
}
