//! Typed operations retain storage, exclusive borrows, and callback identity.

pub mod app;
pub mod client;
pub mod file;
pub mod network;
pub mod service;
pub mod timing;

use std::{mem, num};

use app::{creation, receive::ingress};
/// The route identity vocabulary every manifold generic is parameterized by.
pub use dope_core::driver::route;
use dope_core::{
    driver::{retained::flight, route::table, schedule::clock},
    io::transfer,
};
use dope_net::link::{egress::ResidentSize, pool};
use o3::{base::time::Positive, buffer::hold::limits, depot::capacity};

type Family<T> = std::marker::PhantomData<fn() -> T>;

type Flights<'d, ID> = flight::Slots<'d, ID>;

type Inbound<'p, 'w, 'd, ID, P> = pool::Connections<
    'p,
    'w,
    'd,
    ID,
    <P as ingress::Policy<'p, 'w, 'd, ID>>::Transport,
    <P as ingress::Policy<'p, 'w, 'd, ID>>::Wire,
    <P as ingress::Policy<'p, 'w, 'd, ID>>::State,
    <P as ingress::Policy<'p, 'w, 'd, ID>>::Input,
    <P as ingress::Policy<'p, 'w, 'd, ID>>::Payload,
    { ingress::IOV_CAP },
    <P as ingress::Policy<'p, 'w, 'd, ID>>::Reserve,
>;

type Ingress<W, M> = <M as ingress::Dispatch<W>>::Strategy;

type Receiving<'w, 'd, W, M> =
    ingress::Progress<<Ingress<W, M> as ingress::Strategy<W, M>>::Pending<'w, 'd>>;

/// Preserves a caller's typed factory failure without erasing the framework's
/// own construction failure.

#[derive(Debug)]
pub enum BuildError<E> {
    Create(creation::Error),
    Factory(E),
}

impl<E> From<creation::Error> for BuildError<E> {
    fn from(error: creation::Error) -> Self {
        Self::Create(error)
    }
}

/// Invalid construction relationship not already represented by an input
/// type. This compact value never formats or allocates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppendShape {
    ZeroBlocks,
    ZeroBytes,
    BlockLimit { actual: usize, maximum: usize },
    Total { blocks: usize, bytes: usize },
}

impl AppendShape {
    pub(crate) const fn validate<const N: usize, const B: usize>() -> Result<(), Self> {
        if N == 0 {
            return Err(Self::ZeroBlocks);
        }
        if B == 0 {
            return Err(Self::ZeroBytes);
        }
        if B > transfer::MAX_BYTES {
            return Err(Self::BlockLimit {
                actual: B,
                maximum: transfer::MAX_BYTES,
            });
        }
        if N.checked_mul(B).is_none() {
            return Err(Self::Total {
                blocks: N,
                bytes: B,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ConfigError {
    ConnectorOutboundCapacity {
        requested: usize,
        available: usize,
    },
    ConnectorRouteUnavailable,
    AppendGeometry(AppendShape),
    DatagramPendingZero,
    DatagramPendingCapacity(table::Invalid),
    DatagramReceiveCapacity {
        slots: num::NonZero<usize>,
        bytes: num::NonZero<usize>,
    },
    DatagramFlightZero,
    DatagramFlightCapacity(table::Invalid),
    ListenerCapacity(table::Invalid),
    ListenerAcceptCapacity {
        actual: usize,
        maximum: usize,
    },
    ListenerPeerCapacity(capacity::Error),
    ListenerBoundedEgress {
        body: ResidentSize,
    },
    ListenerUnboundedEgress,
    ListenerWriteFlights {
        actual: usize,
        maximum: usize,
    },
    RetainedReceiveCapacity {
        connections: usize,
        each: usize,
        minimum: usize,
    },
    ScopedReceiveCapacity(limits::Error),
    WatchdogOrder {
        period: Positive,
        deadline: Positive,
    },
    WatchdogRange(clock::Range<Positive>),
}

const _: () = {
    assert!(mem::size_of::<ConfigError>() == 5 * mem::size_of::<usize>());
    assert!(mem::align_of::<ConfigError>() == mem::align_of::<clock::Range>());
    assert!(!mem::needs_drop::<ConfigError>());
};
