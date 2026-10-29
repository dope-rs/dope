use std::ops::ControlFlow;

use dope_core::{
    driver::{
        receive::{Stale, credit::error::Arm},
        result, route,
        route::{Identity, KeyTag, kind, table::conn},
    },
    io::{event::net::recv, recv::Lease, socket, transfer},
};
use o3::{
    depot::{arena::slab::key, queue::tagged::entry},
    mem::rc,
};

use crate::{
    link,
    link::{pool, slot::transmit},
};

#[derive(Clone, Copy, Debug)]
pub enum ConnectError {
    Create(socket::Create),
    Option(dope_core::Errno),
    Admission(result::SubmitError),
    Connect(dope_core::Errno),
    State(rc::Error),
    Receive(dope_core::Errno),
}

#[derive(Clone, Copy, Debug)]
pub enum AttemptError {
    Lookup(key::Observation),
    Finished,
}

#[derive(Clone, Copy, Debug)]
pub enum ConnectFault {
    Transport(ConnectError),
    Attempt(AttemptError),
    Cleanup {
        cause: ConnectError,
        error: AttemptError,
    },
}

pub enum Socket<'d, ID: Identity, X> {
    Pending,
    Failed {
        key: pool::Key<'d, ID>,
        attempt: X,
        cause: ConnectError,
    },
    Stale,
}

pub enum Connect<'d, ID: Identity, X, A> {
    Connected {
        key: pool::Key<'d, ID>,
        attempt: X,
        peer: A,
    },
    Failed {
        key: pool::Key<'d, ID>,
        attempt: X,
        cause: ConnectError,
    },
    Cancelled {
        key: pool::Key<'d, ID>,
    },
}

pub enum DispatchRecv<'d, ID: Identity, C> {
    Control(ControlRecv<'d, ID>),
    Chunk(pool::Key<'d, ID>, C),
}

pub enum ControlRecv<'d, ID: Identity> {
    Eof(pool::Key<'d, ID>),
    Cancelled(pool::Key<'d, ID>),
    Failed {
        key: pool::Key<'d, ID>,
        error: dope_core::Errno,
    },
    Credit {
        key: pool::Key<'d, ID>,
        cause: Arm,
    },
    NoChunk(pool::Key<'d, ID>),
    Discarded {
        key: pool::Key<'d, ID>,
        /// This receive completed an active raw prefix without emitting a chunk.
        finished: bool,
    },
}

pub enum Reservation<'d, ID: Identity, P> {
    Ready(P),
    Control(ControlRecv<'d, ID>),
    Parked(ParkRecv<'d, ID>),
    Stale(Stale),
    Drop,
}

impl<'d, ID: Identity, P> Reservation<'d, ID, P> {
    pub fn map<T>(self, map: impl FnOnce(P) -> T) -> Reservation<'d, ID, T> {
        match self {
            Self::Ready(item) => Reservation::Ready(map(item)),
            Self::Control(control) => Reservation::Control(control),
            Self::Parked(parked) => Reservation::Parked(parked),
            Self::Stale(stale) => Reservation::Stale(stale),
            Self::Drop => Reservation::Drop,
        }
    }
}

pub enum Dispatch<'d, ID: Identity> {
    /// The live receive owner consumed this completion without an application event.
    Settled,
    Ready(ControlRecv<'d, ID>),
    Parked(ParkRecv<'d, ID>),
    Retired(recv::Completion<'d>),
    Observed {
        completion: recv::Completion<'d>,
        next: ControlRecv<'d, ID>,
    },
    Stale {
        completion: recv::Completion<'d>,
        cause: Stale,
    },
    Rejected {
        completion: recv::Completion<'d>,
        key: pool::Key<'d, ID>,
        error: ParkError,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ParkError {
    Submit(result::SubmitError),
    Credit(Arm),
    Register(entry::Error<conn::Capacity>),
    Full,
    Occupied,
    Unavailable,
}

pub enum ParkRecv<'d, ID: Identity> {
    Deferred,
    Closing(pool::Key<'d, ID>),
    Failed {
        key: pool::Key<'d, ID>,
        error: ParkError,
    },
}

/// Move-only receive completion whose exact kernel flight was observed.
/// Deferred delivery can replay its payload without settling kernel authority
/// twice.
#[must_use = "an observed receive completion must be delivered or dropped"]
#[repr(transparent)]
pub struct Completion<'d>(Kind<'d>);

pub enum Kind<'d> {
    Data(Data<'d>),
    Control(Control<'d>),
}

#[must_use = "observed receive data must be delivered, deferred, or dropped"]
pub struct Data<'d> {
    token: route::Erased<'d, kind::Recv>,
    lease: Lease<'d>,
}

#[must_use = "observed receive control must be dispatched or dropped"]
#[repr(transparent)]
pub struct Control<'d>(recv::Control<'d>);

impl<'d> Completion<'d> {
    pub fn classify(self) -> Kind<'d> {
        self.0
    }
}

impl<'d> Data<'d> {
    pub(in crate::link) fn new(token: route::Erased<'d, kind::Recv>, lease: Lease<'d>) -> Self {
        Self { token, lease }
    }

    pub(in crate::link) const fn token(&self) -> route::Erased<'d, kind::Recv> {
        self.token
    }

    pub(in crate::link) fn lease_mut(&mut self) -> &mut Lease<'d> {
        &mut self.lease
    }

    pub(in crate::link) fn into_lease(self) -> Lease<'d> {
        self.lease
    }

    pub(in crate::link) fn complete(self) -> Completion<'d> {
        Completion(Kind::Data(self))
    }
}

impl<'d> Control<'d> {
    pub(in crate::link) fn new(control: recv::Control<'d>) -> Self {
        Self(control)
    }

    pub(in crate::link) const fn token(&self) -> route::Erased<'d, kind::Recv> {
        self.0.token()
    }

    pub const fn status(&self) -> recv::Status {
        self.0.status()
    }

    pub(in crate::link) fn dispatch<T, E>(
        self,
        dispatch: impl FnOnce(recv::ControlKind<'d>) -> ControlFlow<(recv::ControlKind<'d>, E), T>,
    ) -> ControlFlow<(Self, E), T> {
        self.0
            .dispatch(dispatch)
            .map_break(|(state, cause)| (Self(state), cause))
    }

    pub(in crate::link) fn retire(self) -> recv::Completion<'d> {
        self.0.rejoin()
    }

    pub(in crate::link) fn complete(self) -> Completion<'d> {
        Completion(Kind::Control(self))
    }
}

/// Terminal direct-send state delivered to the exact completion callback.
#[derive(Clone, Copy)]
pub enum DirectStatus {
    Sent(Option<link::Consumed>),
    Rejected(result::SubmitError),
    Failed(dope_core::Errno),
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendSource {
    Direct,
    Queued,
    Terminal,
    WriteShutdown,
}

/// Exact send operation and typed failure observed by the transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendFailure<E> {
    Direct(E),
    Queued(E),
    Terminal(E),
    WriteShutdown(E),
}

impl<E> SendFailure<E> {
    pub fn into_error(self) -> E {
        match self {
            Self::Direct(error)
            | Self::Queued(error)
            | Self::Terminal(error)
            | Self::WriteShutdown(error) => error,
        }
    }
}

/// Fully settled send result. No kernel or queued-egress authority crosses
/// this boundary.
#[must_use = "the fully settled send outcome must be dispatched"]
pub enum SendOutcome<'d, ID: Identity, D = ()> {
    Queued {
        key: pool::Key<'d, ID>,
        acknowledged: transfer::Len,
    },
    Direct {
        key: pool::Key<'d, ID>,
        settled: D,
    },
    WriteShutdown {
        key: pool::Key<'d, ID>,
    },
    Terminal {
        key: pool::Key<'d, ID>,
    },
    Rejected {
        key: pool::Key<'d, ID>,
        failure: SendFailure<result::SubmitError>,
    },
    Failed {
        key: pool::Key<'d, ID>,
        failure: SendFailure<dope_core::Errno>,
    },
    Invalid {
        key: pool::Key<'d, ID>,
        error: transmit::Invalid,
    },
    Close(pool::Key<'d, ID>),
    CloseSent {
        key: pool::Key<'d, ID>,
        acknowledged: transfer::Len,
    },
    Drop,
}

const _: () = {
    assert!(std::mem::size_of::<ParkError>() == 3 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Completion<'static>>() == std::mem::size_of::<Kind<'static>>());
    assert!(std::mem::align_of::<Completion<'static>>() == std::mem::align_of::<Kind<'static>>());
    assert!(
        std::mem::size_of::<Completion<'static>>()
            <= std::mem::size_of::<recv::Completion<'static>>()
    );
    assert!(
        std::mem::size_of::<Data<'static>>()
            == std::mem::size_of::<(route::Token, Lease<'static>)>()
    );
    assert!(
        std::mem::align_of::<Data<'static>>()
            == std::mem::align_of::<(route::Token, Lease<'static>)>()
    );
    assert!(
        std::mem::size_of::<Control<'static>>()
            == std::mem::size_of::<(route::Token, recv::Status, recv::ControlKind<'static>,)>()
    );
    assert!(
        std::mem::align_of::<Control<'static>>()
            == std::mem::align_of::<(route::Token, recv::Status, recv::ControlKind<'static>,)>()
    );
    assert!(std::mem::size_of::<Kind<'static>>() <= std::mem::size_of::<Completion<'static>>());
    assert!(std::mem::size_of::<SendFailure<dope_core::Errno>>() == std::mem::size_of::<usize>());
    assert!(
        std::mem::size_of::<SendFailure<result::SubmitError>>() <= std::mem::size_of::<usize>()
    );
    assert!(std::mem::size_of::<DirectStatus>() == std::mem::size_of::<usize>());
    assert!(
        std::mem::size_of::<SendOutcome<'static, KeyTag<0>>>() == 5 * std::mem::size_of::<usize>()
    );
};
