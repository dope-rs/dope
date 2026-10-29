use crate::io::fd::handles;
pub mod datagram;
pub mod event;
pub mod fd;
pub mod fs;
pub(crate) mod outputs;
pub mod recv;
pub mod socket;
pub mod transfer;

use event::{batch, net::accept};

use crate::{
    Errno,
    driver::{
        result::{
            poll,
            poll::fault::{Echo, Fault},
        },
        retained::flight::cancel,
        route,
    },
};

/// A completion emitted by the driver.
///
/// [`Event::into_kind`] consumes its exact payload and operation authority.
/// Safe code cannot mint completion authority.
///
///
///
#[repr(transparent)]
pub struct Event<'d>(event::Kind<'d>);

impl<'d> Event<'d> {
    pub fn into_kind(self) -> event::Kind<'d> {
        self.0
    }

    pub fn route(&self) -> route::Route {
        match &self.0 {
            event::Kind::Accept(done) => done.token().route(),
            event::Kind::Recv(done) | event::Kind::RetiredRecv(done) => done.token().route(),
            event::Kind::Send(done) => done.token().route(),
            event::Kind::Socket(done) => done.token().route(),
            event::Kind::Tuning(done) => done.token().route(),
            event::Kind::Connect(done) => done.token().route(),
            event::Kind::Open(done) => done.token().route(),
            event::Kind::Write(done) => done.token().route(),
            event::Kind::Sync(done) => done.token().route(),
            event::Kind::Shutdown => route::Route::FRAMEWORK,
        }
    }
}

impl<'d> From<event::Kind<'d>> for Event<'d> {
    fn from(kind: event::Kind<'d>) -> Self {
        Self(kind)
    }
}

const _: () =
    assert!(std::mem::size_of::<Event<'static>>() == std::mem::size_of::<event::Kind<'static>>());

/// Cancellation failures retain their operation owners for retry.
/// Completion reports include buffer return and descriptor retirement outcomes.
/// Exhausted accepted slots remain quarantined in the native table until reactor teardown.
#[derive(Debug)]
pub enum Reclaimed {
    /// A consumed native completion that violated its operation contract.
    Fault(Fault),
    /// A native retirement syscall failed while its exact owner remains retained.
    Reactor(poll::Cause),
    /// A cancellation was rejected while its operation owner remains retained.
    Cancellation(cancel::Failure),
    Close {
        slot: handles::PhysicalSlot,
        file: poll::Close,
        reactor: Result<(), poll::Cause>,
    },
    Accept {
        echo: Echo,
        result: Result<handles::Reclaimed, accept::Failure>,
        flow: batch::Flow,
    },
    AcceptRoute {
        target: route::Token,
        cause: accept::Failure,
        flow: batch::Flow,
    },
    Recv {
        echo: Echo,
        result: Result<usize, Errno>,
        flow: batch::Flow,
    },
    RecvRoute {
        target: route::Token,
        result: Result<transfer::Len, Errno>,
        flow: batch::Flow,
    },
    Send {
        echo: Echo,
        result: Result<transfer::Len, Errno>,
    },
    Connect {
        echo: Echo,
        result: Result<(), Errno>,
    },
    Socket {
        echo: Echo,
        slot: handles::PhysicalSlot,
        result: Result<(), socket::Create>,
        close: Option<Result<(), Errno>>,
    },
    Open {
        echo: Echo,
        result: Result<(), Errno>,
        close: Option<Result<(), Errno>>,
    },
    Write {
        echo: Echo,
        result: Result<transfer::Len, Errno>,
    },
    Sync {
        echo: Echo,
        result: Result<(), Errno>,
    },
    Tuning {
        echo: Echo,
        result: Result<(), Errno>,
    },
    Shutdown,
}
