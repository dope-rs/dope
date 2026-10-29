use std::{net, num, pin};

use dope_core::{
    driver::{
        result, retained,
        route::{Identity, KeyTag, indices},
        schedule,
        schedule::{app, clock},
    },
    io::datagram,
};
use o3::{base::cell::region, buffer::bytes::bounded::Overflow, mem};

use crate::app::dispatch;

mod endpoint;
pub mod group;
pub mod hook;
pub mod packet;
pub mod plan;
mod prepared;
pub mod send;
mod socket;

use dope_core::io::datagram::ReceiveError;
pub use endpoint::Endpoint;
use hook::ingress;
pub use prepared::Prepared;
pub use socket::Socket;
pub const GSO_LIMITS: Option<datagram::GsoLimits> = datagram::GSO_LIMITS;

/// Read-only handler access while retaining the installed endpoint and driver brand.
///
/// ```compile_fail
/// use dope_core::driver::route::KeyTag;
/// use dope_manifold::network::datagram::View;
/// fn escape(view: View<'_, '_, KeyTag<0>, u32, ()>) -> &'static u32 {
///     view.read(|handler| handler)
/// }
/// ```
///
/// ```compile_fail
/// use dope_core::driver::route::KeyTag;
/// use dope_manifold::network::datagram::View;
/// fn substitute<'s, 'a: 's, 'b: 's>(view: View<'s, 'a, KeyTag<0>, (), ()>)
///     -> View<'s, 'b, KeyTag<0>, (), ()> where 'a: 'b {
///     view
/// }
/// ```
#[repr(transparent)]
pub struct View<'step, 'd: 'step, ID: Identity, H, B, M: send::Mode = send::Plain> {
    owner: dispatch::raw::Installed<'step, 'd, Endpoint<'d, ID, H, B, M>>,
}

impl<'step, 'd: 'step, ID: Identity, H, B, M: send::Mode> View<'step, 'd, ID, H, B, M> {
    pub(in crate::network::datagram) fn new(
        owner: dispatch::raw::Installed<'step, 'd, Endpoint<'d, ID, H, B, M>>,
    ) -> Self {
        Self { owner }
    }

    pub fn read<R>(&self, read: impl for<'a> FnOnce(&'a H) -> R) -> R {
        read(self.owner.as_ref().get_ref().handler())
    }
}

const _: () = {
    assert!(
        std::mem::size_of::<View<'static, 'static, KeyTag<0>, (), ()>>()
            == std::mem::size_of::<usize>()
    );
    assert!(
        std::mem::size_of::<Option<View<'static, 'static, KeyTag<0>, (), ()>>>()
            == std::mem::size_of::<usize>()
    );
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RejectCause {
    Inactive,
    QueueFull,
    IdentityExhausted,
    Submit(result::SubmitError),
    Slot {
        requested: indices::SlotIndex,
        capacity: usize,
    },
    Transfer(Overflow),
    BudgetMissing {
        required: usize,
    },
    Resident {
        required: num::NonZero<usize>,
        available: usize,
    },
    GsoUnavailable,
    GsoShape(datagram::Shape),
}

#[must_use = "a rejected datagram owner must be recovered or handled"]
#[repr(transparent)]
pub struct Rejected<T>(mem::Rejected<T, RejectCause>);

impl<T> Rejected<T> {
    pub(crate) const fn new(value: T, cause: RejectCause) -> Self {
        Self(mem::Rejected::new(value, cause))
    }

    pub const fn cause(&self) -> RejectCause {
        *self.0.error()
    }

    pub const fn value(&self) -> &T {
        self.0.value()
    }

    pub fn into_parts(self) -> (T, RejectCause) {
        self.0.into_parts()
    }

    pub(crate) fn map<U>(self, map: impl FnOnce(T) -> U) -> Rejected<U> {
        Rejected(self.0.map(map))
    }
}

pub trait Install {
    fn installed(self: pin::Pin<&mut Self>);
}

pub trait Shutdown {
    fn shutdown(self: pin::Pin<&mut Self>);
}

pub trait Finalize {
    fn finalized(self: pin::Pin<&mut Self>, state: Option<datagram::Residency>);
}

pub trait Transfer<'d, ID: Identity> {
    fn transferred(self: pin::Pin<&mut Self>, transfer: send::Transfer<'d, ID>);
}

pub trait Transport<'d, ID: Identity>: Ingress<'d, ID> {
    /// Drives protocol transport work without an application context.
    fn pre_park<'turn>(
        self: pin::Pin<&mut Self>,
        sock: pin::Pin<&mut hook::Socket<'d, ID, Self::Hooks>>,
        now: clock::Moment<'d>,
        work: app::Work<'turn, 'd>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    );

    /// Contributes protocol work or a deadline in addition to socket I/O.
    fn progress(
        self: pin::Pin<&Self>,
        clock: clock::Clock<'_, 'd>,
        token: &region::Token<region::Identity<'d, 1>>,
    ) -> schedule::Progress<'d>;
}

pub trait Application<'d, ID: Identity>: Ingress<'d, ID> {
    /// Queues application work against the canonical sample immediately
    /// before the socket is driven.
    fn pre_park<'turn>(
        self: pin::Pin<&mut Self>,
        sock: pin::Pin<&mut hook::Socket<'d, ID, Self::Hooks>>,
        now: clock::Moment<'d>,
        work: app::Work<'turn, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    /// Contributes application work or a deadline in addition to socket I/O.
    fn progress(
        self: pin::Pin<&Self>,
        clock: clock::Clock<'_, 'd>,
        token: &region::Token<region::Identity<'d, 1>>,
    ) -> schedule::Progress<'d>;
}

pub trait Ingress<'d, ID: Identity>: Sized {
    type Hooks: hook::Bundle;
    type Mode: ingress::Failure<'d, ID, Self>;
}

pub trait Time<'d, ID: Identity>: Ingress<'d, ID> {
    /// Delivers one packet with the canonical sample for its driver turn.
    fn packet<'turn>(
        self: pin::Pin<&mut Self>,
        packet: Result<(net::SocketAddr, packet::Packet<'turn, 'd>), ReceiveError>,
        now: clock::Moment<'d>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    );
}

pub trait Access<'d, ID: Identity>: Ingress<'d, ID> {
    /// Delivers one packet with socket access.
    fn packet<'turn>(
        self: pin::Pin<&mut Self>,
        packet: Result<(net::SocketAddr, packet::Packet<'turn, 'd>), ReceiveError>,
        sock: pin::Pin<&'turn mut hook::Socket<'d, ID, Self::Hooks>>,
    );
}

pub trait Coupled<'d, ID: Identity>: Ingress<'d, ID> {
    /// Delivers one packet with socket access and the canonical driver sample.
    fn packet<'turn>(
        self: pin::Pin<&mut Self>,
        packet: Result<(net::SocketAddr, packet::Packet<'turn, 'd>), ReceiveError>,
        sock: pin::Pin<&'turn mut hook::Socket<'d, ID, Self::Hooks>>,
        now: clock::Moment<'d>,
    );
}

pub trait Settlement<'d, ID: Identity>: Ingress<'d, ID> {
    /// Consumes the exact terminal result and payload owner of every accepted
    /// send, including unsubmitted and forced-shutdown retirement.
    fn settle<'turn>(
        self: pin::Pin<&mut Self>,
        done: send::Settlement<'turn, 'd, ID, <Self::Hooks as hook::Bundle>::Buffer>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    );
}

const RECV_ARM_TAG: indices::SlotIndex = indices::SlotIndex::ZERO;
