use std::{marker, mem, net};

use dope_core::{
    driver::{
        result,
        route::{Identity, indices},
        schedule::clock,
    },
    io::{datagram, datagram::source, recv, socket::msg, transfer},
};
use o3::buffer::{bytes::bounded, pool};

use crate::network::datagram::{RejectCause, packet};

pub(super) mod flights;
pub(super) mod kernel;
pub mod owners;
pub(super) mod pending;
pub(super) mod retained;
mod sealed;
pub(super) use sealed::Sealed;
mod stage;

pub(super) use stage::{Draft, Stage};

pub enum Outcome {
    Sent(transfer::Len),
    Failed(dope_core::Errno),
    Submit(result::SubmitError),
    Shutdown,
}

impl From<Result<transfer::Len, dope_core::Errno>> for Outcome {
    fn from(outcome: Result<transfer::Len, dope_core::Errno>) -> Self {
        match outcome {
            Ok(bytes) => Self::Sent(bytes),
            Err(error) => Self::Failed(error),
        }
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum Owner<'d, ID: Identity> {
    Queued(owners::Queued<'d, ID>),
    Flight(owners::Flight<'d, ID>),
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Transfer<'d, ID: Identity> {
    queued: owners::Queued<'d, ID>,
    flight: owners::Flight<'d, ID>,
}

impl<'d, ID: Identity> Transfer<'d, ID> {
    pub(crate) const fn new(
        queued: owners::Queued<'d, ID>,
        flight: owners::Flight<'d, ID>,
    ) -> Self {
        Self { queued, flight }
    }

    pub const fn queued(self) -> owners::Queued<'d, ID> {
        self.queued
    }

    pub const fn flight(self) -> owners::Flight<'d, ID> {
        self.flight
    }
}

pub enum Payload<'turn, 'd, B = pool::Cursor> {
    Owned(bounded::Value<Vec<u8>>),
    Buffer(bounded::Value<B>),
    Packet(packet::Packet<'turn, 'd>),
}

#[must_use = "a terminal datagram settlement owns its retired payload"]
pub struct Settlement<'turn, 'd, ID: Identity, B = pool::Cursor> {
    owner: Owner<'d, ID>,
    destination: net::SocketAddr,
    outcome: Outcome,
    payload: Payload<'turn, 'd, B>,
    settled_at: clock::Moment<'d>,
}

impl<'turn, 'd, ID: Identity, B> Settlement<'turn, 'd, ID, B> {
    pub(in crate::network::datagram) fn new(
        owner: Owner<'d, ID>,
        peer: net::SocketAddr,
        outcome: Outcome,
        payload: Payload<'turn, 'd, B>,
        at: clock::Moment<'d>,
    ) -> Self {
        Self {
            owner,
            destination: peer,
            outcome,
            payload,
            settled_at: at,
        }
    }

    pub const fn owner(&self) -> Owner<'d, ID> {
        self.owner
    }

    pub const fn destination(&self) -> net::SocketAddr {
        self.destination
    }

    pub const fn outcome(&self) -> &Outcome {
        &self.outcome
    }

    pub const fn payload(&self) -> &Payload<'turn, 'd, B> {
        &self.payload
    }

    pub const fn settled_at(&self) -> clock::Moment<'d> {
        self.settled_at
    }

    pub fn into_parts(
        self,
    ) -> (
        Owner<'d, ID>,
        net::SocketAddr,
        Outcome,
        Payload<'turn, 'd, B>,
        clock::Moment<'d>,
    ) {
        (
            self.owner,
            self.destination,
            self.outcome,
            self.payload,
            self.settled_at,
        )
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Phase {
    Ready,
    Deferred,
    Failed(result::SubmitError),
    Stopped,
}

pub(super) enum Pass {
    Run,
    Stop,
}

impl Phase {
    pub(super) fn can_accept(self) -> bool {
        self.rejection().is_none()
    }

    pub(super) fn rejection(self) -> Option<RejectCause> {
        match self {
            Self::Ready | Self::Deferred => None,
            Self::Failed(error) => Some(RejectCause::Submit(error)),
            Self::Stopped => Some(RejectCause::Inactive),
        }
    }

    pub(super) fn can_submit(self) -> bool {
        self == Self::Ready
    }

    pub(super) fn begin(&mut self) -> Pass {
        match self {
            Self::Ready => Pass::Run,
            Self::Deferred => {
                *self = Self::Ready;
                Pass::Run
            }
            Self::Failed(_) | Self::Stopped => Pass::Stop,
        }
    }

    pub(super) fn stop(&mut self) {
        if self.can_accept() {
            *self = Self::Stopped;
        }
    }

    pub(super) fn retirement(self) -> Outcome {
        match self {
            Self::Failed(error) => Outcome::Submit(error),
            Self::Ready | Self::Deferred | Self::Stopped => Outcome::Shutdown,
        }
    }
}

pub(super) struct Queued<'d, B = pool::Cursor, M: Mode = Plain> {
    pub(super) addr: net::SocketAddr,
    pub(super) retained: retained::Retained<'d, B>,
    pub(super) control: M::Queued,
}

impl<'d, B, M: Mode> Queued<'d, B, M> {
    pub(super) fn new(
        retained: retained::Retained<'d, B>,
        addr: net::SocketAddr,
        control: M::Queued,
    ) -> Self {
        Self {
            addr,
            retained,
            control,
        }
    }
}
const _: () = assert!(mem::size_of::<bounded::Value<Vec<u8>>>() == mem::size_of::<Vec<u8>>());
const _: () =
    assert!(mem::size_of::<bounded::Value<pool::Cursor>>() == mem::size_of::<pool::Cursor>());
const _: () =
    assert!(mem::size_of::<bounded::Value<recv::Lease<'_>>>() == mem::size_of::<recv::Lease<'_>>());

const _: () = {
    assert!(mem::size_of::<kernel::Payload<'static>>() == 4 * mem::size_of::<usize>());
    assert!(mem::size_of::<Payload<'static, 'static>>() == 4 * mem::size_of::<usize>());
};

pub struct Plain;
pub struct Sourced<G = ()>(marker::PhantomData<fn() -> G>);
pub(in crate::network::datagram) struct Routed;

pub trait Mode: Sealed {
    type Queued;
    type Flight;

    fn prepare(control: Self::Queued) -> Self::Flight;
    fn restore(control: Self::Flight) -> Self::Queued;
    fn attach<'a>(control: &'a mut Self::Flight, message: msg::Builder<'a>) -> msg::Builder<'a>;
}

/// Destinations that preserve optional segmentation through queue admission.
pub trait Segmented: Mode {
    type Address;

    fn address(address: &Self::Address) -> net::SocketAddr;
    fn control(address: Self::Address, segment: Option<datagram::GsoSegment>) -> Self::Queued;
}

impl Mode for Plain {
    type Queued = Option<datagram::GsoSegment>;
    type Flight = Option<datagram::GsoControl>;

    fn prepare(control: Self::Queued) -> Self::Flight {
        control.map(datagram::GsoControl::new)
    }

    fn restore(control: Self::Flight) -> Self::Queued {
        control.map(datagram::GsoControl::into_segment)
    }

    fn attach<'a>(
        control: &'a mut Self::Flight,
        mut message: msg::Builder<'a>,
    ) -> msg::Builder<'a> {
        if let Some(control) = control {
            control.attach(&mut message);
        }
        message
    }
}

impl Mode for Routed {
    type Queued = indices::SlotIndex;
    type Flight = indices::SlotIndex;

    fn prepare(control: Self::Queued) -> Self::Flight {
        control
    }

    fn restore(control: Self::Flight) -> Self::Queued {
        control
    }

    fn attach<'a>(_: &'a mut Self::Flight, message: msg::Builder<'a>) -> msg::Builder<'a> {
        message
    }
}

impl Mode for Sourced {
    type Queued = source::Control;
    type Flight = source::Control;

    fn prepare(control: Self::Queued) -> Self::Flight {
        control
    }

    fn restore(control: Self::Flight) -> Self::Queued {
        control
    }

    fn attach<'a>(
        control: &'a mut Self::Flight,
        mut message: msg::Builder<'a>,
    ) -> msg::Builder<'a> {
        message.control(<source::Control as AsRef<[u8]>>::as_ref(control));
        message
    }
}

impl Mode for Sourced<datagram::Gso> {
    type Queued = (source::Control, Option<datagram::GsoSegment>);
    type Flight = source::Segmented;

    fn prepare((source, segment): Self::Queued) -> Self::Flight {
        source::Segmented::new(source, segment)
    }

    fn restore(control: Self::Flight) -> Self::Queued {
        control.into_parts()
    }

    fn attach<'a>(
        control: &'a mut Self::Flight,
        mut message: msg::Builder<'a>,
    ) -> msg::Builder<'a> {
        control.attach(&mut message);
        message
    }
}

impl Segmented for Plain {
    type Address = net::SocketAddr;

    fn address(address: &Self::Address) -> net::SocketAddr {
        *address
    }

    fn control(_: Self::Address, segment: Option<datagram::GsoSegment>) -> Self::Queued {
        segment
    }
}

impl Segmented for Sourced<datagram::Gso> {
    type Address = source::Source;

    fn address(address: &Self::Address) -> net::SocketAddr {
        address.peer()
    }

    fn control(address: Self::Address, segment: Option<datagram::GsoSegment>) -> Self::Queued {
        (address.control(), segment)
    }
}

impl Sealed for Plain {}
impl Sealed for Routed {}
impl Sealed for Sourced {}
impl Sealed for Sourced<datagram::Gso> {}

const _: () = {
    assert!(mem::size_of::<Sourced>() == 0);
    assert!(mem::size_of::<Sourced<datagram::Gso>>() == 0);
    assert!(
        mem::size_of::<<Plain as Mode>::Queued>() == mem::size_of::<Option<datagram::GsoSegment>>()
    );
    assert!(mem::size_of::<<Sourced as Mode>::Queued>() == mem::size_of::<source::Control>());
    assert!(mem::size_of::<<Sourced as Mode>::Flight>() == mem::size_of::<source::Control>());
};
