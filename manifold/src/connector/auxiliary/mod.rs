use std::{convert, error, fmt, mem};

use dope_core::driver::schedule::{self, ready};
use dope_net::link::egress::data;
use o3::cell::region;

use crate::connector::{attempt, connection};

mod policy;
pub(crate) use policy::Policy;

#[must_use = "an auxiliary request must be submitted or settled terminally"]
pub struct Request<'d, B, const ID: u8 = 0> {
    ticket: Ticket<'d, ID>,
    payload: B,
}

impl<'d, B, const ID: u8> Request<'d, B, ID> {
    pub const fn new(target: connection::Id<'d, ID>, payload: B) -> Self {
        Self {
            ticket: Ticket { target },
            payload,
        }
    }

    pub const fn target(&self) -> connection::Id<'d, ID> {
        self.ticket.target
    }

    pub(crate) fn into_parts(self) -> (Ticket<'d, ID>, B) {
        (self.ticket, self.payload)
    }
}

#[must_use = "an auxiliary ticket must be returned through Control::complete"]
pub struct Ticket<'d, const ID: u8 = 0> {
    target: connection::Id<'d, ID>,
}

impl<'d, const ID: u8> Ticket<'d, ID> {
    pub const fn target(&self) -> connection::Id<'d, ID> {
        self.target
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Stale,
    Capacity,
    NoTarget,
    Connect,
    Wire,
    Transport,
    Timeout,
}

impl fmt::Display for Error {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(match self {
            Self::Stale => "auxiliary target is stale",
            Self::Capacity => "auxiliary capacity is exhausted",
            Self::NoTarget => "auxiliary target has no connected peer",
            Self::Connect => "auxiliary connection failed",
            Self::Wire => "auxiliary wire setup failed",
            Self::Transport => "auxiliary transport failed",
            Self::Timeout => "auxiliary delivery timed out",
        })
    }
}

impl error::Error for Error {}

pub trait Control<'d, B: data::Payload<'d>, const ID: u8 = 0>: Sized {
    fn start(&mut self, ready: ready::Target<'d>);

    fn has_requests(&self) -> bool;

    fn take_request<'turn>(
        &mut self,
        permit: schedule::ApplicationPermit<'turn, 'd>,
        region: &mut region::Token<'d>,
    ) -> Option<Request<'d, B, ID>>;

    fn has_cancellations(&self) -> bool;

    fn take_cancellation<'turn>(
        &mut self,
        permit: schedule::MaintenancePermit<'turn, 'd>,
        region: &mut region::Token<'d>,
    ) -> Option<connection::Id<'d, ID>>;

    fn complete(
        &mut self,
        ticket: Ticket<'d, ID>,
        result: Result<(), Error>,
        region: &mut region::Token<'d>,
    );

    fn stop(&mut self, region: &mut region::Token<'d>);
}

pub struct Disabled;

#[repr(transparent)]
pub struct Enabled<C> {
    pub(crate) control: C,
}

impl<C> Enabled<C> {
    pub const fn new(control: C) -> Self {
        Self { control }
    }
}

const _: () = assert!(mem::size_of::<Disabled>() == 0);
const _: () = assert!(mem::size_of::<Enabled<u64>>() == mem::size_of::<u64>());
const _: () = assert!(mem::align_of::<Enabled<u64>>() == mem::align_of::<u64>());

impl Policy for Disabled {}
impl<C> Policy for Enabled<C> {}

#[doc(hidden)]
pub trait Mode<'d, B: data::Payload<'d>, const ID: u8>: Policy {
    type Owner: Ownership<'d, ID> + Kind;
    type RequestAuthority;

    fn physical_capacity(primary: usize) -> Option<usize>;

    fn request_target(authority: &Self::RequestAuthority) -> connection::Id<'d, ID>;

    fn auxiliary(authority: Self::RequestAuthority) -> Self::Owner;

    fn into_ticket(authority: Self::RequestAuthority) -> Ticket<'d, ID>;

    fn start(&mut self, ready: ready::Target<'d>);

    fn has_requests(&self) -> bool;

    fn take_request<'turn>(
        &mut self,
        permit: schedule::ApplicationPermit<'turn, 'd>,
        region: &mut region::Token<'d>,
    ) -> Option<(Self::RequestAuthority, B)>;

    fn has_cancellations(&self) -> bool;

    fn take_cancellation<'turn>(
        &mut self,
        permit: schedule::MaintenancePermit<'turn, 'd>,
        region: &mut region::Token<'d>,
    ) -> Option<connection::Id<'d, ID>>;

    fn complete(
        &mut self,
        ticket: Ticket<'d, ID>,
        result: Result<(), Error>,
        region: &mut region::Token<'d>,
    );

    fn stop(&mut self, region: &mut region::Token<'d>);

    fn settle(
        &mut self,
        owner: &mut Self::Owner,
        result: Result<(), Error>,
        region: &mut region::Token<'d>,
    ) -> bool {
        let Some(ticket) = owner.take_ticket() else {
            return false;
        };
        self.complete(ticket, result, region);
        true
    }
}

impl<'d, B: data::Payload<'d>, const ID: u8> Mode<'d, B, ID> for Disabled {
    type Owner = Primary<'d, ID>;
    type RequestAuthority = convert::Infallible;

    fn physical_capacity(primary: usize) -> Option<usize> {
        Some(primary)
    }

    fn request_target(authority: &Self::RequestAuthority) -> connection::Id<'d, ID> {
        match *authority {}
    }

    fn auxiliary(authority: Self::RequestAuthority) -> Self::Owner {
        match authority {}
    }

    fn into_ticket(authority: Self::RequestAuthority) -> Ticket<'d, ID> {
        match authority {}
    }

    fn start(&mut self, _: ready::Target<'d>) {}

    fn has_requests(&self) -> bool {
        false
    }

    fn take_request<'turn>(
        &mut self,
        _: schedule::ApplicationPermit<'turn, 'd>,
        _: &mut region::Token<'d>,
    ) -> Option<(Self::RequestAuthority, B)> {
        None
    }

    fn has_cancellations(&self) -> bool {
        false
    }

    fn take_cancellation<'turn>(
        &mut self,
        _: schedule::MaintenancePermit<'turn, 'd>,
        _: &mut region::Token<'d>,
    ) -> Option<connection::Id<'d, ID>> {
        None
    }

    fn complete(&mut self, _: Ticket<'d, ID>, _: Result<(), Error>, _: &mut region::Token<'d>) {}

    fn stop(&mut self, _: &mut region::Token<'d>) {}
}

impl<'d, B, const ID: u8, C> Mode<'d, B, ID> for Enabled<C>
where
    B: data::Payload<'d>,
    C: Control<'d, B, ID>,
{
    type Owner = Mixed<'d, ID>;
    type RequestAuthority = Ticket<'d, ID>;

    fn physical_capacity(primary: usize) -> Option<usize> {
        primary.checked_mul(2)
    }

    fn request_target(ticket: &Self::RequestAuthority) -> connection::Id<'d, ID> {
        ticket.target()
    }

    fn auxiliary(ticket: Self::RequestAuthority) -> Self::Owner {
        Mixed::Auxiliary(Some(ticket))
    }

    fn into_ticket(ticket: Self::RequestAuthority) -> Ticket<'d, ID> {
        ticket
    }

    fn start(&mut self, ready: ready::Target<'d>) {
        self.control.start(ready);
    }

    fn has_requests(&self) -> bool {
        self.control.has_requests()
    }

    fn take_request<'turn>(
        &mut self,
        permit: schedule::ApplicationPermit<'turn, 'd>,
        region: &mut region::Token<'d>,
    ) -> Option<(Self::RequestAuthority, B)> {
        self.control
            .take_request(permit, region)
            .map(Request::into_parts)
    }

    fn has_cancellations(&self) -> bool {
        self.control.has_cancellations()
    }

    fn take_cancellation<'turn>(
        &mut self,
        permit: schedule::MaintenancePermit<'turn, 'd>,
        region: &mut region::Token<'d>,
    ) -> Option<connection::Id<'d, ID>> {
        self.control.take_cancellation(permit, region)
    }

    fn complete(
        &mut self,
        ticket: Ticket<'d, ID>,
        result: Result<(), Error>,
        region: &mut region::Token<'d>,
    ) {
        self.control.complete(ticket, result, region);
    }

    fn stop(&mut self, region: &mut region::Token<'d>) {
        self.control.stop(region);
    }
}

#[doc(hidden)]
#[repr(transparent)]
pub struct Primary<'d, const ID: u8>(attempt::Id<'d, ID>);

#[doc(hidden)]
pub trait Kind {
    fn is_auxiliary(&self) -> bool;
}

#[doc(hidden)]
pub trait Ownership<'d, const ID: u8>: Sized {
    fn primary(attempt: attempt::Id<'d, ID>) -> Self;

    fn attempt(&self) -> Option<attempt::Id<'d, ID>>;

    fn auxiliary_target(&self) -> Option<connection::Id<'d, ID>>;

    fn take_ticket(&mut self) -> Option<Ticket<'d, ID>>;
}

impl<'d, const ID: u8> Ownership<'d, ID> for Primary<'d, ID> {
    fn primary(attempt: attempt::Id<'d, ID>) -> Self {
        Self(attempt)
    }

    fn attempt(&self) -> Option<attempt::Id<'d, ID>> {
        Some(self.0)
    }

    fn auxiliary_target(&self) -> Option<connection::Id<'d, ID>> {
        None
    }

    fn take_ticket(&mut self) -> Option<Ticket<'d, ID>> {
        None
    }
}

impl<const ID: u8> Kind for Primary<'_, ID> {
    fn is_auxiliary(&self) -> bool {
        false
    }
}

#[doc(hidden)]
pub enum Mixed<'d, const ID: u8> {
    Primary(attempt::Id<'d, ID>),
    Auxiliary(Option<Ticket<'d, ID>>),
}

#[doc(hidden)]
#[derive(Clone, Copy)]
pub enum Event<'d, const ID: u8> {
    Primary(attempt::Id<'d, ID>),
    Auxiliary,
}

impl<'d, const ID: u8> Ownership<'d, ID> for Mixed<'d, ID> {
    fn primary(attempt: attempt::Id<'d, ID>) -> Self {
        Self::Primary(attempt)
    }

    fn attempt(&self) -> Option<attempt::Id<'d, ID>> {
        match self {
            Self::Primary(attempt) => Some(*attempt),
            Self::Auxiliary(_) => None,
        }
    }

    fn auxiliary_target(&self) -> Option<connection::Id<'d, ID>> {
        match self {
            Self::Primary(_) => None,
            Self::Auxiliary(ticket) => ticket.as_ref().map(Ticket::target),
        }
    }

    fn take_ticket(&mut self) -> Option<Ticket<'d, ID>> {
        match self {
            Self::Primary(_) => None,
            Self::Auxiliary(ticket) => ticket.take(),
        }
    }
}

impl<const ID: u8> Kind for Mixed<'_, ID> {
    fn is_auxiliary(&self) -> bool {
        matches!(self, Self::Auxiliary(_))
    }
}

const _: () =
    assert!(mem::size_of::<Primary<'static, 0>>() == mem::size_of::<attempt::Id<'static, 0>>());
