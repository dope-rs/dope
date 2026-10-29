//! Capabilities branded to one connector driver turn and its sub-operations.

use std::mem;

use dope_core::driver::{
    self,
    route::{Identity, KeyTag},
    schedule::{app::Work, clock, ready},
};
use dope_net::{
    link::egress::{self, data},
    wire,
};
use o3::base::cell::region;

use crate::{
    client::{
        self, close, conn,
        session::{self, app},
    },
    service::bindings::{Bound, endpoint},
};

pub struct Context<'a, 'e, 'd, C, B: data::raw::Payload<'d>, ID: Identity = KeyTag<0>> {
    pub conn_id: conn::Id<'d, ID>,
    pub state: &'a mut C,
    pub sink: egress::Queue<'a, 'e, 'd, { client::IOV_CAP }, B>,
    ready: ready::Target<'d>,
    turn: driver::Context<'a, 'd>,
    closing: conn::CloseRequest<'a, 'd, ID>,
}

/// One connection-establishment turn with a single identity source.
#[must_use = "startup turn owns the connection lifetime and mutable capabilities"]
pub struct Startup<'a, 'e, 'd, C, B: data::raw::Payload<'d>, ID: Identity = KeyTag<0>> {
    pub lifetime: conn::Lifetime<'d, ID>,
    pub state: &'a mut C,
    pub sink: egress::Queue<'a, 'e, 'd, { client::IOV_CAP }, B>,
    pub ready: ready::Target<'d>,
    pub driver: driver::Context<'a, 'd>,
    pub close: conn::CloseRequest<'a, 'd, ID>,
}

impl<'a, 'e, 'd, C, B: data::raw::Payload<'d>, ID: Identity> Startup<'a, 'e, 'd, C, B, ID> {
    pub(super) fn new(
        lifetime: conn::Lifetime<'d, ID>,
        state: &'a mut C,
        sink: egress::Queue<'a, 'e, 'd, { client::IOV_CAP }, B>,
        ready: ready::Target<'d>,
        driver: driver::Context<'a, 'd>,
        close: conn::CloseRequest<'a, 'd, ID>,
    ) -> Self {
        Self {
            lifetime,
            state,
            sink,
            ready,
            driver,
            close,
        }
    }

    pub const fn connection(&self) -> conn::Id<'d, ID> {
        self.lifetime.connection()
    }
}

/// Disjoint projection of every protocol-mutable callback capability.
/// It is an ephemeral reborrow created only from [`Context`].
pub struct Parts<'a, 'e, 'd, C, B: data::raw::Payload<'d>, ID: Identity = KeyTag<0>> {
    pub conn_id: conn::Id<'d, ID>,
    pub state: &'a mut C,
    pub sink: egress::Queue<'a, 'e, 'd, { client::IOV_CAP }, B>,
    pub driver: driver::Context<'a, 'd>,
    pub close: conn::CloseRequest<'a, 'd, ID>,
}

impl<'a, 'e, 'd, C, B: data::raw::Payload<'d>, ID: Identity> Parts<'a, 'e, 'd, C, B, ID> {
    pub fn request(&mut self, rule: close::Directive) {
        self.close.request(rule)
    }
}

/// Retirement work and the exact driver turn that admits it.
pub struct Retirement<'a, 'turn, 'd> {
    work: Work<'turn, 'd>,
    turn: driver::Context<'a, 'd>,
}

/// Move-only final context for one transport-drained connection generation.
/// The invariant source survives every public projection independently of `state`.
#[must_use = "retirement context owns the final transport proof"]
pub struct Retired<'a, 'd, C, ID: Identity, S> {
    transport: Drained<'d, ID, S>,
    pub state: &'a mut C,
    turn: driver::Context<'a, 'd>,
}

impl<'a, 'd, ID: Identity, N: session::Parser<'d, ID>> Retired<'a, 'd, N::ConnState, ID, N> {
    pub(super) fn new<'w, W>(
        retired: conn::Retired<'d, ID, app::State<'w, 'd, ID, N, W>>,
        state: &'a mut N::ConnState,
        turn: driver::Context<'a, 'd>,
    ) -> Self
    where
        'd: 'w,
        W: wire::Spec + 'd,
    {
        Self {
            transport: Drained::new(retired.into_session()),
            state,
            turn,
        }
    }
}

impl<'a, 'd, C, ID: Identity, S> Retired<'a, 'd, C, ID, S> {
    pub const fn connection(&self) -> conn::Id<'d, ID> {
        self.transport.connection()
    }

    pub const fn reason(&self) -> close::Reason {
        self.transport.reason()
    }

    pub const fn transmit(&self) -> Option<&conn::Transmit> {
        self.transport.retired.transmit()
    }

    pub fn state_token(&mut self) -> &mut region::Token<region::Identity<'d, 1>> {
        self.turn.state_token()
    }

    pub fn into_drained(self) -> Drained<'d, ID, S> {
        self.transport
    }

    pub fn into_parts(
        self,
    ) -> (
        Drained<'d, ID, S>,
        &'a mut C,
        &'a mut region::Token<region::Identity<'d>>,
    ) {
        (self.transport, self.state, &mut self.turn.into_regions().0)
    }
}

impl<'a, 'life, 'd: 'life, C, P: ?Sized, L, F, ID: Identity, const MAX: usize>
    Retired<'a, 'd, C, ID, Bound<'life, 'd, P, L, ID, MAX, F>>
{
    pub(crate) fn into_policy(
        self,
    ) -> Retired<'a, 'd, C, ID, endpoint::State<'life, 'd, P, L, ID, MAX>> {
        Retired {
            transport: Drained::new(self.transport.retired.into_policy()),
            state: self.state,
            turn: self.turn,
        }
    }
}

impl<'a, 'e, 'd, C, B: data::raw::Payload<'d>, ID: Identity> Context<'a, 'e, 'd, C, B, ID> {
    pub(super) fn new(
        conn_id: conn::Id<'d, ID>,
        state: &'a mut C,
        sink: egress::Queue<'a, 'e, 'd, { client::IOV_CAP }, B>,
        ready: ready::Target<'d>,
        turn: driver::Context<'a, 'd>,
        closing: conn::CloseRequest<'a, 'd, ID>,
    ) -> Self {
        Self {
            conn_id,
            state,
            sink,
            ready,
            turn,
            closing,
        }
    }

    pub fn now(&self) -> clock::Moment<'d> {
        self.turn.driver_ref().clock().now()
    }

    pub fn region(&mut self) -> &mut region::Token<region::Identity<'d>> {
        self.turn.region_token()
    }

    /// Returns the generation-checked scheduler target for this connection.
    pub fn wake_target(&self) -> ready::Target<'d> {
        self.ready
    }

    /// Reborrows state, egress, driver, and close authority together.
    pub fn parts(&mut self) -> Parts<'_, 'e, 'd, C, B, ID> {
        Parts {
            conn_id: self.conn_id,
            state: &mut *self.state,
            sink: self.sink.reborrow(),
            driver: self.turn.reborrow(),
            close: self.closing.reborrow(),
        }
    }

    pub(super) fn retirement<'turn>(&mut self, work: Work<'turn, 'd>) -> Retirement<'_, 'turn, 'd> {
        Retirement::new(work, self.turn.reborrow())
    }

    pub fn request(&mut self, rule: close::Directive) {
        self.closing.request(rule)
    }

    pub fn close_status(&self) -> conn::CloseStatus {
        self.closing.status()
    }
}

impl<'a, 'turn, 'd> Retirement<'a, 'turn, 'd> {
    pub(super) fn new(work: Work<'turn, 'd>, turn: driver::Context<'a, 'd>) -> Self {
        Self { work, turn }
    }

    pub fn now(&self) -> clock::Moment<'d> {
        self.turn.driver_ref().clock().now()
    }

    pub fn work(&self) -> Work<'turn, 'd> {
        self.work
    }

    pub fn state_token(&mut self) -> &mut region::Token<region::Identity<'d, 1>> {
        self.turn.state_token()
    }

    pub fn parts(&mut self) -> (Work<'turn, 'd>, &mut region::Token<region::Identity<'d>>) {
        (self.work, self.turn.region_token())
    }
}

/// Proof that transport ownership and the application drain barrier have ended
/// for one exact connection generation and invariant session source.
#[must_use = "transport retirement must be paired with protocol deactivation"]
pub struct Drained<'d, ID: Identity, S> {
    retired: conn::Retired<'d, ID, S>,
}

impl<'d, ID: Identity, S> Drained<'d, ID, S> {
    fn new(retired: conn::Retired<'d, ID, S>) -> Self {
        Self { retired }
    }

    pub const fn connection(&self) -> conn::Id<'d, ID> {
        self.retired.connection()
    }

    pub const fn reason(&self) -> close::Reason {
        self.retired.reason()
    }
}

const _: () =
    assert!(mem::size_of::<Retirement<'static, 'static, 'static>>() == 2 * mem::size_of::<usize>());
const _: () = assert!(
    mem::size_of::<Startup<'static, 'static, 'static, (), data::Inline<0>, KeyTag<0>>>()
        + mem::size_of::<conn::Id<'static, KeyTag<0>>>()
        == mem::size_of::<Context<'static, 'static, 'static, (), data::Inline<0>, KeyTag<0>>>()
            + mem::size_of::<conn::Lifetime<'static, KeyTag<0>>>()
);
const _: () = assert!(
    mem::size_of::<Drained<'static, KeyTag<0>, ()>>()
        == mem::size_of::<conn::Retired<'static, KeyTag<0>, ()>>()
);
