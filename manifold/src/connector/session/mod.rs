use std::marker;

use dope_core::{
    driver::schedule::{self, ready},
    io::socket,
};
use dope_net::link::egress::{self, data};
use o3::{buffer::resident, cell::region};

use crate::{
    connector::{self, app, codec, connection, lifecycle},
    timing,
};

mod application;
mod parser;
mod retirement;

pub(crate) use application::Application;

pub(crate) const INGRESS_BUF_CAP: usize = u32::MAX as usize;
pub(crate) const DEFAULT_INGRESS_CAP: usize = 16 * 1024 * 1024;

#[doc(hidden)]
pub struct Connection<'d, const ID: u8, N: Session<'d, ID>> {
    ingress: resident::Snapshot<'d, { INGRESS_BUF_CAP }>,
    parse_state: <N::Codec as codec::Codec>::ParseState,
    conn_state: N::ConnState,
    retirement_reason: Option<lifecycle::CloseReason>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, const ID: u8, N: Session<'d, ID>> Connection<'d, ID, N> {
    fn new(budget: &resident::Budget<'d>, codec: &N::Codec) -> Self {
        Self {
            ingress: resident::Snapshot::new(budget),
            parse_state: <N::Codec as codec::Codec>::parse_state(codec),
            conn_state: N::ConnState::default(),
            retirement_reason: None,
            driver: marker::PhantomData,
        }
    }
}

pub enum AdmittedSettlement<'turn, 'd> {
    Available(schedule::ApplicationPermit<'turn, 'd>),
    Consumed,
    Yield,
}

pub struct Ctx<'a, 'd, N: Session<'d, ID>, const ID: u8 = 0> {
    pub conn_id: connection::Id<'d, ID>,
    pub state: &'a mut N::ConnState,
    pub sink: egress::Queue<'a, 'd, { connector::IOV_CAP }, N::Send>,
    pub region: &'a mut region::Token<'d>,
    close_reason: &'a mut Option<lifecycle::CloseReason>,
}

impl<'a, 'd, N: Session<'d, ID>, const ID: u8> Ctx<'a, 'd, N, ID> {
    /// Records the first protocol-level terminal cause for this epoch.
    pub fn close_with(&mut self, reason: lifecycle::CloseReason) {
        if self.close_reason.is_none() {
            *self.close_reason = Some(reason);
        }
    }
}

pub trait Session<'d, const ID: u8 = 0>: Sized {
    type Codec: codec::Codec;
    type ConnState: lifecycle::Lifecycle;
    type Send: data::Payload<'d>;

    fn codec(&self) -> &Self::Codec;

    fn activate(&self, connection: connection::Id<'d, ID>, ready: ready::Target<'d>) {
        let _ = (connection, ready);
    }

    fn connect(&mut self, peer: socket::Addr, context: &mut Ctx<'_, 'd, Self, ID>);

    fn response<'input>(
        &mut self,
        head: <Self::Codec as codec::Codec>::Head<'input, 'd>,
        context: &mut Ctx<'_, 'd, Self, ID>,
    ) where
        'd: 'input;

    /// Activates protocol state for a payload after its transfer into the
    /// exact connection's egress queue has committed.
    fn begin(
        &self,
        connection: connection::Id<'d, ID>,
        parser: &mut <Self::Codec as codec::Codec>::ParseState,
    ) {
        let _ = (connection, parser);
    }

    /// Settles response-side work produced by the last parsed frame.
    /// Returning false preserves the connection generation and prevents the
    /// parser from advancing to the next frame until a later budgeted turn.
    fn settle_responses<'turn>(
        &mut self,
        work: schedule::Application<'turn, 'd>,
        context: &mut Ctx<'_, 'd, Self, ID>,
    ) -> bool {
        let _ = (work, context);
        true
    }

    fn settle_responses_admitted<'turn>(
        &mut self,
        permit: schedule::ApplicationPermit<'turn, 'd>,
        work: schedule::Application<'turn, 'd>,
        context: &mut Ctx<'_, 'd, Self, ID>,
    ) -> AdmittedSettlement<'turn, 'd> {
        let _ = (work, context);
        AdmittedSettlement::Available(permit)
    }

    /// Receives malformed inbound protocol bytes.  The connector forces a
    /// recoverable close after this hook returns, independently of the
    /// connection state's normal close request.
    fn protocol_error(
        &mut self,
        error: <Self::Codec as codec::Codec>::Error,
        context: &mut Ctx<'_, 'd, Self, ID>,
    ) {
        let _ = error;
        context.close_with(lifecycle::CloseReason::Protocol);
    }

    fn flush_trailer(&mut self, context: &mut Ctx<'_, 'd, Self, ID>) {
        let _ = context;
    }

    fn sent(&self) {}

    fn drain_requests(
        &self,
        connection: connection::Id<'d, ID>,
        parser: &mut <Self::Codec as codec::Codec>::ParseState,
        drain: &mut app::RequestDrain<'_, 'd, Self::Send>,
        region: &mut region::Token<'d>,
    ) -> app::Requests {
        use connector::app::Requests;
        let _ = (connection, parser, drain, region);
        Requests::default()
    }
}

/// Owns the ordered retirement of one exact connection generation.
///
/// This is a static capability: it adds no runtime object or session state.
pub trait Retirement<'d, const ID: u8 = 0>: Session<'d, ID> {
    /// Quarantines the exact connection generation before any multi-turn
    /// request or response retirement begins.
    fn begin_retirement(
        &self,
        connection: connection::Id<'d, ID>,
        reason: lifecycle::CloseReason,
        region: &mut region::Token<'d>,
    ) {
        let _ = (connection, reason, region);
    }

    fn retire_requests<'turn>(
        &self,
        connection: connection::Id<'d, ID>,
        work: schedule::Application<'turn, 'd>,
        region: &mut region::Token<'d>,
    ) -> egress::ClearProgress {
        let _ = (connection, work, region);
        egress::ClearProgress::Done
    }

    /// Retry retains the exact connection generation in close phase without releasing its slot.
    fn retire_responses<'turn>(
        &self,
        connection: connection::Id<'d, ID>,
        reason: lifecycle::CloseReason,
        work: schedule::Application<'turn, 'd>,
        region: &mut region::Token<'d>,
    ) -> egress::ClearProgress {
        let _ = (connection, reason, work, region);
        egress::ClearProgress::Done
    }

    fn defer_close(
        &self,
        connection: connection::Id<'d, ID>,
        state: &Self::ConnState,
        region: &mut region::Token<'d>,
    ) -> bool {
        let _ = (connection, region);
        <Self::ConnState as lifecycle::Lifecycle>::defer_close(state)
    }

    fn is_drained(
        &self,
        connection: connection::Id<'d, ID>,
        state: &Self::ConnState,
        region: &mut region::Token<'d>,
    ) -> bool {
        let _ = (connection, region);
        <Self::ConnState as lifecycle::Lifecycle>::is_drained(state)
    }

    fn disconnect(&mut self, context: &mut Ctx<'_, 'd, Self, ID>, reason: lifecycle::CloseReason);
}

/// Supplies executor-facing scheduling policy for a protocol session.
///
/// This is a static capability: it adds no runtime object or session state.
pub trait Scheduling<'d, const ID: u8 = 0>: Session<'d, ID> {
    fn pre_park<'turn>(
        &mut self,
        work: schedule::Application<'turn, 'd>,
        region: &mut region::Token<'d>,
    ) {
        let _ = (work, region);
    }

    fn progress(&self, region: &region::Token<'d>) -> schedule::Progress<'d> {
        let _ = region;
        schedule::Progress::Quiescent
    }

    fn inbound(
        &self,
        connection: connection::Id<'d, ID>,
        state: &Self::ConnState,
        default: timing::Window,
        region: &mut region::Token<'d>,
    ) -> app::Inbound {
        let _ = (connection, state, default, region);
        app::Inbound::Quiescent
    }
}

/// Declares that a protocol session supports one exact service route and
/// connection-capacity bound.
///
/// Implementations are deliberately explicit: implementing [`Session`] does
/// not make a protocol compatible with every service connector.
///
/// A target for one route does not satisfy another route:
///
/// ```compile_fail,E0277
/// use dope_manifold::connector::session::Target;
///
/// fn require<'d, N: Target<'d, 8, 1>>() {}
///
/// fn wrong_route<'d, N: Target<'d, 7, 1>>() {
///     require::<'d, N>();
/// }
/// ```
///
/// A target for one connection bound does not silently opt into a larger one:
///
/// ```compile_fail,E0277
/// use dope_manifold::connector::session::Target;
///
/// fn require<'d, N: Target<'d, 7, 2>>() {}
///
/// fn unsupported_capacity<'d, N: Target<'d, 7, 1>>() {
///     require::<'d, N>();
/// }
/// ```
pub trait Target<'d, const ID: u8, const MAX_CONNECTIONS: usize>:
    Session<'d, ID> + Retirement<'d, ID> + Scheduling<'d, ID>
{
}
