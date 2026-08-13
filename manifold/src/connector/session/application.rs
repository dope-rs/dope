use std::marker;

use dope_core::{
    driver::{self, schedule},
    io::socket,
};
use dope_net::{link::egress, wire};
use o3::{buffer::resident, cell::region};

use crate::{
    connector::{
        self,
        app::{self, continuation},
        attempt, connection, lifecycle,
        session::{self, parser, retirement::Retirement as _},
    },
    receive, timing,
};

type WireMarker<'d, W> = (fn() -> W, fn(&'d ()) -> &'d ());

#[doc(hidden)]
pub struct Application<'d, const ID: u8, N: session::Session<'d, ID>, W: wire::Wire> {
    pub(super) session: N,
    pub(super) ingress_budget: resident::Budget<'d>,
    wire: marker::PhantomData<WireMarker<'d, W>>,
}

impl<'d, const ID: u8, N: session::Session<'d, ID>, W: wire::Wire> Application<'d, ID, N, W> {
    pub(crate) fn new(session: N, ingress_cap: usize, region: &region::Token<'d>) -> Self {
        Self {
            session,
            ingress_budget: resident::Budget::new(ingress_cap, region),
            wire: marker::PhantomData,
        }
    }

    pub(crate) const fn session(&self) -> &N {
        &self.session
    }

    pub(crate) fn session_mut(&mut self) -> &mut N {
        &mut self.session
    }
}
impl<'d, const ID: u8, N: session::Session<'d, ID>, W: wire::Wire> app::Application<'d, ID>
    for Application<'d, ID, N, W>
{
    type Conn = session::Connection<'d, ID, N>;
    type Wire = W;
    type Send = N::Send;
    type Input = receive::Retained;

    fn connection(&self) -> Self::Conn {
        session::Connection::new(&self.ingress_budget, self.session.codec())
    }
}

impl<'d, const ID: u8, N: session::Session<'d, ID>, W: wire::Wire> app::Receive<'d, ID>
    for Application<'d, ID, N, W>
{
    type Continuation = continuation::Resumable;
}

impl<'d, const ID: u8, N: session::Session<'d, ID>, W: wire::Wire> app::ResumableReceive<'d, ID>
    for Application<'d, ID, N, W>
{
    fn resume<'turn, O>(
        &mut self,
        permit: schedule::ApplicationPermit<'turn, 'd>,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> app::ResumeOutcome {
        use connector::app::ChunkOutcome;
        let (conn_id, conn, close_reason, work) = connection.into_parts();
        let region = driver.region_token();
        let session::Connection {
            ingress,
            parse_state,
            conn_state,
            ..
        } = conn;
        let mut context = session::Ctx {
            conn_id,
            state: conn_state,
            sink: egress,
            region,
            close_reason,
        };
        let drained = parser::Parser::new(
            &mut self.session,
            ingress,
            &self.ingress_budget,
            parse_state,
            &mut context,
            work,
        )
        .run_admitted(permit);
        let outcome = match drained {
            parser::Outcome::Close => ChunkOutcome::CloseReconnect,
            parser::Outcome::Yield => return app::ResumeOutcome::Yield,
            parser::Outcome::Capacity => ChunkOutcome::Capacity,
            parser::Outcome::Overrun => ChunkOutcome::Overrun,
            parser::Outcome::Complete => {
                match <N::ConnState as lifecycle::Lifecycle>::wants_close(conn_state) {
                    lifecycle::Close::Keep => ChunkOutcome::Ok,
                    lifecycle::Close::Reconnect => ChunkOutcome::CloseReconnect,
                    lifecycle::Close::Permanent => ChunkOutcome::ClosePermanent,
                }
            }
        };
        app::ResumeOutcome::Complete(outcome)
    }
}

impl<'d, const ID: u8, N: session::Session<'d, ID>, W: wire::Wire> app::RetainedReceive<'d, ID>
    for Application<'d, ID, N, W>
{
    const RETENTION: crate::receive::Retention = crate::receive::Retention::new(1, 0);

    fn retained_chunk<O>(
        &mut self,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, Self::Send>,
        mut chunk: <Self::Wire as wire::Wire>::RetainedRecv<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> app::ResumeOutcome {
        use connector::app::ChunkOutcome;
        let (conn_id, conn, close_reason, work) = connection.into_parts();
        let region = driver.region_token();
        let session::Connection {
            ingress,
            parse_state,
            conn_state,
            ..
        } = conn;
        let mut context = session::Ctx {
            conn_id,
            state: conn_state,
            sink: egress,
            region,
            close_reason,
        };
        let drained = parser::Parser::new(
            &mut self.session,
            ingress,
            &self.ingress_budget,
            parse_state,
            &mut context,
            work,
        )
        .run_retained(&mut chunk);
        let outcome = match drained {
            parser::Outcome::Close => ChunkOutcome::CloseReconnect,
            parser::Outcome::Yield => return app::ResumeOutcome::Yield,
            parser::Outcome::Capacity => ChunkOutcome::Capacity,
            parser::Outcome::Overrun => ChunkOutcome::Overrun,
            parser::Outcome::Complete => {
                match <N::ConnState as lifecycle::Lifecycle>::wants_close(conn_state) {
                    lifecycle::Close::Keep => ChunkOutcome::Ok,
                    lifecycle::Close::Reconnect => ChunkOutcome::CloseReconnect,
                    lifecycle::Close::Permanent => ChunkOutcome::ClosePermanent,
                }
            }
        };
        app::ResumeOutcome::Complete(outcome)
    }
}

impl<'d, const ID: u8, N: session::Retirement<'d, ID> + session::Scheduling<'d, ID>, W: wire::Wire>
    app::Lifecycle<'d, ID> for Application<'d, ID, N, W>
{
    fn connected<O>(
        &mut self,
        _key: attempt::Id<'d, ID>,
        peer: socket::Addr,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let ready = connection.wake_target();
        let (conn_id, conn, close_reason, _work) = connection.into_parts();
        conn.retirement_reason = None;
        self.session.activate(conn_id, ready);
        self.session.connect(
            peer,
            &mut session::Ctx {
                conn_id,
                state: &mut conn.conn_state,
                sink: egress,
                region: driver.region_token(),
                close_reason,
            },
        );
    }

    fn before_send<O>(
        &mut self,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let (conn_id, conn, close_reason, _work) = connection.into_parts();
        self.session.flush_trailer(&mut session::Ctx {
            conn_id,
            state: &mut conn.conn_state,
            sink: egress,
            region: driver.region_token(),
            close_reason,
        });
    }

    fn sent(&mut self, _connection: connection::Id<'d, ID>, _has_pending_egress: bool) {
        self.session.sent();
    }

    fn close<O>(
        &mut self,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, Self::Send>,
        reason: lifecycle::CloseReason,
        driver: &mut driver::Context<'_, 'd>,
    ) -> app::CloseOutcome {
        self.retire_connection(connection, egress, reason, driver)
    }

    fn close_kind<O>(
        &self,
        connection: connection::Ref<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        _driver: &mut driver::Context<'_, 'd>,
    ) -> Option<app::CloseKind> {
        use connector::lifecycle::Close;
        match <N::ConnState as lifecycle::Lifecycle>::wants_close(&connection.state().conn_state) {
            Close::Keep => None,
            Close::Reconnect => Some(app::CloseKind::Reconnect),
            Close::Permanent => Some(app::CloseKind::Permanent),
        }
    }

    fn defer_close<O>(
        &self,
        connection: connection::Ref<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> bool {
        self.session.defer_close(
            connection.id(),
            &connection.state().conn_state,
            driver.region_token(),
        )
    }

    fn is_drained<O>(
        &self,
        connection: connection::Ref<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> bool {
        self.session.is_drained(
            connection.id(),
            &connection.state().conn_state,
            driver.region_token(),
        )
    }

    fn inbound<O>(
        &self,
        connection: connection::Ref<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        default: timing::Window,
        driver: &mut driver::Context<'_, 'd>,
    ) -> app::Inbound {
        self.session.inbound(
            connection.id(),
            &connection.state().conn_state,
            default,
            driver.region_token(),
        )
    }
}

impl<'d, const ID: u8, N: session::Session<'d, ID>, W: wire::Wire> app::RequestSource<'d, ID>
    for Application<'d, ID, N, W>
{
    fn drain_requests(
        &self,
        token: connection::Id<'d, ID>,
        connection: &mut Self::Conn,
        drain: &mut app::RequestDrain<'_, 'd, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> app::Requests {
        self.session.drain_requests(
            token,
            &mut connection.parse_state,
            drain,
            driver.region_token(),
        )
    }
}

impl<'d, const ID: u8, N: session::Scheduling<'d, ID>, W: wire::Wire> app::Scheduling<'d, ID>
    for Application<'d, ID, N, W>
{
    fn pre_park<'turn>(
        &mut self,
        work: schedule::Application<'turn, 'd>,
        region: &mut region::Token<'d>,
    ) {
        self.session.pre_park(work, region);
    }

    fn shutdown(&mut self) {
        let _ = self;
    }

    fn progress(&self, region: &region::Token<'d>) -> schedule::Progress<'d> {
        self.session.progress(region)
    }
}
