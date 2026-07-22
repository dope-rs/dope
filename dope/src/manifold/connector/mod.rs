mod app;
mod codec;
mod core;
mod lifecycle;
mod port;
mod session;
pub mod source;
pub mod state;

pub use core::Core;
use std::marker::PhantomData;

pub use app::{ChunkOutcome, CloseKind, ConnApp, Requests};
pub use codec::Codec;
pub use lifecycle::{Close, Lifecycle, Stateless};
pub use port::{Port, Receiver, Sender};
pub use session::Session;
pub use state::Ctx;

use crate::DriverContext;
use crate::manifold::connector::source::{DialKey, Static};
use crate::manifold::connector::state::State;
use crate::manifold::env::{Bundle, Env};
use crate::runtime::profile::Balanced;
use dope_core::driver::token::Token;
use dope_net::link::slot::Slot;
use dope_net::tcp::Tcp;
use dope_net::wire::Wire;
use dope_net::wire::identity::Identity;
use o3::buffer::{RetainBytes, SnapshotBuf};

const INGRESS_BUF_CAP: usize = 16 * 1024 * 1024;
const INGRESS_INITIAL_CAP: usize = 16 * 1024;

pub struct SessionConn<'d, N: Session<'d>> {
    ingress: SnapshotBuf<INGRESS_BUF_CAP>,
    parse_state: <N::Codec as Codec>::ParseState,
    conn_state: N::ConnState,
    driver: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, N: Session<'d>> Default for SessionConn<'d, N> {
    fn default() -> Self {
        Self {
            ingress: SnapshotBuf::with_capacity(INGRESS_INITIAL_CAP),
            parse_state: <N::Codec as Codec>::ParseState::default(),
            conn_state: N::ConnState::default(),
            driver: PhantomData,
        }
    }
}

type Marker<'d, W> = (fn() -> W, fn(&'d ()) -> &'d ());

pub struct SessionApp<'d, N: Session<'d>, W: Wire> {
    session: N,
    _w: PhantomData<Marker<'d, W>>,
}

impl<'d, N: Session<'d>, W: Wire> ConnApp<'d> for SessionApp<'d, N, W> {
    type Conn = SessionConn<'d, N>;
    type Wire = W;
    type Send = N::Send;

    fn chunk<R: RetainBytes>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        chunk: R,
        driver: &mut DriverContext<'_, 'd>,
    ) -> ChunkOutcome {
        if chunk.is_empty() {
            return ChunkOutcome::Ok;
        }
        let conn_id = slot.token();
        let State { conn, egress, .. } = &mut slot.state;
        let SessionConn {
            ingress,
            parse_state,
            conn_state,
            ..
        } = conn;
        if ingress.try_extend_from_slice(chunk.as_slice()).is_err() {
            return ChunkOutcome::Overrun;
        }
        loop {
            let Some(pending) = ingress.snapshot() else {
                break;
            };
            let Some((head, consumed)) = self.session.codec().parse(parse_state, &pending) else {
                break;
            };
            if consumed == 0 || consumed > pending.len() {
                break;
            }
            ingress.advance(consumed);
            drop(pending);
            self.session.response(
                head,
                &mut Ctx {
                    conn_id,
                    state: conn_state,
                    sink: egress,
                    region: driver.region_token(),
                },
            );
        }
        match conn_state.wants_close() {
            Close::Keep => ChunkOutcome::Ok,
            Close::Reconnect => ChunkOutcome::CloseReconnect,
            Close::Permanent => ChunkOutcome::ClosePermanent,
        }
    }

    fn connected(
        &mut self,
        _key: DialKey,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let conn_id = slot.token();
        self.session
            .activate(conn_id, slot.ready_key(), driver.region_token());
        let State { conn, egress, .. } = &mut slot.state;
        self.session.connect(&mut Ctx {
            conn_id,
            state: &mut conn.conn_state,
            sink: egress,
            region: driver.region_token(),
        });
    }

    fn before_send(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let conn_id = slot.token();
        let State { conn, egress, .. } = &mut slot.state;
        self.session.flush_trailer(&mut Ctx {
            conn_id,
            state: &mut conn.conn_state,
            sink: egress,
            region: driver.region_token(),
        });
    }

    fn send(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        sent: usize,
        _driver: &mut DriverContext<'_, 'd>,
    ) {
        self.session.sent(slot.token(), sent);
    }

    fn close(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let conn_id = slot.token();
        let State { conn, egress, .. } = &mut slot.state;
        self.session.disconnect(&mut Ctx {
            conn_id,
            state: &mut conn.conn_state,
            sink: egress,
            region: driver.region_token(),
        });
    }

    fn defer_close(
        &self,
        slot: &Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        self.session.defer_close(
            slot.token(),
            &slot.state.conn.conn_state,
            driver.region_token(),
        )
    }

    fn is_drained(
        &self,
        slot: &Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        self.session.is_drained(
            slot.token(),
            &slot.state.conn.conn_state,
            driver.region_token(),
        )
    }

    fn drain_requests(
        &self,
        token: Token,
        push: impl FnMut(Self::Send) -> Result<(), Self::Send>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Requests {
        self.session
            .drain_requests(token, push, driver.region_token())
    }

    fn pre_park(&mut self) {
        self.session.pre_park();
    }

    fn idle(&self) -> crate::runtime::Idle {
        self.session.idle()
    }

    fn inbound_idle_timeout(&self) -> Option<std::time::Duration> {
        self.session.inbound_idle_timeout()
    }
}

pub type Connector<'d, const ID: u8, N, S = Static<Tcp>, E = Bundle<Tcp, Identity, Balanced>> =
    Core<'d, ID, SessionApp<'d, N, <E as Env>::Wire>, S, E>;
