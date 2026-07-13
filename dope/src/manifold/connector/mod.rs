mod app;
mod core;
mod protocol;
pub mod session;
pub mod source;

pub use core::Core;
use std::marker::PhantomData;

pub use app::{ChunkOutcome, ConnApp};
pub use protocol::{Close, Codec, Lifecycle, Session, Stateless};
pub use session::Ctx;

use crate::manifold::buf;
use crate::manifold::connector::session::State;
use crate::manifold::connector::source::Static;
use crate::manifold::env::{Bundle, Env};
use crate::transport::Tcp;
use crate::transport::link::Slot;
use crate::transport::wire::{Identity, RecvChunk, Wire};
use crate::{Driver, backend};

const INGRESS_BUF_CAP: usize = 16 * 1024 * 1024;

pub struct SessionConn<N: Session> {
    ingress: buf::Accum<INGRESS_BUF_CAP>,
    parse_state: <N::Codec as Codec>::ParseState,
    conn_state: N::ConnState,
}

impl<N: Session> Default for SessionConn<N> {
    fn default() -> Self {
        Self {
            ingress: buf::Accum::new(),
            parse_state: <N::Codec as Codec>::ParseState::default(),
            conn_state: N::ConnState::default(),
        }
    }
}

impl<N: Session> State<SessionConn<N>> {
    pub fn conn_state(&self) -> &N::ConnState {
        &self.conn.conn_state
    }

    pub fn conn_state_mut(&mut self) -> &mut N::ConnState {
        &mut self.conn.conn_state
    }
}

pub struct SessionApp<N: Session, W: Wire> {
    session: N,
    _w: PhantomData<fn() -> W>,
}

impl<N: Session, W: Wire> ConnApp for SessionApp<N, W> {
    type Conn = SessionConn<N>;
    type Wire = W;

    fn on_chunk<'d>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        chunk: RecvChunk<'_>,
        _driver: &'d Driver,
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
        } = conn;
        if !ingress.extend(chunk.as_slice()) {
            return ChunkOutcome::Overrun;
        }
        loop {
            let Some(pending) = ingress.peek() else { break };
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
                },
            );
        }
        match conn_state.wants_close() {
            Close::Keep => ChunkOutcome::Ok,
            Close::Reconnect => ChunkOutcome::CloseReconnect,
            Close::Permanent => ChunkOutcome::ClosePermanent,
        }
    }

    fn on_connected<'d>(
        &mut self,
        _tag: u32,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        _driver: &'d Driver,
    ) {
        let conn_id = slot.token();
        let State { conn, egress, .. } = &mut slot.state;
        self.session.connect(&mut Ctx {
            conn_id,
            state: &mut conn.conn_state,
            sink: egress,
        });
    }

    fn before_send<'d>(&mut self, slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>) {
        let conn_id = slot.token();
        let State { conn, egress, .. } = &mut slot.state;
        self.session.flush_trailer(&mut Ctx {
            conn_id,
            state: &mut conn.conn_state,
            sink: egress,
        });
    }

    fn on_send<'d>(
        &mut self,
        _slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        _sent: usize,
        _driver: &'d Driver,
    ) {
    }

    fn on_close<'d>(&mut self, slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>) {
        let conn_id = slot.token();
        let State { conn, egress, .. } = &mut slot.state;
        self.session.disconnect(&mut Ctx {
            conn_id,
            state: &mut conn.conn_state,
            sink: egress,
        });
    }

    fn defer_close<'d>(&self, slot: &Slot<'d, Self::Wire, State<Self::Conn>>) -> bool {
        slot.state.conn.conn_state.defer_close()
    }

    fn is_drained<'d>(&self, slot: &Slot<'d, Self::Wire, State<Self::Conn>>) -> bool {
        slot.state.conn.conn_state.is_drained()
    }
}

pub type Connector<
    'd,
    const ID: u8,
    N,
    S = Static<Tcp>,
    E = Bundle<Tcp, Identity, backend::profile::Production>,
> = Core<'d, ID, SessionApp<N, <E as Env>::Wire>, S, E>;
