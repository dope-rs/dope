use std::time::Duration;

use super::source::DialKey;
use super::state::State;
use crate::DriverContext;
use crate::ProvidedView;
use crate::runtime::Idle;
use dope_core::driver::token::{SlotIndex, Token};
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;
use o3::buffer::{Borrowed, Bytes, RetainBytes};

pub enum ChunkOutcome {
    Ok,
    Overrun,
    CloseReconnect,
    ClosePermanent,
}

/// How an app-initiated close (returned from [`ConnApp::drain_requests`]) treats
/// the dial target — the symmetric counterpart of the receive path's
/// [`ChunkOutcome::CloseReconnect`] / [`ChunkOutcome::ClosePermanent`]. Without
/// this a request off the receive path (a supervisor tick, a timer) could only
/// ever reconnect, so a terminal fault it detected would redial into the same
/// doomed connection forever.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseKind {
    /// Drop the socket and redial (transient: idle timeout, rotation).
    Reconnect,
    /// Drop the socket and DO NOT redial (terminal: retries exhausted,
    /// credentials permanently rejected).
    Permanent,
}

#[derive(Default)]
pub struct Requests {
    pub shutdown: Option<i32>,
    pub close: Option<CloseKind>,
}

pub trait ConnApp<'d>: Sized {
    type Conn: Default;
    type Wire: Wire;
    type Send: AsRef<[u8]>;

    const RETAIN_RAW_RECV: bool = false;

    /// Inbound-idle liveness bound for this app's connections. `Some(d)` opts in:
    /// if no bytes arrive from the peer for longer than `d`, the connector forces
    /// a *recoverable* reconnect (drops the socket, redials a fresh generation) —
    /// the only way to detect a silently vanished / half-open peer, which never
    /// surfaces a readable EOF. `None` (default) disables it at zero cost. The
    /// value should exceed the protocol's own keepalive cadence (e.g. AMQP:
    /// `2 ×` the negotiated heartbeat interval), and may change over a
    /// connection's life (the bound is re-read when the deadline is (re)armed).
    fn inbound_idle_timeout(&self) -> Option<Duration> {
        None
    }

    fn max_retained_recv_chunks(_: usize) -> usize {
        0
    }

    fn chunk<R: RetainBytes>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        chunk: R,
        driver: &mut DriverContext<'_, 'd>,
    ) -> ChunkOutcome;

    fn retained_chunk(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        chunk: ProvidedView<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> ChunkOutcome {
        let bytes = Bytes::<Borrowed<'_>>::from(chunk.as_slice());
        self.chunk(slot, bytes, driver)
    }

    fn connected(
        &mut self,
        key: DialKey,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        driver: &mut DriverContext<'_, 'd>,
    );

    fn connect_failed(&mut self, key: DialKey, driver: &mut DriverContext<'_, '_>) {
        let _ = (key, driver);
    }

    fn before_send(&mut self, slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>) {
        let _ = slot;
    }

    fn send(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        sent: usize,
        driver: &mut DriverContext<'_, 'd>,
    );

    fn close(&mut self, slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>);

    fn defer_close(&self, slot: &Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>) -> bool {
        let _ = slot;
        false
    }

    fn is_drained(&self, slot: &Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>) -> bool {
        let _ = slot;
        true
    }

    fn drain_requests(
        &self,
        token: Token,
        push: impl FnMut(Self::Send) -> Result<(), Self::Send>,
    ) -> Requests {
        let _ = (token, push);
        Requests::default()
    }

    fn take_cancel(&self) -> Option<(DialKey, SlotIndex)> {
        None
    }

    fn pre_park(&mut self) {}

    fn idle(&self) -> Idle {
        Idle::Park(None)
    }
}
