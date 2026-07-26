use std::io;
use std::time::Duration;

use super::source::DialKey;
use super::state::State;
use crate::DriverContext;
use crate::io::provided::ProvidedView;
use crate::runtime::dispatcher::Idle;
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

/// How [`ConnApp::drain_requests`] treats the dial target on close.
/// This gives requests outside the receive path both recoverable and terminal
/// outcomes, matching [`ChunkOutcome`].
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
    pub close: Option<CloseKind>,
}

pub trait ConnApp<'d>: Sized {
    type Conn: Default;
    type Wire: Wire;
    type Send: AsRef<[u8]>;

    const RETAIN_RAW_RECV: bool = false;

    /// Returns the inbound-idle bound; `None` disables tracking at zero cost.
    /// Expiry forces a recoverable reconnect, so the bound should exceed the
    /// protocol keepalive cadence. It is re-read whenever the deadline is armed.
    fn inbound_idle_timeout(&self) -> Option<Duration> {
        None
    }

    fn max_retained_recv_chunks(_: usize) -> io::Result<usize> {
        Ok(0)
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

    fn before_send(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let _ = (slot, driver);
    }

    fn send(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        sent: usize,
        driver: &mut DriverContext<'_, 'd>,
    );

    fn close(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        driver: &mut DriverContext<'_, 'd>,
    );

    fn defer_close(
        &self,
        slot: &Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        let _ = (slot, driver);
        false
    }

    fn is_drained(
        &self,
        slot: &Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        let _ = (slot, driver);
        true
    }

    fn drain_requests(
        &self,
        token: Token,
        push: impl FnMut(Self::Send) -> Result<(), Self::Send>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Requests {
        let _ = (token, push, driver);
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
