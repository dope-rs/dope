use std::time::Duration;

use super::Ctx;
use super::app::Requests;
use super::codec::Codec;
use super::lifecycle::Lifecycle;
use crate::runtime::Idle;
use dope_core::driver::ready::ReadyKey;
use dope_core::driver::token::Token;

pub trait Session<'d>: Sized {
    type Codec: Codec;
    type ConnState: Lifecycle;
    type Send: AsRef<[u8]>;

    fn codec(&self) -> &Self::Codec;

    fn activate(&self, token: Token, ready: ReadyKey<'d>) {
        let _ = (token, ready);
    }

    fn connect(&mut self, ctx: &mut Ctx<'_, 'd, Self>);

    fn response(&mut self, head: <Self::Codec as Codec>::Head, ctx: &mut Ctx<'_, 'd, Self>);

    fn disconnect(&mut self, ctx: &mut Ctx<'_, 'd, Self>);

    fn flush_trailer(&mut self, ctx: &mut Ctx<'_, 'd, Self>) {
        let _ = ctx;
    }

    fn sent(&self, token: Token, sent: usize) {
        let _ = (token, sent);
    }

    fn drain_requests(
        &self,
        token: Token,
        push: impl FnMut(Self::Send) -> Result<(), Self::Send>,
    ) -> Requests {
        let _ = (token, push);
        Requests::default()
    }

    fn defer_close(&self, token: Token, state: &Self::ConnState) -> bool {
        let _ = token;
        state.defer_close()
    }

    fn is_drained(&self, token: Token, state: &Self::ConnState) -> bool {
        let _ = token;
        state.is_drained()
    }

    fn pre_park(&mut self) {}

    fn idle(&self) -> Idle {
        Idle::Park(None)
    }

    /// Inbound-idle liveness bound (see `ConnApp::inbound_idle_timeout`). A
    /// protocol sets this to just over its keepalive cadence — e.g. AMQP returns
    /// `2 ×` the negotiated heartbeat once the handshake fixes the interval — so
    /// the connector force-reconnects a silently vanished peer. `None` disables.
    fn inbound_idle_timeout(&self) -> Option<Duration> {
        None
    }
}
