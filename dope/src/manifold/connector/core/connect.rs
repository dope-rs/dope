use std::pin::Pin;

use dope_core::io::{ConnectEvent, SocketEvent};
use dope_net::Transport;
use dope_net::link::raw::event::{ConnectStep, SocketStep};

use super::Core;
use super::send::SendPhase;
use super::source::SourcePhase;
use crate::DriverContext;
use crate::manifold::connector::app::ConnApp;
use crate::manifold::connector::source::Dialer;
use crate::manifold::env::Env;
use dope_core::driver::token::Token;

pub(super) trait ConnectPhase<'d, const ID: u8, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    fn socket(
        self: Pin<&mut Self>,
        token: Token,
        event: SocketEvent,
        driver: &mut DriverContext<'_, 'd>,
    );

    fn connect(
        self: Pin<&mut Self>,
        token: Token,
        event: ConnectEvent,
        driver: &mut DriverContext<'_, 'd>,
    );
}

impl<'d, const ID: u8, A, S, E> ConnectPhase<'d, ID, A, S, E> for Core<'d, ID, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    fn socket(
        self: Pin<&mut Self>,
        token: Token,
        event: SocketEvent,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let now = driver.turn_now();
        let this = self.project();
        let stream = <E::Transport as Transport>::StreamConfig::default();
        let upstreams = &mut *this.upstreams;
        let step = this.pool.drive_socket_cqe(token, &event, driver, |slot| {
            let dial = slot.state.dial;
            let prepared = upstreams
                .sock_addr(dial)
                .map(|addr| (addr, upstreams.stream_config(dial).unwrap_or(stream)));
            (dial, prepared)
        });
        if let SocketStep::Failed { peeked: Some(dial) } = step {
            upstreams.connect_outcome(dial, false, now);
        }
    }

    fn connect(
        mut self: Pin<&mut Self>,
        token: Token,
        event: ConnectEvent,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let now = driver.turn_now();
        let (idx, key) = {
            let this = self.as_mut().project();
            let step = this
                .pool
                .drive_connect_cqe(token, &event, driver, |slot| slot.state.dial);
            match step {
                ConnectStep::Connected { idx, peeked } => (idx, peeked),
                ConnectStep::Failed { peeked, .. } => {
                    this.app.connect_failed(peeked, driver);
                    this.upstreams.connect_outcome(peeked, false, now);
                    return;
                }
                ConnectStep::Drop { peeked } => {
                    if let Some(key) = peeked {
                        this.app.connect_failed(key, driver);
                        this.upstreams.connect_outcome(key, false, now);
                    }
                    return;
                }
            }
        };
        {
            let this = self.as_mut().project();
            if let Some(slot) = this.pool.get_mut(idx) {
                slot.state.last_recv = Some(now);
                this.app.connected(key, slot, driver);
            }
        }
        self.as_mut().submit_egress(idx, driver);
        if self.as_ref().project_ref().liveness_timer.is_none()
            && let Some(timeout) = self.as_ref().project_ref().app.inbound_idle_timeout()
        {
            self.as_mut().arm_liveness(now + timeout);
        }
        self.project().upstreams.connect_outcome(key, true, now);
    }
}
