use std::pin::Pin;
use std::time::Instant;

use dope_core::driver::token::{SlotIndex, Token};
use dope_core::io::RecvEvent;
use dope_core::io::provided::ProvidedView;
use dope_net::Transport;
use dope_net::link::pool::DispatchRecv;
use dope_net::wire::Wire;
use o3::buffer::RetainBytes;

use super::Core;
use super::close::ClosePhase;
use super::send::SendPhase;
use crate::DriverContext;
use crate::manifold::Outcome;
use crate::manifold::connector::app::{ChunkOutcome, ConnApp};
use crate::manifold::connector::source::Dialer;
use crate::manifold::env::Env;

pub(super) trait RecvPhase<'d, const ID: u8, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    fn recv_chunk<R: RetainBytes>(
        self: Pin<&mut Self>,
        idx: SlotIndex,
        chunk: R,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome;

    fn recv_retained_chunk(
        self: Pin<&mut Self>,
        idx: SlotIndex,
        chunk: ProvidedView<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome;

    fn finish_chunk(
        self: Pin<&mut Self>,
        idx: SlotIndex,
        outcome: ChunkOutcome,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome;

    fn dispatch_chunk<C, F>(
        self: Pin<&mut Self>,
        dispatch: DispatchRecv<C>,
        now: Instant,
        driver: &mut DriverContext<'_, 'd>,
        recv: F,
    ) where
        F: FnOnce(Pin<&mut Self>, SlotIndex, C, &mut DriverContext<'_, 'd>) -> Outcome;

    fn handle_recv(
        self: Pin<&mut Self>,
        token: Token,
        more: bool,
        event: RecvEvent<'d>,
        driver: &mut DriverContext<'_, 'd>,
    );
}

impl<'d, const ID: u8, A, S, E> RecvPhase<'d, ID, A, S, E> for Core<'d, ID, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    fn recv_chunk<R: RetainBytes>(
        mut self: Pin<&mut Self>,
        idx: SlotIndex,
        chunk: R,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        let outcome = {
            let this = self.as_mut().project();
            let Some(slot) = this.pool.get_mut(idx) else {
                return Outcome::Ok;
            };
            this.app.chunk(slot, chunk, driver)
        };
        self.finish_chunk(idx, outcome, driver)
    }

    fn recv_retained_chunk(
        mut self: Pin<&mut Self>,
        idx: SlotIndex,
        chunk: ProvidedView<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        let outcome = {
            let this = self.as_mut().project();
            let Some(slot) = this.pool.get_mut(idx) else {
                return Outcome::Ok;
            };
            this.app.retained_chunk(slot, chunk, driver)
        };
        self.finish_chunk(idx, outcome, driver)
    }

    fn finish_chunk(
        mut self: Pin<&mut Self>,
        idx: SlotIndex,
        outcome: ChunkOutcome,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        if matches!(outcome, ChunkOutcome::Overrun) {
            return Outcome::Overrun;
        }
        self.as_mut().submit_egress(idx, driver);
        match outcome {
            ChunkOutcome::Ok => Outcome::Ok,
            ChunkOutcome::Overrun => Outcome::Overrun,
            ChunkOutcome::CloseReconnect => Outcome::CloseAfter,
            ChunkOutcome::ClosePermanent => {
                let key = self
                    .as_mut()
                    .project()
                    .pool
                    .get(idx)
                    .map(|slot| slot.state.dial);
                if let Some(key) = key {
                    self.project().upstreams.kill(key);
                }
                Outcome::CloseAfter
            }
        }
    }

    fn dispatch_chunk<C, F>(
        mut self: Pin<&mut Self>,
        dispatch: DispatchRecv<C>,
        now: Instant,
        driver: &mut DriverContext<'_, 'd>,
        recv: F,
    ) where
        F: FnOnce(Pin<&mut Self>, SlotIndex, C, &mut DriverContext<'_, 'd>) -> Outcome,
    {
        match dispatch {
            DispatchRecv::Drop => {}
            DispatchRecv::Close(idx) => self.as_mut().close_slot(idx, driver),
            DispatchRecv::NoChunk(idx) | DispatchRecv::Discarded(idx) => {
                if let Some(slot) = self.as_mut().project().pool.get_mut(idx) {
                    slot.state.last_recv = Some(now);
                }
                self.as_mut().submit_egress(idx, driver);
                self.as_mut().maybe_close(idx, driver);
            }
            DispatchRecv::Chunk(idx, chunk) => {
                if let Some(slot) = self.as_mut().project().pool.get_mut(idx) {
                    slot.state.last_recv = Some(now);
                }
                match recv(self.as_mut(), idx, chunk, driver) {
                    Outcome::Ok => self.as_mut().maybe_close(idx, driver),
                    Outcome::Overrun => {
                        if let Some(slot) = self.as_mut().project().pool.get_mut(idx) {
                            slot.core.mark_aborted();
                        }
                        self.as_mut().close_slot(idx, driver)
                    }
                    Outcome::CloseAfter => {
                        self.as_mut().project().pool.set_close_after(idx);
                        self.as_mut().maybe_close(idx, driver);
                    }
                }
            }
        }
    }

    fn handle_recv(
        mut self: Pin<&mut Self>,
        token: Token,
        more: bool,
        event: RecvEvent<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let now = driver.turn_now();
        if A::RETAIN_RAW_RECV && A::Wire::RAW_RECV {
            let dispatch = self
                .as_mut()
                .project()
                .pool
                .dispatch_retained_recv(token, more, event);
            self.dispatch_chunk(
                dispatch,
                now,
                driver,
                |this, idx, chunk, driver| match chunk {
                    Some(chunk) => this.recv_retained_chunk(idx, chunk, driver),
                    None => Outcome::Overrun,
                },
            );
            return;
        }
        let dispatch = self
            .as_mut()
            .project()
            .pool
            .dispatch_recv(token, more, &event);
        self.dispatch_chunk(dispatch, now, driver, Self::recv_chunk);
    }
}
