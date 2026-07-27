use crate::DriverContext;
use crate::manifold::Outcome;
use crate::manifold::env::Env;
use crate::manifold::listener::Listener;
use dope_core::driver::token::{Epoch, SlotIndex, Token};
use dope_core::io::RecvEvent;
use dope_core::io::provided::ProvidedView;
use dope_net::link::raw::pool::DispatchRecv;
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;
use o3::buffer::{Borrowed, Bytes, RetainBytes};
use std::io;
use std::net::IpAddr;
use std::pin::Pin;

use super::egress::{Egress, EgressPhase, SlotFlow};
use super::idle::IdlePhase;
use super::send::SendPhase;
use super::state::{Aux, State};

pub trait Application<'d>: Sized {
    type Conn: Default + 'static;
    type Wire: Wire;

    const RETAIN_RAW_RECV: bool = false;

    fn max_retained_recv_chunks(_: usize) -> io::Result<usize> {
        Ok(0)
    }

    fn connection(self: Pin<&Self>) -> Self::Conn {
        Self::Conn::default()
    }

    fn chunk<R: RetainBytes>(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        chunk: R,
        aux: &mut Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome;

    fn retained_chunk(
        mut self: Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        chunk: ProvidedView<'d>,
        aux: &mut Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        let bytes = Bytes::<Borrowed<'_>>::from(chunk.as_slice());
        self.as_mut().chunk(slot, bytes, aux, driver)
    }

    fn send(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        sent: usize,
        aux: &mut Aux,
        driver: &mut DriverContext<'_, 'd>,
    );

    fn close(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        aux: &mut Aux,
    );

    fn teardown(
        mut self: Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        aux: &mut Aux,
    ) {
        self.as_mut().close(slot, aux);
    }

    fn defer_close(self: Pin<&Self>, slot: &Slot<'d, Self::Wire, State<Self::Conn>>) -> bool {
        let _ = slot;
        false
    }

    fn capped(self: Pin<&mut Self>, peer_ip: IpAddr) {
        let _ = peer_ip;
    }

    fn activate(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        aux: &mut Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let _ = (slot, aux, driver);
    }

    fn accept(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        aux: &mut Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        let _ = (slot, aux, driver);
        Outcome::Ok
    }
}

pub(super) trait ApplicationPhase<'d, const ID: u8, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    fn pump_recv(
        self: Pin<&mut Self>,
        token: Token,
        more: bool,
        event: RecvEvent<'d>,
        driver: &mut DriverContext<'_, 'd>,
    );

    fn dispatch_chunk<C, F>(
        self: Pin<&mut Self>,
        token: Token,
        dispatch: DispatchRecv<C>,
        driver: &mut DriverContext<'_, 'd>,
        recv: F,
    ) where
        F: FnOnce(Pin<&mut Self>, SlotIndex, C, &mut DriverContext<'_, 'd>) -> Outcome;

    fn flush_after_recv(
        self: Pin<&mut Self>,
        idx: SlotIndex,
        epoch: Epoch,
        refresh_idle: bool,
        driver: &mut DriverContext<'_, 'd>,
    );
}

impl<'d, const ID: u8, A, E> ApplicationPhase<'d, ID, A, E> for Listener<'d, ID, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    fn pump_recv(
        mut self: Pin<&mut Self>,
        token: Token,
        more: bool,
        event: RecvEvent<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        if A::RETAIN_RAW_RECV && A::Wire::RAW_RECV {
            let dispatch = {
                let this = self.as_mut().project();
                this.pool.dispatch_retained_recv(token, more, event)
            };
            self.dispatch_chunk(token, dispatch, driver, |mut this, idx, chunk, driver| {
                let Some(chunk) = chunk else {
                    return Outcome::Overrun;
                };
                let mut this = this.as_mut().project();
                this.idle.arm(idx, driver.turn_now());
                match this.pool.get_mut(idx) {
                    Some(slot) => this
                        .app
                        .as_mut()
                        .retained_chunk(slot, chunk, this.aux, driver),
                    None => Outcome::Ok,
                }
            });
            return;
        }
        let dispatch = {
            let this = self.as_mut().project();
            this.pool.dispatch_recv(token, more, &event)
        };
        self.dispatch_chunk(token, dispatch, driver, |mut this, idx, chunk, driver| {
            let mut this = this.as_mut().project();
            this.idle.arm(idx, driver.turn_now());
            match this.pool.get_mut(idx) {
                Some(slot) => this.app.as_mut().chunk(slot, chunk, this.aux, driver),
                None => Outcome::Ok,
            }
        });
    }

    fn dispatch_chunk<C, F>(
        mut self: Pin<&mut Self>,
        token: Token,
        dispatch: DispatchRecv<C>,
        driver: &mut DriverContext<'_, 'd>,
        recv: F,
    ) where
        F: FnOnce(Pin<&mut Self>, SlotIndex, C, &mut DriverContext<'_, 'd>) -> Outcome,
    {
        match dispatch {
            DispatchRecv::Drop => {}
            DispatchRecv::Close(idx) => {
                Self::close_inherent(self.as_mut(), idx, driver);
            }
            DispatchRecv::NoChunk(idx) => {
                self.as_mut()
                    .flush_after_recv(idx, token.epoch(), false, driver);
            }
            DispatchRecv::Discarded(idx) => {
                self.as_mut()
                    .flush_after_recv(idx, token.epoch(), true, driver);
            }
            DispatchRecv::Chunk(idx, chunk) => match recv(self.as_mut(), idx, chunk, driver) {
                Outcome::Ok => {
                    self.as_mut()
                        .flush_after_recv(idx, token.epoch(), false, driver);
                    self.as_mut().arm_send_deadline(idx, driver);
                }
                Outcome::Overrun => {
                    if let Some(slot) = self.as_mut().project().pool.get_mut(idx) {
                        slot.core.mark_aborted();
                    }
                    Self::close_inherent(self.as_mut(), idx, driver)
                }
                Outcome::CloseAfter => {
                    self.as_mut().project().pool.set_close_after(idx);
                    self.as_mut().maybe_close_inherent(idx, driver);
                }
            },
        }
    }

    fn flush_after_recv(
        mut self: Pin<&mut Self>,
        idx: SlotIndex,
        epoch: Epoch,
        refresh_idle: bool,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        {
            let this = self.as_mut().project();
            if refresh_idle {
                this.idle.arm(idx, driver.turn_now());
            }
            let ud = Token::new(ID, idx, epoch);
            if let Some(slot) = this.pool.get_mut(idx) {
                slot.flush_pending(driver, ud);
                if !slot.core.is_send_inflight() && matches!(slot.egress(), Egress::Stalled) {
                    let write_buf = this.aux.write_buf_raw(slot);
                    slot.resume_send(write_buf, ud, driver);
                }
            }
        }
        self.as_mut().maybe_close_inherent(idx, driver);
    }
}
