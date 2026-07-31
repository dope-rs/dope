use crate::DriverContext;
use crate::manifold::Outcome;
use crate::manifold::env::Env;
use crate::manifold::listener::Listener;
use dope_core::driver::token::{SlotIndex, Token};
use dope_core::io::RecvEvent;
use dope_net::link::raw::event::DispatchRecv;
use dope_net::link::slot::Slot;
use dope_net::wire::{RecvChunk, Wire};
use o3::buffer::RetainBytes;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;

use super::egress::{Egress, EgressPhase, SlotFlow};
use super::idle::IdlePhase;
use super::send::SendPhase;
use super::state::{EgressCtx, State};

pub trait Application<'d>: Sized {
    type Conn: Default + 'static;
    type Wire: Wire;
    type Hooks: ApplicationHooks<'d, Self>;

    const RETAIN_RAW_RECV: bool = false;

    fn max_retained_recv_chunks(_: usize) -> io::Result<usize> {
        Ok(0)
    }

    fn connection(self: Pin<&Self>) -> Self::Conn {
        Self::Conn::default()
    }
}

/// Statically selected callbacks for a listener [`Application`].
///
/// The policy is an associated type only: no value is stored and every call is
/// monomorphized for the application and policy pair.
pub trait ApplicationHooks<'d, A: Application<'d>> {
    fn chunk<R: RetainBytes>(
        app: Pin<&mut A>,
        slot: &mut Slot<'d, A::Wire, State<A::Conn>>,
        egress: EgressCtx<'_, '_>,
        chunk: R,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome;

    fn retained_chunk(
        app: Pin<&mut A>,
        slot: &mut Slot<'d, A::Wire, State<A::Conn>>,
        egress: EgressCtx<'_, '_>,
        chunk: <A::Wire as Wire>::RetainedRecv<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        let _ = (app, slot, egress, chunk, driver);
        Outcome::Overrun
    }

    fn send(
        app: Pin<&mut A>,
        slot: &mut Slot<'d, A::Wire, State<A::Conn>>,
        egress: EgressCtx<'_, '_>,
        sent: usize,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let _ = (app, slot, egress, sent, driver);
    }

    fn close(
        app: Pin<&mut A>,
        slot: &mut Slot<'d, A::Wire, State<A::Conn>>,
        egress: EgressCtx<'_, '_>,
    ) {
        let _ = (app, slot, egress);
    }

    fn teardown(
        app: Pin<&mut A>,
        slot: &mut Slot<'d, A::Wire, State<A::Conn>>,
        egress: EgressCtx<'_, '_>,
    ) {
        Self::close(app, slot, egress);
    }

    fn defer_close(app: Pin<&A>, slot: &Slot<'d, A::Wire, State<A::Conn>>) -> bool {
        let _ = (app, slot);
        false
    }

    fn capped(app: Pin<&mut A>, peer_ip: IpAddr) {
        let _ = (app, peer_ip);
    }

    fn activate(
        app: Pin<&mut A>,
        slot: &mut Slot<'d, A::Wire, State<A::Conn>>,
        egress: EgressCtx<'_, '_>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let _ = (app, slot, egress, driver);
    }

    fn accept(
        app: Pin<&mut A>,
        slot: &mut Slot<'d, A::Wire, State<A::Conn>>,
        egress: EgressCtx<'_, '_>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        let _ = (app, slot, egress, driver);
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

    fn resume_recv(self: Pin<&mut Self>, target: Token, driver: &mut DriverContext<'_, 'd>);

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
        token: Token,
        refresh_idle: bool,
        driver: &mut DriverContext<'_, 'd>,
    );
}

impl<'pool, 'd, const ID: u8, A, E> ApplicationPhase<'d, ID, A, E> for Listener<'pool, 'd, ID, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    fn resume_recv(mut self: Pin<&mut Self>, target: Token, driver: &mut DriverContext<'_, 'd>) {
        let resumed = self.as_mut().project().pool.resume_recv(target);
        if !resumed {
            return;
        }
        loop {
            let deferred = self.as_mut().project().pool.pop_resumed_recv(target);
            let Some((token, more, event)) = deferred else {
                break;
            };
            self.as_mut().pump_recv(token, more, event, driver);
        }
        self.as_mut().project().pool.flush_rearm(driver);
    }

    fn pump_recv(
        mut self: Pin<&mut Self>,
        token: Token,
        more: bool,
        mut event: RecvEvent<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        if A::RETAIN_RAW_RECV {
            let dispatch = {
                let this = self.as_mut().project();
                this.pool.dispatch_retained_recv(token, more, event)
            };
            self.dispatch_chunk(token, dispatch, driver, |mut this, idx, chunk, driver| {
                let mut this = this.as_mut().project();
                this.idle.arm(idx, driver.turn_now());
                match this.pool.get_mut(idx) {
                    Some(slot) => {
                        let egress =
                            EgressCtx::for_slot(this.aux, this.egress_arena, slot.token().slot());
                        A::Hooks::retained_chunk(this.app.as_mut(), slot, egress, chunk, driver)
                    }
                    None => Outcome::Ok,
                }
            });
            return;
        }
        let dispatch = {
            let this = self.as_mut().project();
            this.pool.dispatch_recv(token, more, &mut event)
        };
        self.dispatch_chunk(token, dispatch, driver, |mut this, idx, batch, driver| {
            let mut this = this.as_mut().project();
            this.idle.arm(idx, driver.turn_now());
            let Some(slot) = this.pool.get_mut(idx) else {
                return Outcome::Ok;
            };
            for chunk in batch {
                let egress = EgressCtx::for_slot(this.aux, this.egress_arena, slot.token().slot());
                let outcome = match chunk {
                    RecvChunk::Borrowed(chunk) => {
                        A::Hooks::chunk(this.app.as_mut(), slot, egress, chunk, driver)
                    }
                    RecvChunk::Owned(chunk) => {
                        A::Hooks::chunk(this.app.as_mut(), slot, egress, chunk, driver)
                    }
                };
                if !matches!(outcome, Outcome::Ok) {
                    return outcome;
                }
            }
            Outcome::Ok
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
                self.as_mut().flush_after_recv(idx, token, false, driver);
            }
            DispatchRecv::Discarded(idx) => {
                self.as_mut().flush_after_recv(idx, token, true, driver);
            }
            DispatchRecv::Chunk(idx, chunk) => match recv(self.as_mut(), idx, chunk, driver) {
                Outcome::Ok => {
                    self.as_mut().flush_after_recv(idx, token, false, driver);
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
        token: Token,
        refresh_idle: bool,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        {
            let this = self.as_mut().project();
            if refresh_idle {
                this.idle.arm(idx, driver.turn_now());
            }
            let ud = token.with_kind(0);
            if let Some(slot) = this.pool.get_mut(idx) {
                slot.flush_pending(driver, ud);
                let egress = this.egress_arena.queue_for(idx.raw() as usize);
                if !slot.core.is_send_inflight() && matches!(slot.egress(&egress), Egress::Stalled)
                {
                    let write_buf = this.aux.write_buf_raw(slot);
                    slot.resume_send(write_buf, ud, driver);
                }
            }
        }
        self.as_mut().maybe_close_inherent(idx, driver);
    }
}
