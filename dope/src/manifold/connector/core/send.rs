use std::pin::Pin;

use dope_core::driver::token::{SlotIndex, Token};
use dope_core::io::SendEvent;
use dope_net::Transport;
use dope_net::link::egress::arena;
use dope_net::link::raw::event::SendOutcome;
use dope_net::link::raw::pool::outbound::OutboundPool;
use dope_net::link::slot::{PEND_CLOSE, PEND_EGRESS, PendingQueue, Slot};
use dope_net::wire::{Reclaim, Wire};

use super::Core;
use super::close::ClosePhase;
use crate::DriverContext;
use crate::manifold::connector::app::{CloseKind, ConnApp};
use crate::manifold::connector::source::Dialer;
use crate::manifold::connector::state::State;
use crate::manifold::env::Env;

pub(super) trait SendPhase<'d, const ID: u8, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    fn drain_requests(
        app: &A,
        egress_arena: &mut arena::Arena<'_, A::Send>,
        dirty: &PendingQueue,
        idx: SlotIndex,
        slot: &mut Slot<'d, A::Wire, State<A::Conn, A::Send>>,
        driver: &mut DriverContext<'_, 'd>,
    );

    fn apply_requests(self: Pin<&mut Self>, target: Token, driver: &mut DriverContext<'_, 'd>);

    fn submit_egress(self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>);

    fn flush_dirty(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>);

    fn handle_send(
        self: Pin<&mut Self>,
        token: Token,
        event: SendEvent,
        driver: &mut DriverContext<'_, 'd>,
    );
}

impl<'pool, 'd, const ID: u8, A, S, E> SendPhase<'d, ID, A, S, E> for Core<'pool, 'd, ID, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    fn drain_requests(
        app: &A,
        egress_arena: &mut arena::Arena<'_, A::Send>,
        dirty: &PendingQueue,
        idx: SlotIndex,
        slot: &mut Slot<'d, A::Wire, State<A::Conn, A::Send>>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let target = slot.token();
        let mut enqueued = false;
        let egress = egress_arena.queue_for(slot.state.lane);
        let requests = app.drain_requests(
            target,
            |bytes| egress.try_enqueue(bytes).inspect(|()| enqueued = true),
            driver,
        );
        if enqueued {
            dirty.mark(idx, &slot.state.pending, PEND_EGRESS);
        }
        match requests.close {
            Some(CloseKind::Reconnect) => {
                dirty.mark(idx, &slot.state.pending, PEND_CLOSE);
            }
            Some(CloseKind::Permanent) => {
                slot.state.close_permanent = true;
                dirty.mark(idx, &slot.state.pending, PEND_CLOSE);
            }
            None => {}
        }
    }

    fn apply_requests(mut self: Pin<&mut Self>, target: Token, driver: &mut DriverContext<'_, 'd>) {
        let this = self.as_mut().project();
        let Some((idx, slot)) = this.pool.by_target_mut(target) else {
            return;
        };
        Self::drain_requests(this.app, this.egress_arena, this.dirty, idx, slot, driver);
    }

    fn submit_egress(self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>) {
        let this = self.project();
        let Some(outbound) = this.pool.send_slot(idx) else {
            return;
        };
        let slot = outbound.slot;
        let token = outbound.token;
        if !slot.state.establish.is_done() {
            return;
        }
        this.app
            .before_send(slot, this.egress_arena.queue_for(slot.state.lane), driver);
        let reclaim_on_submit = matches!(<A::Wire as Wire>::RECLAIM, Reclaim::OnSubmit);
        let consumed = {
            let mut egress = this.egress_arena.queue_for(slot.state.lane);
            let vectored = egress.prepare_send(u32::MAX as usize);
            if vectored.is_empty() {
                slot.flush_pending(driver, token);
                return;
            }
            let consumed = Slot::<A::Wire, State<A::Conn, A::Send>>::submit_wire_vectored(
                &mut slot.core,
                &mut slot.wire,
                &mut slot.send,
                vectored,
                token,
                driver,
            );
            if reclaim_on_submit && !egress.try_ack(consumed) {
                slot.core.begin_close();
                return;
            }
            consumed
        };
        if reclaim_on_submit {
            Self::drain_requests(this.app, this.egress_arena, this.dirty, idx, slot, driver);
        }
        let stalled =
            reclaim_on_submit && consumed == 0 && !A::Wire::holds_plain(&slot.wire, &slot.send);
        if !slot.core.is_send_inflight() && !stalled {
            this.dirty.mark(idx, &slot.state.pending, PEND_EGRESS);
        }
    }

    fn flush_dirty(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let n = self.as_ref().project_ref().dirty.len();
        for _ in 0..n {
            let (idx, flags) = {
                let this = self.as_mut().project();
                let Some(idx) = this.dirty.pop() else {
                    break;
                };
                let Some(slot) = this.pool.get(idx) else {
                    continue;
                };
                (idx, slot.state.pending.take_flags())
            };
            if flags & PEND_EGRESS != 0 {
                self.as_mut().submit_egress(idx, driver);
            }
            if flags & PEND_CLOSE != 0 {
                self.as_mut().close_slot(idx, driver);
            }
        }
    }

    fn handle_send(
        mut self: Pin<&mut Self>,
        token: Token,
        event: SendEvent,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let (idx, sent) = match self
            .as_mut()
            .project()
            .pool
            .classify_send(driver, token, event)
        {
            SendOutcome::Sent { idx, n } => (idx, n),
            SendOutcome::Close(idx) => {
                return self.as_mut().close_slot(idx, driver);
            }
            SendOutcome::Drop => return,
        };
        {
            let this = self.as_mut().project();
            if let Some(slot) = this.pool.get_mut(idx) {
                if matches!(<A::Wire as Wire>::RECLAIM, Reclaim::OnComplete)
                    && !this.egress_arena.queue_for(slot.state.lane).try_ack(sent)
                {
                    self.as_mut().close_slot(idx, driver);
                    return;
                }
                this.app.send(
                    slot,
                    this.egress_arena.queue_for(slot.state.lane),
                    sent,
                    driver,
                );
                Self::drain_requests(this.app, this.egress_arena, this.dirty, idx, slot, driver);
            }
        }
        self.as_mut().submit_egress(idx, driver);
        self.maybe_close(idx, driver);
    }
}
