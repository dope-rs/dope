use std::pin::Pin;

use dope_core::driver::token::{SlotIndex, Token};
use dope_core::io::SendEvent;
use dope_net::Transport;
use dope_net::link::pool::SendOutcome;
use dope_net::link::slot::{PEND_CLOSE, PEND_EGRESS, PEND_SHUTDOWN, PendingQueue, Slot};
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

impl<'d, const ID: u8, A, S, E> SendPhase<'d, ID, A, S, E> for Core<'d, ID, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    fn drain_requests(
        app: &A,
        dirty: &PendingQueue,
        idx: SlotIndex,
        slot: &mut Slot<'d, A::Wire, State<A::Conn, A::Send>>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let target = slot.token();
        let mut enqueued = false;
        let requests = app.drain_requests(
            target,
            |bytes| slot.state.enqueue_send(bytes).inspect(|()| enqueued = true),
            driver,
        );
        if enqueued {
            dirty.mark(idx, &slot.state.pending, PEND_EGRESS);
        }
        if let Some(how) = requests.shutdown {
            slot.state.pending.set_shutdown(how);
            dirty.mark(idx, &slot.state.pending, PEND_SHUTDOWN);
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
        Self::drain_requests(this.app, this.dirty, idx, slot, driver);
    }

    fn submit_egress(self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>) {
        let this = self.project();
        let Some((slot, token)) = this.pool.send_slot(idx) else {
            return;
        };
        if !slot.state.establish.is_done() {
            return;
        }
        this.app.before_send(slot, driver);
        let vectored = slot.state.prepare_send(u32::MAX as usize);
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
        if matches!(<A::Wire as Wire>::RECLAIM, Reclaim::OnSubmit) {
            slot.state.ack_send(consumed);
            Self::drain_requests(this.app, this.dirty, idx, slot, driver);
        }
        let stalled = matches!(<A::Wire as Wire>::RECLAIM, Reclaim::OnSubmit)
            && consumed == 0
            && !slot.wire.holds_plain(&slot.send);
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
            if flags & PEND_SHUTDOWN != 0 {
                let this = self.as_mut().project();
                let how = this
                    .pool
                    .get(idx)
                    .map(|slot| slot.state.pending.shutdown_how())
                    .unwrap_or(0);
                if let Some(fd) = this.pool.fd_of(idx) {
                    let _ = <E::Transport as Transport>::submit_shutdown(driver, fd, how);
                }
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
                if matches!(<A::Wire as Wire>::RECLAIM, Reclaim::OnComplete) {
                    slot.state.ack_send(sent);
                }
                this.app.send(slot, sent, driver);
                Self::drain_requests(this.app, this.dirty, idx, slot, driver);
            }
        }
        self.as_mut().submit_egress(idx, driver);
        self.maybe_close(idx, driver);
    }
}
