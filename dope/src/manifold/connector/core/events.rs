use std::pin::Pin;

use super::{ConnPool, Core};
use crate::DriverContext;
use crate::manifold::Outcome;
use crate::manifold::connector::app::{ChunkOutcome, CloseKind, ConnApp};
use crate::manifold::connector::source::Dialer;
use crate::manifold::connector::state::State;
use crate::manifold::env::Env;
use dope_core::backend::Sqe;
use dope_core::driver::submission::Submission;
use dope_core::driver::token::kind::{CONNECT, SOCKET};
use dope_core::driver::token::{SlotIndex, Token};
use dope_core::io::provided::{ProvidedLease, ProvidedView};
use dope_net::Transport;
use dope_net::link::pool::{ConnectStep, DispatchRecv, SendOutcome, SocketStep};
use dope_net::link::slot::{PEND_CLOSE, PEND_EGRESS, PEND_SHUTDOWN, PendingQueue, Slot};
use dope_net::wire::{Reclaim, Wire};
use o3::buffer::{ByteSpan, RetainBytes};

pub(super) trait Events<'d, const ID: u8, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    fn rouse(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>);

    fn flush_cancellations(self: Pin<&mut Self>);

    fn drain_requests(
        app: &A,
        dirty: &PendingQueue,
        idx: SlotIndex,
        slot: &mut Slot<'d, A::Wire, State<A::Conn, A::Send>>,
    );

    fn apply_requests(self: Pin<&mut Self>, target: Token);

    fn socket(
        self: Pin<&mut Self>,
        ud: Token,
        event: dope_core::io::SocketEvent,
        driver: &mut DriverContext<'_, 'd>,
    );

    fn connect(
        self: Pin<&mut Self>,
        ud: Token,
        event: dope_core::io::ConnectEvent,
        driver: &mut DriverContext<'_, 'd>,
    );

    fn submit_egress(self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>);

    fn close_slot(self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>);

    fn drain_close(
        pool: &mut ConnPool<'d, ID, E::Transport, A::Wire, A::Conn, A::Send>,
        dirty: &PendingQueue,
        app: &A,
        idx: SlotIndex,
        driver: &mut DriverContext<'_, 'd>,
    );

    fn flush_dirty(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>);

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

    fn handle_recv(
        self: Pin<&mut Self>,
        token: Token,
        more: bool,
        event: dope_core::io::RecvEvent,
        driver: &mut DriverContext<'_, 'd>,
    );

    fn handle_send(
        self: Pin<&mut Self>,
        token: Token,
        event: dope_core::io::SendEvent,
        driver: &mut DriverContext<'_, 'd>,
    );

    fn maybe_close(self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>);
}

impl<'d, const ID: u8, A, S, E> Events<'d, ID, A, S, E> for Core<'d, ID, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    fn rouse(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.as_mut().flush_cancellations();
        self.as_mut().poll_source(driver);
        self.as_mut().poll_liveness(driver);
        self.flush_dirty(driver);
    }

    fn flush_cancellations(mut self: Pin<&mut Self>) {
        loop {
            let cancel = self.as_ref().project_ref().app.take_cancel();
            let Some((key, idx)) = cancel else {
                break;
            };
            let this = self.as_mut().project();
            let Some(slot) = this.pool.get(idx) else {
                continue;
            };
            if slot.state.dial == key {
                this.dirty.mark(idx, &slot.state.pending, PEND_CLOSE);
            }
        }
    }

    fn drain_requests(
        app: &A,
        dirty: &PendingQueue,
        idx: SlotIndex,
        slot: &mut Slot<'d, A::Wire, State<A::Conn, A::Send>>,
    ) {
        let target = slot.token();
        let mut enqueued = false;
        let requests = app.drain_requests(target, |bytes| {
            slot.state.enqueue_send(bytes).inspect(|()| enqueued = true)
        });
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

    fn apply_requests(mut self: Pin<&mut Self>, target: Token) {
        let this = self.as_mut().project();
        let Some((idx, slot)) = this.pool.by_target_mut(target) else {
            return;
        };
        Self::drain_requests(this.app, this.dirty, idx, slot);
    }

    fn socket(
        self: Pin<&mut Self>,
        ud: Token,
        e: dope_core::io::SocketEvent,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let now = driver.turn_now();
        let this = self.project();
        let stream = *this.stream;
        let upstreams = &mut *this.upstreams;
        let step = this.pool.drive_socket_cqe(ud, &e, driver, |slot| {
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
        ud: Token,
        e: dope_core::io::ConnectEvent,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let now = driver.turn_now();
        let (idx, key) = {
            let this = self.as_mut().project();
            let step = this
                .pool
                .drive_connect_cqe(ud, &e, driver, |slot| slot.state.dial);
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
        // Arm the inbound-idle deadline on the first established connection; a
        // later connection has a strictly later deadline, so the standing arm
        // (and its fire-then-rescan) already covers it.
        if self.as_ref().project_ref().liveness_timer.is_none()
            && let Some(timeout) = self.as_ref().project_ref().app.inbound_idle_timeout()
        {
            self.as_mut().arm_liveness(now + timeout);
        }
        self.project().upstreams.connect_outcome(key, true, now);
    }

    fn submit_egress(self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>) {
        let this = self.project();
        let Some((slot, ud)) = this.pool.send_slot(idx) else {
            return;
        };
        if !slot.state.establish.is_done() {
            return;
        }
        this.app.before_send(slot);
        let vectored = slot.state.prepare_send(u32::MAX as usize);
        if vectored.is_empty() {
            slot.flush_pending(driver, ud);
            return;
        }
        let consumed = Slot::<A::Wire, State<A::Conn, A::Send>>::submit_wire_vectored(
            &mut slot.core,
            &mut slot.wire,
            &mut slot.send,
            vectored,
            ud,
            driver,
        );
        if matches!(<A::Wire as Wire>::RECLAIM, Reclaim::OnSubmit) {
            slot.state.ack_send(consumed);
            Self::drain_requests(this.app, this.dirty, idx, slot);
        }
        let stalled = matches!(<A::Wire as Wire>::RECLAIM, Reclaim::OnSubmit)
            && consumed == 0
            && !slot.wire.holds_plain(&slot.send);
        if !slot.core.is_send_inflight() && !stalled {
            this.dirty.mark(idx, &slot.state.pending, PEND_EGRESS);
        }
    }

    fn close_slot(self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>) {
        let now = driver.turn_now();
        let this = self.project();
        let slot_meta = this.pool.get_mut(idx).and_then(|slot| {
            if slot.state.retired {
                return None;
            }
            slot.state.retired = true;
            let established = slot.state.establish.is_done();
            let key = slot.state.dial;
            let permanent = slot.state.close_permanent;
            if established {
                this.app.close(slot);
            }
            Some((key, permanent))
        });
        if let Some((key, permanent)) = slot_meta {
            if permanent {
                // App-initiated `CloseKind::Permanent`: retire the dial target so
                // the source never redials it — same terminal effect as a
                // received `ChunkOutcome::ClosePermanent`.
                this.upstreams.kill(key);
            } else {
                this.upstreams.disconnect(key, now);
            }
        }
        Self::drain_close(this.pool, this.dirty, this.app, idx, driver);
    }

    fn drain_close(
        pool: &mut ConnPool<'d, ID, E::Transport, A::Wire, A::Conn, A::Send>,
        dirty: &PendingQueue,
        app: &A,
        idx: SlotIndex,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let (send_inflight, establishing, connecting, ud) = match pool.get(idx) {
            Some(s) => (
                s.core.is_send_inflight(),
                !s.state.establish.is_done(),
                s.state.establish.is_connecting(),
                s.token(),
            ),
            None => return,
        };
        if establishing {
            let op_kind = if connecting { CONNECT } else { SOCKET };
            let cancel = if connecting {
                Sqe::cancel(ud, op_kind)
            } else {
                let Some(fd) = pool.fd_of(idx) else {
                    return;
                };
                Sqe::cancel_create(fd.slot())
            };
            let cancelled = driver.push(cancel).is_ok();
            if let Some(slot) = pool.get_mut(idx) {
                slot.core.begin_close();
                if !cancelled {
                    dirty.mark(idx, &slot.state.pending, PEND_CLOSE);
                }
            }
            return;
        }
        if send_inflight {
            if let Some(slot) = pool.get_mut(idx) {
                slot.core.begin_close();
            }
            return;
        }
        if pool
            .get_mut(idx)
            .is_some_and(|s| s.seal_graceful(driver, ud))
        {
            return;
        }
        let drained = pool.get(idx).map(|s| app.is_drained(s)).unwrap_or(true);
        if drained {
            pool.try_close(idx, driver);
        } else if let Some(slot) = pool.get_mut(idx) {
            dirty.mark(idx, &slot.state.pending, PEND_CLOSE);
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
                    .map(|s| s.state.pending.shutdown_how())
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
        if matches!(outcome, ChunkOutcome::Overrun) {
            return Outcome::Overrun;
        }
        self.as_mut().submit_egress(idx, driver);
        match outcome {
            ChunkOutcome::Ok => Outcome::Ok,
            ChunkOutcome::Overrun => Outcome::Overrun,
            ChunkOutcome::CloseReconnect => Outcome::CloseAfter,
            ChunkOutcome::ClosePermanent => {
                let key = self.as_mut().project().pool.get(idx).map(|s| s.state.dial);
                if let Some(key) = key {
                    self.project().upstreams.kill(key);
                }
                Outcome::CloseAfter
            }
        }
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
        if matches!(outcome, ChunkOutcome::Overrun) {
            return Outcome::Overrun;
        }
        self.as_mut().submit_egress(idx, driver);
        match outcome {
            ChunkOutcome::Ok => Outcome::Ok,
            ChunkOutcome::Overrun => Outcome::Overrun,
            ChunkOutcome::CloseReconnect => Outcome::CloseAfter,
            ChunkOutcome::ClosePermanent => {
                let key = self.as_mut().project().pool.get(idx).map(|s| s.state.dial);
                if let Some(key) = key {
                    self.project().upstreams.kill(key);
                }
                Outcome::CloseAfter
            }
        }
    }

    fn handle_recv(
        mut self: Pin<&mut Self>,
        token: Token,
        more: bool,
        e: dope_core::io::RecvEvent,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        // Any inbound bytes prove the peer is alive → refresh the liveness clock
        // (the watchdog reads `last_recv` lazily; recv touches no timer).
        let now = driver.turn_now();
        let buffer = match e {
            dope_core::io::RecvEvent::Data { len, bid } => {
                Some(unsafe { ProvidedLease::from_completion(driver, len, bid) })
            }
            _ => None,
        };
        let outcome = self
            .as_mut()
            .project()
            .pool
            .dispatch_recv(token, more, e, buffer.as_ref());
        match outcome {
            DispatchRecv::Drop => {}
            DispatchRecv::Close(idx) => Self::close_slot(self.as_mut(), idx, driver),
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
                let outcome = if A::RETAIN_RAW_RECV && A::Wire::RAW_RECV {
                    let chunk = {
                        let lease = buffer
                            .as_ref()
                            .expect("raw receive chunk requires a provided buffer");
                        let (offset, len) = lease
                            .range_of(chunk.as_slice())
                            .expect("Wire::RAW_RECV chunk must reference its input");
                        drop(chunk);
                        lease.retained_view(offset, len)
                    };
                    self.as_mut().recv_retained_chunk(idx, chunk, driver)
                } else {
                    self.as_mut().recv_chunk(idx, chunk, driver)
                };
                match outcome {
                    Outcome::Ok => self.as_mut().maybe_close(idx, driver),
                    Outcome::Overrun => {
                        if let Some(slot) = self.as_mut().project().pool.get_mut(idx) {
                            slot.core.mark_aborted();
                        }
                        Self::close_slot(self.as_mut(), idx, driver)
                    }
                    Outcome::CloseAfter => {
                        self.as_mut().project().pool.set_close_after(idx);
                        self.as_mut().maybe_close(idx, driver);
                    }
                }
            }
        }
        if let Some(buffer) = buffer.as_ref() {
            buffer.release(driver);
        }
    }

    fn handle_send(
        mut self: Pin<&mut Self>,
        token: Token,
        e: dope_core::io::SendEvent,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let (idx, n) = match self.as_mut().project().pool.classify_send(driver, token, e) {
            SendOutcome::Sent { idx, n } => (idx, n),
            SendOutcome::Close(idx) => {
                return Self::close_slot(self.as_mut(), idx, driver);
            }
            SendOutcome::Drop => return,
        };
        {
            let this = self.as_mut().project();
            if let Some(slot) = this.pool.get_mut(idx) {
                if matches!(<A::Wire as Wire>::RECLAIM, Reclaim::OnComplete) {
                    slot.state.ack_send(n);
                }
                this.app.send(slot, n, driver);
                Self::drain_requests(this.app, this.dirty, idx, slot);
            }
        }
        self.as_mut().submit_egress(idx, driver);
        self.maybe_close(idx, driver);
    }

    fn maybe_close(mut self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>) {
        let close = {
            let this = self.as_ref().project_ref();
            let Some(slot) = this.pool.get(idx) else {
                return;
            };
            slot.core.should_close(this.app.defer_close(slot))
        };
        if close {
            Self::close_slot(self.as_mut(), idx, driver);
        }
    }
}
