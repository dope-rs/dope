use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Instant;

use super::SessionApp;
use super::app::{ChunkOutcome, ConnApp};
use super::protocol::Session;
use super::session::State;
use super::source::{Action, Dialer};
use crate::manifold::Outcome;
use crate::manifold::env::Env;
use crate::manifold::route::TypedToken;
use crate::manifold::timer::{HasTimer, Ticket, Timer};
use crate::transport::Transport;
use crate::transport::link::{
    ConnectStep, DispatchRecv, PEND_CLOSE, PEND_EGRESS, PEND_SHUTDOWN, Pool, SendOutcome,
    SocketStep,
};
use crate::transport::wire::{Reclaim, Wire};
use crate::{Drive, Driver, Lend, Profile, backend};

#[pin_project::pin_project(!Unpin)]
pub struct Core<const ID: u8, A, S, E>
where
    A: ConnApp,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport<Addr: Clone>,
{
    pub(super) pool: Pool<ID, E::Transport, A::Wire, State<A::Conn>>,
    pub(super) app: A,
    pub(super) upstreams: S,
    stream_opts: <E::Transport as Transport>::StreamOpts,
    wire_cfg: <A::Wire as Wire>::InitConfig,
    dirty: Vec<backend::token::LocalIdx>,
    backoff_timer: Option<Ticket>,
    timer: Timer<0>,
    backoff_slot: Box<backend::park::Slot>,
    draining: bool,
    _e: PhantomData<E>,
}

impl<const ID: u8, A, S, E> Core<ID, A, S, E>
where
    A: ConnApp,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn with_app(app: A, mut upstreams: S, max_conn: usize, driver: &mut Driver) -> Self {
        let reservation = driver
            .reserve_outbound(max_conn as u32)
            .expect("dope: connector outbound reservation");
        let backoff_sentinel = backend::token::Token::new(
            ID,
            backend::token::LocalIdx::new(max_conn as u32),
            backend::token::Epoch::INITIAL,
        );
        let backoff_slot = Box::new(backend::park::Parker::make_slot(&*driver, backoff_sentinel));
        upstreams.resize(max_conn);
        let pool = Pool::new(max_conn, reservation);
        Self {
            pool,
            app,
            upstreams,
            stream_opts: <E::Transport as Transport>::StreamOpts::default(),
            wire_cfg: <<A::Wire as Wire>::InitConfig as Default>::default(),
            dirty: Vec::with_capacity(max_conn),
            backoff_timer: None,
            timer: Timer::with_capacity(max_conn * 2 + 1),
            backoff_slot,
            draining: false,
            _e: PhantomData,
        }
    }

    pub fn set_cfg(&mut self, cfg: <A::Wire as Wire>::InitConfig) {
        self.wire_cfg = cfg;
    }

    pub fn app_mut(self: Pin<&mut Self>) -> &mut A {
        self.project().app
    }

    pub fn request_timer(self: Pin<&mut Self>) -> Pin<&mut Timer<0>> {
        Pin::new(self.project().timer)
    }

    pub fn dial(
        mut self: Pin<&mut Self>,
        addr: <E::Transport as Transport>::Addr,
        driver: &mut Driver,
    ) -> Option<u32> {
        let tag = self.as_mut().project().upstreams.dial(addr)?;
        self.poll_source(driver);
        Some(tag)
    }

    pub fn enqueue_dial(
        self: Pin<&mut Self>,
        addr: <E::Transport as Transport>::Addr,
    ) -> Option<u32> {
        self.project().upstreams.dial(addr)
    }

    pub fn set_stream_opts(self: Pin<&mut Self>, opts: <E::Transport as Transport>::StreamOpts) {
        *self.project().stream_opts = opts;
    }

    pub fn cancel_dial(self: Pin<&mut Self>, tag: u32) {
        let this = self.project();
        this.upstreams.kill(tag);
        let cap = this.pool.capacity() as u32;
        for raw in 0..cap {
            let idx = backend::token::LocalIdx::new(raw);
            let matches = this
                .pool
                .get(idx)
                .map(|s| s.state.upstream_tag == tag)
                .unwrap_or(false);
            if matches {
                if let Some(slot) = this.pool.get_mut(idx)
                    && slot.state.pending.mark(PEND_CLOSE)
                {
                    this.dirty.push(idx);
                }
                break;
            }
        }
    }

    pub fn shutdown_conn(self: Pin<&mut Self>, conn_id: backend::token::Token, how: i32) {
        let this = self.project();
        let Some(idx) = this.pool.decode_token(conn_id) else {
            return;
        };
        let Some(slot) = this.pool.get_mut(idx) else {
            return;
        };
        slot.state.pending.set_shutdown(how);
        if slot.state.pending.mark(PEND_SHUTDOWN) {
            this.dirty.push(idx);
        }
    }

    pub fn request_flush(self: Pin<&mut Self>, conn_id: backend::token::Token) {
        let this = self.project();
        let Some(local_idx) = this.pool.decode_token(conn_id) else {
            return;
        };
        let Some(slot) = this.pool.get_mut(local_idx) else {
            return;
        };
        if slot.state.pending.mark(PEND_EGRESS) {
            this.dirty.push(local_idx);
        }
    }

    pub fn revive_upstreams(self: Pin<&mut Self>) {
        let this = self.project();
        this.upstreams.revive();
        this.backoff_slot.wake();
    }

    pub fn request_close(self: Pin<&mut Self>, conn_id: backend::token::Token) {
        let now = Instant::now();
        let this = self.project();
        let Some(local_idx) = this.pool.decode_token(conn_id) else {
            return;
        };
        let tag = {
            let Some(slot) = this.pool.get_mut(local_idx) else {
                return;
            };
            let tag = slot.state.upstream_tag;
            if slot.state.pending.mark(PEND_CLOSE) {
                this.dirty.push(local_idx);
            }
            tag
        };
        this.upstreams.disconnect(tag, now);
    }

    pub fn state_for(
        self: Pin<&mut Self>,
        conn_id: backend::token::Token,
    ) -> Option<&mut State<A::Conn>> {
        let (_, slot) = self.project().pool.get_mut_by_target(conn_id)?;
        Some(&mut slot.state)
    }

    fn rouse(mut self: Pin<&mut Self>, driver: &mut Driver) {
        self.as_mut().poll_source(driver);
        self.flush_dirty(driver);
    }

    fn poll_source(self: Pin<&mut Self>, driver: &mut Driver) {
        let this = self.project();
        if *this.draining {
            return;
        }
        let backoff_fired = this.backoff_timer.is_some_and(|t| this.timer.is_fired(t));
        if !this.upstreams.has_pending() && !backoff_fired {
            return;
        }
        let now = Instant::now();
        if backoff_fired && let Some(t) = *this.backoff_timer {
            this.timer.cancel(t);
            *this.backoff_timer = None;
        }
        let cap = this.pool.capacity();
        for _ in 0..cap {
            match this.upstreams.poll_connect(now) {
                Action::Connect { addr, tag } => {
                    let state = State::<A::Conn> {
                        upstream_tag: tag,
                        ..Default::default()
                    };
                    let wire = <A::Wire as Wire>::new(this.wire_cfg);
                    let submitted = this.pool.submit_socket(&addr, wire, state, driver);
                    if submitted.is_none() {
                        this.upstreams.connect_deferred(tag, now);
                        break;
                    }
                }
                Action::Backoff { min_retry_at } => {
                    if this.backoff_timer.is_none() {
                        let waker = this.backoff_slot.wake_ref();
                        *this.backoff_timer = this.timer.try_arm(min_retry_at, waker);
                    }
                    break;
                }
                Action::Idle => break,
            }
        }
    }

    fn on_socket(
        self: Pin<&mut Self>,
        ud: backend::token::Token,
        e: backend::SocketEvent,
        driver: &mut Driver,
    ) {
        let now = Instant::now();
        let this = self.project();
        let Some(local) = this.pool.decode_token(ud) else {
            return;
        };
        let Some(slot) = this.pool.get(local) else {
            return;
        };
        let tag = slot.state.upstream_tag;
        let Some(sock_addr) = this.upstreams.sock_addr(tag) else {
            this.pool.release(local);
            this.upstreams.connect_outcome(tag, false, now);
            return;
        };
        let step = this
            .pool
            .drive_socket_cqe(ud, &e, sock_addr, this.stream_opts, driver);
        if let SocketStep::Failed = step {
            this.upstreams.connect_outcome(tag, false, now);
        }
    }

    fn on_connect(
        mut self: Pin<&mut Self>,
        ud: backend::token::Token,
        e: backend::ConnectEvent,
        driver: &mut Driver,
    ) {
        let now = Instant::now();
        let (idx, tag) = {
            let this = self.as_mut().project();
            let step = this
                .pool
                .drive_connect_cqe(ud, &e, driver, |slot| slot.state.upstream_tag);
            match step {
                ConnectStep::Connected { idx, peeked } => (idx, peeked),
                ConnectStep::Failed { peeked, .. } => {
                    this.app.on_connect_failed(peeked, driver);
                    this.upstreams.connect_outcome(peeked, false, now);
                    return;
                }
                ConnectStep::Drop { peeked } => {
                    if let Some(tag) = peeked {
                        this.app.on_connect_failed(tag, driver);
                        this.upstreams.connect_outcome(tag, false, now);
                    }
                    return;
                }
            }
        };
        {
            let this = self.as_mut().project();
            if let Some(slot) = this.pool.get_mut(idx) {
                this.app.on_connected(tag, slot, driver);
            }
        }
        self.as_mut().submit_egress(idx, driver);
        self.project().upstreams.connect_outcome(tag, true, now);
    }

    fn submit_egress(self: Pin<&mut Self>, idx: backend::token::LocalIdx, driver: &mut Driver) {
        let this = self.project();
        let Some((slot, ud)) = this.pool.send_slot(idx) else {
            return;
        };
        if !slot.state.establish.is_done() {
            return;
        }
        this.app.before_send(slot);
        let vectored = slot.state.prepare_send(u32::MAX as usize);
        if vectored.iovs.is_empty() {
            slot.flush_pending(ud, driver);
            return;
        }
        let consumed = slot
            .wire
            .submit_send_vectored(&mut slot.core, vectored, ud, driver);
        if matches!(<A::Wire as Wire>::RECLAIM, Reclaim::OnSubmit) {
            slot.state.ack_send(consumed);
        }
        if !slot.core.is_send_inflight() && slot.state.pending.mark(PEND_EGRESS) {
            this.dirty.push(idx);
        }
    }

    fn close_slot(self: Pin<&mut Self>, idx: backend::token::LocalIdx, driver: &mut Driver) {
        let now = Instant::now();
        let this = self.project();
        let slot_meta = this
            .pool
            .get(idx)
            .map(|s| (s.state.establish.is_done(), s.state.upstream_tag));
        if let Some((established, tag)) = slot_meta {
            if established && let Some(slot) = this.pool.get_mut(idx) {
                this.app.on_close(slot);
            }
            this.upstreams.disconnect(tag, now);
        }
        Self::drain_close(this.pool, this.dirty, this.app, idx, driver);
    }

    fn drain_close(
        pool: &mut Pool<ID, E::Transport, A::Wire, State<A::Conn>>,
        dirty: &mut Vec<backend::token::LocalIdx>,
        app: &A,
        idx: backend::token::LocalIdx,
        driver: &mut Driver,
    ) {
        let (send_inflight, establishing, connecting) = match pool.get(idx) {
            Some(s) => (
                s.core.is_send_inflight(),
                !s.state.establish.is_done(),
                s.state.establish.is_connecting(),
            ),
            None => return,
        };
        if establishing {
            let op_kind = if connecting {
                backend::token::kind::CONNECT
            } else {
                backend::token::kind::SOCKET
            };
            let ud = pool.op(idx);
            let cancelled = driver.push(backend::sqe::Sqe::cancel(ud, op_kind)).is_ok();
            if let Some(slot) = pool.get_mut(idx) {
                slot.core.begin_close();
                if !cancelled && slot.state.pending.mark(PEND_CLOSE) {
                    dirty.push(idx);
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
        let ud = pool.op(idx);
        if pool.get_mut(idx).is_some_and(|s| s.seal_graceful(ud, driver)) {
            return;
        }
        let drained = pool.get(idx).map(|s| app.is_drained(s)).unwrap_or(true);
        if drained {
            pool.try_close(idx, driver);
        } else if let Some(slot) = pool.get_mut(idx)
            && slot.state.pending.mark(PEND_CLOSE)
        {
            dirty.push(idx);
        }
    }

    fn flush_dirty(mut self: Pin<&mut Self>, driver: &mut Driver) {
        let n = self.as_ref().project_ref().dirty.len();
        for i in 0..n {
            let (idx, flags) = {
                let this = self.as_mut().project();
                let idx = this.dirty[i];
                let Some(slot) = this.pool.get_mut(idx) else {
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
                    let _ = <E::Transport as Transport>::submit_shutdown(fd, how, driver);
                }
            }
            if flags & PEND_CLOSE != 0 {
                let this = self.as_mut().project();
                Self::drain_close(this.pool, this.dirty, this.app, idx, driver);
            }
        }
        self.as_mut().project().dirty.drain(..n);
    }

    fn on_recv_chunk(
        mut self: Pin<&mut Self>,
        idx: backend::token::LocalIdx,
        chunk: crate::transport::wire::RecvChunk<'_>,
        driver: &mut Driver,
    ) -> Outcome {
        let outcome = {
            let this = self.as_mut().project();
            let Some(slot) = this.pool.get_mut(idx) else {
                return Outcome::Ok;
            };
            this.app.on_chunk(slot, chunk, driver)
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
                let tag = self
                    .as_mut()
                    .project()
                    .pool
                    .get(idx)
                    .map(|s| s.state.upstream_tag);
                if let Some(tag) = tag {
                    self.project().upstreams.kill(tag);
                }
                Outcome::CloseAfter
            }
        }
    }

    fn handle_recv(
        mut self: Pin<&mut Self>,
        token: backend::token::Token,
        more: bool,
        e: backend::RecvEvent,
        driver: &mut Driver,
    ) {
        let (cqe_bid, outcome) = self
            .as_mut()
            .project()
            .pool
            .dispatch_recv(token, more, e, driver);
        match outcome {
            DispatchRecv::Drop => {}
            DispatchRecv::Close(idx) => Self::close_slot(self.as_mut(), idx, driver),
            DispatchRecv::NoChunk(idx) => {
                self.as_mut().submit_egress(idx, driver);
                self.as_mut().maybe_close(idx, driver);
            }
            DispatchRecv::Chunk(idx, chunk) => {
                match self.as_mut().on_recv_chunk(idx, chunk, driver) {
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
        driver.release(cqe_bid);
    }

    fn handle_send(
        mut self: Pin<&mut Self>,
        token: backend::token::Token,
        e: backend::SendEvent,
        driver: &mut Driver,
    ) {
        let (idx, n) = match self.as_mut().project().pool.classify_send(token, e, driver) {
            SendOutcome::Sent { idx, n } => (idx, n),
            SendOutcome::Close(idx) => return Self::close_slot(self.as_mut(), idx, driver),
            SendOutcome::Drop => return,
        };
        {
            let this = self.as_mut().project();
            if let Some(slot) = this.pool.get_mut(idx) {
                if matches!(<A::Wire as Wire>::RECLAIM, Reclaim::OnComplete) {
                    slot.state.ack_send(n);
                }
                this.app.on_send(slot, n, driver);
            }
        }
        self.as_mut().submit_egress(idx, driver);
        self.maybe_close(idx, driver);
    }

    fn maybe_close(mut self: Pin<&mut Self>, idx: backend::token::LocalIdx, driver: &mut Driver) {
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

impl<const ID: u8, N, S, E> Core<ID, SessionApp<N, E::Wire>, S, E>
where
    N: Session,
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn new(session: N, upstreams: S, max_conn: usize, driver: &mut Driver) -> Self {
        let app = SessionApp {
            session,
            _w: PhantomData,
        };
        Self::with_app(app, upstreams, max_conn, driver)
    }

    pub fn session(&self) -> &N {
        &self.app.session
    }

    pub fn session_mut(self: Pin<&mut Self>) -> &mut N {
        &mut self.project().app.session
    }

    pub fn with_cfg(mut self, cfg: <E::Wire as Wire>::InitConfig) -> Self {
        self.set_cfg(cfg);
        self
    }
}

impl<const ID: u8, A, S, E> HasTimer for Core<ID, A, S, E>
where
    A: ConnApp,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport<Addr: Clone>,
{
    fn timer(self: Pin<&mut Self>) -> Pin<&mut Timer<0>> {
        self.request_timer()
    }
}

impl<const ID: u8, A, S, E> crate::manifold::Manifold for Core<ID, A, S, E>
where
    A: ConnApp,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport<Addr: Clone>,
{
    const ID: u8 = ID;

    fn dispatch(mut self: Pin<&mut Self>, ev: backend::Event, driver: &mut Driver) {
        match ev {
            backend::Event::Recv(token, more, e) => {
                self.as_mut().handle_recv(token, more, e, driver)
            }
            backend::Event::Send(token, e) => self.as_mut().handle_send(token, e, driver),
            backend::Event::Socket(token, e) => self.as_mut().on_socket(token, e, driver),
            backend::Event::Connect(token, e) => self.as_mut().on_connect(token, e, driver),
            _ => {}
        }
        self.as_mut().project().pool.flush_rearm(driver);
        self.flush_dirty(driver);
    }

    fn pre_park(mut self: Pin<&mut Self>, driver: &mut Driver) {
        {
            let this = self.as_mut().project();
            Pin::new(this.timer).pre_park(driver);
        }
        self.as_mut().poll_source(driver);
        self.flush_dirty(driver);
    }

    fn on_wake(self: Pin<&mut Self>, _target: TypedToken<Self>, driver: &mut Driver) {
        self.rouse(driver);
    }

    fn idle(self: Pin<&Self>) -> crate::runtime::dispatcher::Idle {
        let this = self.project_ref();
        if !this.dirty.is_empty() || (E::Profile::HYBRID_PARK && this.pool.pending_recv_rearm()) {
            return crate::runtime::dispatcher::Idle::Busy;
        }
        Pin::new(this.timer).idle()
    }

    fn on_shutdown(mut self: Pin<&mut Self>, driver: &mut Driver) {
        {
            let this = self.as_mut().project();
            *this.draining = true;
            if let Some(t) = this.backoff_timer.take() {
                this.timer.cancel(t);
            }
        }
        let cap = self.as_ref().project_ref().pool.capacity() as u32;
        for raw in 0..cap {
            self.as_mut()
                .close_slot(backend::token::LocalIdx::new(raw), driver);
        }
        self.flush_dirty(driver);
    }
}
