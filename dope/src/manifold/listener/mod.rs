pub mod accept;
mod application;
mod arena;
pub mod config;
mod idle;
pub mod recv;
pub mod send;

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Instant;

pub use accept::Accept;
pub use application::Application;
pub use config::Config;
use idle::IdleSet;

use crate::manifold::env::{Bundle, Env};
use crate::manifold::{Manifold, Outcome};
use crate::transport::config::Submittable;
use crate::transport::link::{
    Core, DeferredEgress, DispatchRecv, PEND_CLOSE, PEND_EGRESS, PEND_SHUTDOWN, PendingFlags, Pool,
    SendOutcome, Slot,
};
use crate::transport::wire::{Reclaim, Wire};
use crate::transport::{Tcp, Transport};
use crate::{Drive, Driver, Lend, Profile, backend};

pub struct State<C: Default + 'static> {
    pub conn: C,
    pub(super) send: send::State,
    pub(super) pending: PendingFlags,
    pub(super) deferred: DeferredEgress,
    pub(super) peer_ip: Option<std::net::IpAddr>,
}

impl<C: Default + 'static> Default for State<C> {
    fn default() -> Self {
        Self {
            conn: C::default(),
            send: send::State::default(),
            pending: PendingFlags::default(),
            deferred: DeferredEgress::default(),
            peer_ip: None,
        }
    }
}

impl<C: Default + 'static> State<C> {
    pub fn peer_ip(&self) -> Option<std::net::IpAddr> {
        self.peer_ip
    }
}

pub struct Aux {
    arena: arena::Arena,
    scratch: Box<[u8]>,
}

impl Aux {
    fn write_buf_raw<'d, W: Wire, C: Default + 'static>(
        &mut self,
        slot: &mut Slot<'d, W, State<C>>,
    ) -> &mut [u8] {
        let handle = slot.state.send.arena.get_or_insert_with(|| {
            self.arena
                .borrow()
                .expect("arena: max_conn hard cap guarantees a free buffer")
        });
        self.arena.slice(handle)
    }

    pub fn write_buf_for<'d, W: Wire, C: Default + 'static>(
        &mut self,
        slot: &mut Slot<'d, W, State<C>>,
    ) -> &mut [u8] {
        if slot.owes_egress() {
            return &mut self.scratch;
        }
        self.write_buf_raw(slot)
    }
}

#[pin_project::pin_project(!Unpin)]
pub struct Listener<
    'd,
    const ID: u8,
    A,
    E = Bundle<Tcp, <A as Application>::Wire, backend::profile::Production>,
> where
    A: Application,
    E: Env<Wire = A::Wire>,
{
    pool: Pool<'d, ID, E::Transport, A::Wire, State<A::Conn>>,
    app: A,
    aux: Aux,
    accept: Accept<'d, E::Transport>,
    bound_addr: SocketAddr,
    idle: IdleSet,
    idle_send: IdleSet,
    idle_abs_age: IdleSet,
    dirty: Vec<backend::token::LocalIdx>,
    wire_cfg: <A::Wire as Wire>::InitConfig,
}

/// What a slot owes the socket, most urgent first. `Held` outranks `Deferred`:
/// handing more bytes to a wire that is already sitting on plaintext would let
/// the two coalesce into one CQE and lose a completion.
enum Egress {
    Clear,
    Inflight,
    Plain,
    Held,
    Deferred,
}

impl<'d, W: Wire, C: Default + 'static> Slot<'d, W, State<C>> {
    fn egress(&self) -> Egress {
        if self.core.is_send_inflight() {
            Egress::Inflight
        } else if self.state.send.sent_plain < self.state.send.total_plain {
            Egress::Plain
        } else if self.wire.holds_plain() {
            Egress::Held
        } else if !self.state.deferred.is_idle() {
            Egress::Deferred
        } else {
            Egress::Clear
        }
    }

    fn owes_egress(&self) -> bool {
        !matches!(self.egress(), Egress::Clear)
    }

    /// Whether a send went out, and whether the slot still has bytes it can act
    /// on itself. [`Egress::Held`] is neither: only the wire can move it along.
    fn after_handoff(&mut self) -> (bool, bool) {
        let armed = self.core.is_send_inflight();
        let restage = matches!(self.egress(), Egress::Plain | Egress::Deferred)
            && self.state.pending.mark(PEND_EGRESS);
        (armed, restage)
    }

    fn adopt_deferred_close(&mut self) {
        if self.state.deferred.close_after() {
            self.core.set_close_after();
        }
    }

    fn accept_write(&mut self, write_buf_cap: usize, written: usize) -> bool {
        if written > write_buf_cap {
            self.core.set_close_after();
            return false;
        }
        self.state.send.write_buf_len = written;
        true
    }

    fn hand_plain(&mut self, plain: &[u8], ud: backend::token::Token, driver: &'d Driver) {
        let was_inflight = self.core.is_send_inflight();
        let consumed = self.wire.submit_send(&mut self.core, plain, ud, driver);
        let armed = self.core.is_send_inflight() && !was_inflight;
        self.state.send.record_handoff(consumed, armed);
    }

    fn hand_split(
        &mut self,
        iovs: [backend::socket::IoVec; 2],
        ud: backend::token::Token,
        driver: &'d Driver,
    ) {
        let was_inflight = self.core.is_send_inflight();
        let vectored = crate::transport::wire::Vectored {
            iovs: &iovs,
            iov_storage: &mut self.state.send.pending_iovs,
            msghdr_storage: &mut self.state.send.pending_msghdr,
        };
        let consumed = self
            .wire
            .submit_send_vectored(&mut self.core, vectored, ud, driver);
        let armed = self.core.is_send_inflight() && !was_inflight;
        self.state.send.record_handoff(consumed, armed);
    }

    pub fn submit_buffered(
        &mut self,
        write_buf: &mut [u8],
        written: usize,
        ud: backend::token::Token,
        driver: &'d Driver,
    ) {
        if self.owes_egress() {
            let n = written.min(write_buf.len());
            let bytes = o3::buffer::Shared::copy_from_slice(&write_buf[..n]);
            if !self.state.deferred.stage(bytes, false) {
                self.core.set_close_after();
            }
            return;
        }
        if !self.accept_write(write_buf.len(), written) {
            return;
        }
        self.state.send.begin(written, send::SendSource::None);
        self.hand_plain(&write_buf[..written], ud, driver);
    }

    pub fn submit_split_static(
        &mut self,
        write_buf: &mut [u8],
        hdr_written: usize,
        body: &'static [u8],
        ud: backend::token::Token,
        driver: &'d Driver,
    ) {
        self.submit_split(
            write_buf,
            hdr_written,
            send::SendSource::Static(body),
            ud,
            driver,
        );
    }

    pub fn submit_split_shared(
        &mut self,
        write_buf: &mut [u8],
        hdr_written: usize,
        body: o3::buffer::Shared,
        ud: backend::token::Token,
        driver: &'d Driver,
    ) {
        self.submit_split(
            write_buf,
            hdr_written,
            send::SendSource::Shared(body),
            ud,
            driver,
        );
    }

    fn submit_split(
        &mut self,
        write_buf: &mut [u8],
        hdr_written: usize,
        source: send::SendSource,
        ud: backend::token::Token,
        driver: &'d Driver,
    ) {
        if self.owes_egress() {
            let n = hdr_written.min(write_buf.len());
            let hdr_ok = self
                .state
                .deferred
                .stage(o3::buffer::Shared::copy_from_slice(&write_buf[..n]), false);
            let body = source.body();
            // a headerless body would be a corrupt frame
            let body_ok = hdr_ok
                && (body.is_empty()
                    || self
                        .state
                        .deferred
                        .stage(o3::buffer::Shared::copy_from_slice(body), false));
            if !hdr_ok || !body_ok {
                self.core.set_close_after();
            }
            return;
        }
        if !self.accept_write(write_buf.len(), hdr_written) {
            return;
        }
        self.state
            .send
            .begin(hdr_written + source.body().len(), source);
        self.resume_send(write_buf, ud, driver);
    }

    fn resume_send(&mut self, write_buf: &[u8], ud: backend::token::Token, driver: &'d Driver) {
        let sent = self.state.send.sent_plain;
        let hdr_len = self.state.send.write_buf_len;
        let hdr_rem: &[u8] = if sent < hdr_len {
            &write_buf[sent..hdr_len]
        } else {
            &[]
        };
        let body = self.state.send.source.body();
        let body_off = sent.saturating_sub(hdr_len);
        let body_rem: &[u8] = if body_off < body.len() {
            &body[body_off..]
        } else {
            &[]
        };
        let iovs = [
            backend::socket::IoVec::from_slice(hdr_rem),
            backend::socket::IoVec::from_slice(body_rem),
        ];
        self.hand_split(iovs, ud, driver);
    }
}

impl<'d, const ID: u8, A, E> Listener<'d, ID, A, E>
where
    A: Application,
    E: Env<Wire = A::Wire>,
{
    fn new(
        listener_fd: backend::socket::Fd<'d>,
        bound_addr: SocketAddr,
        app: A,
        max_conn: usize,
        stream_opts: <E::Transport as Transport>::StreamOpts,
        per_ip_cap: u32,
    ) -> Self {
        Self {
            pool: Pool::new(max_conn, backend::OutboundReservation::empty()),
            accept: Accept::new(listener_fd, max_conn as u32, stream_opts, per_ip_cap),
            bound_addr,
            app,
            aux: Aux {
                arena: arena::Arena::new(max_conn),
                scratch: vec![0u8; send::WRITE_BUF_CAP].into_boxed_slice(),
            },
            idle: IdleSet::new(max_conn, E::Profile::IDLE_WINDOW),
            idle_send: IdleSet::new(
                if E::Profile::SEND_DEADLINE.is_some() {
                    max_conn
                } else {
                    0
                },
                E::Profile::SEND_DEADLINE.unwrap_or(std::time::Duration::ZERO),
            ),
            idle_abs_age: IdleSet::new(
                if E::Profile::ABS_CONN_AGE.is_some() {
                    max_conn
                } else {
                    0
                },
                E::Profile::ABS_CONN_AGE.unwrap_or(std::time::Duration::ZERO),
            ),
            dirty: Vec::new(),
            wire_cfg: <<A::Wire as Wire>::InitConfig as Default>::default(),
        }
    }

    pub fn set_cfg(&mut self, cfg: <A::Wire as Wire>::InitConfig) {
        self.wire_cfg = cfg;
    }

    pub fn open_in(app: A, cfg: Config<E::Transport>, driver: &'d Driver) -> io::Result<Self> {
        let mut listener = Self::assemble(app, cfg, driver)?;
        listener.accept.request_rearm();
        Ok(listener)
    }

    fn assemble(app: A, cfg: Config<E::Transport>, driver: &'d Driver) -> io::Result<Self> {
        let Config {
            max_conn,
            bind,
            backlog,
            mut stream_opts,
            listener_opts,
        } = cfg;
        <E::Transport as Transport>::apply_profile_defaults(
            &mut stream_opts,
            E::Profile::USER_TIMEOUT,
        );
        assert!(
            max_conn > 0 && max_conn <= backend::token::SLOT_MASK as usize,
            "max_conn must be in 1..=(1<<24)-1"
        );
        let (listener_fd, bound_addr) = <E::Transport as Transport>::bind_listener_slot(
            driver,
            &bind,
            backlog,
            &listener_opts,
        )?;
        let per_ip_cap = <E::Transport as Transport>::per_ip_cap(&listener_opts)
            .unwrap_or(E::Profile::PER_IP_CAP);
        Ok(Self::new(
            listener_fd,
            bound_addr,
            app,
            max_conn,
            stream_opts,
            per_ip_cap,
        ))
    }

    fn flush_dirty(mut self: Pin<&mut Self>, driver: &'d Driver) {
        let n = self.as_ref().project_ref().dirty.len();
        for i in 0..n {
            let (local_idx, flags) = {
                let this = self.as_mut().project();
                let local_idx = this.dirty[i];
                let Some(slot) = this.pool.get_mut(local_idx) else {
                    continue;
                };
                (local_idx, slot.state.pending.take_flags())
            };
            if flags & PEND_CLOSE != 0 {
                Self::close_inherent(self.as_mut(), local_idx, driver);
                continue;
            }
            if flags & PEND_EGRESS != 0 {
                self.as_mut().commit_chunk(local_idx, driver);
            }
            if flags & PEND_SHUTDOWN != 0 {
                let this = self.as_mut().project();
                let how = this
                    .pool
                    .get(local_idx)
                    .map(|s| s.state.pending.shutdown_how())
                    .unwrap_or(0);
                if let Some(fd) = this.pool.fd_of(local_idx) {
                    let _ = <E::Transport as Transport>::submit_shutdown(fd, how, driver);
                }
            }
        }
        self.as_mut().project().dirty.drain(..n);
    }

    fn commit_chunk(
        mut self: Pin<&mut Self>,
        local_idx: backend::token::LocalIdx,
        driver: &'d Driver,
    ) {
        let (armed_send, restage) = {
            let this = self.as_mut().project();
            let send_ud = this.pool.op(local_idx);
            let Some(slot) = this.pool.get_mut(local_idx) else {
                return;
            };
            if slot.core.is_closing() {
                (false, false)
            } else {
                match slot.egress() {
                    Egress::Inflight => (false, false),
                    Egress::Plain => {
                        let write_buf = this.aux.write_buf_raw(slot);
                        slot.resume_send(write_buf, send_ud, driver);
                        slot.after_handoff()
                    }
                    Egress::Held | Egress::Clear => {
                        slot.adopt_deferred_close();
                        slot.flush_pending(send_ud, driver);
                        (false, false)
                    }
                    Egress::Deferred => {
                        slot.adopt_deferred_close();
                        let vectored = slot.state.deferred.prepare_send(u32::MAX as usize);
                        let consumed = slot.wire.submit_send_vectored(
                            &mut slot.core,
                            vectored,
                            send_ud,
                            driver,
                        );
                        if matches!(<A::Wire as Wire>::RECLAIM, Reclaim::OnSubmit) {
                            slot.state.deferred.ack(consumed);
                        }
                        slot.after_handoff()
                    }
                }
            }
        };
        if armed_send && E::Profile::SEND_DEADLINE.is_some() {
            self.as_mut()
                .project()
                .idle_send
                .arm(local_idx, Instant::now());
        }
        if restage {
            self.as_mut().project().dirty.push(local_idx);
        }
        self.maybe_close_inherent(local_idx, driver);
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr>
    where
        E::Transport: 'static,
    {
        Ok(self.bound_addr)
    }

    pub fn handler_mut(&mut self) -> &mut A {
        &mut self.app
    }

    pub fn handler_mut_pin(self: Pin<&mut Self>) -> &mut A {
        self.project().app
    }

    pub fn send_buf(self: Pin<&mut Self>, conn_id: backend::token::Token) -> Option<&mut [u8]> {
        let this = self.project();
        let local_idx = this.pool.decode_token(conn_id)?;
        let slot = this.pool.get_mut(local_idx)?;
        Some(this.aux.write_buf_for(slot))
    }

    pub fn commit_send(
        mut self: Pin<&mut Self>,
        conn_id: backend::token::Token,
        written: usize,
        driver: &'d Driver,
    ) -> bool {
        let local_idx = {
            let this = self.as_mut().project();
            let Some(idx) = this.pool.decode_token(conn_id) else {
                return false;
            };
            let ud = this.pool.op(idx);
            let Some(slot) = this.pool.get_mut(idx) else {
                return false;
            };
            let buf = this.aux.write_buf_for(slot);
            slot.submit_buffered(buf, written, ud, driver);
            idx
        };
        self.as_mut().arm_send_deadline(local_idx);
        self.maybe_close_inherent(local_idx, driver);
        true
    }

    fn arm_send_deadline(self: Pin<&mut Self>, idx: backend::token::LocalIdx) {
        if E::Profile::SEND_DEADLINE.is_none() {
            return;
        }
        let this = self.project();
        let inflight = this
            .pool
            .get(idx)
            .map(|s| s.core.is_send_inflight())
            .unwrap_or(false);
        if inflight {
            this.idle_send.arm(idx, Instant::now());
        }
    }

    pub fn set_close_after(self: Pin<&mut Self>, conn_id: backend::token::Token) {
        let this = self.project();
        if let Some(idx) = this.pool.decode_token(conn_id) {
            this.pool.set_close_after(idx);
        }
    }

    pub fn conn_view_mut(
        self: Pin<&mut Self>,
        conn_id: backend::token::Token,
    ) -> Option<ConnView<'_, A::Conn>> {
        let this = self.project();
        let (_, slot) = this.pool.get_mut_by_target(conn_id)?;
        let inflight = slot.owes_egress();
        Some(ConnView {
            state: &mut slot.state.conn,
            inflight,
        })
    }

    pub fn mark_send(
        self: Pin<&mut Self>,
        conn_id: backend::token::Token,
        bytes: o3::buffer::Shared,
    ) -> bool {
        let this = self.project();
        let Some((local_idx, slot)) = this.pool.get_mut_by_target(conn_id) else {
            return false;
        };
        let staged = slot.state.deferred.stage(bytes, false);
        if slot.state.pending.mark(PEND_EGRESS) {
            this.dirty.push(local_idx);
        }
        staged
    }

    pub fn shutdown(self: Pin<&mut Self>, conn_id: backend::token::Token, how: i32) {
        let this = self.project();
        let Some((local_idx, slot)) = this.pool.get_mut_by_target(conn_id) else {
            return;
        };
        slot.state.pending.set_shutdown(how);
        if slot.state.pending.mark(PEND_SHUTDOWN) {
            this.dirty.push(local_idx);
        }
    }

    pub fn close(self: Pin<&mut Self>, conn_id: backend::token::Token) {
        let this = self.project();
        let Some((local_idx, slot)) = this.pool.get_mut_by_target(conn_id) else {
            return;
        };
        if slot.state.pending.mark(PEND_CLOSE) {
            this.dirty.push(local_idx);
        }
    }

    fn pump_recv(
        mut self: Pin<&mut Self>,
        token: backend::token::Token,
        more: bool,
        e: backend::RecvEvent,
        driver: &'d Driver,
    ) {
        let (cqe_bid, outcome) = {
            let this = self.as_mut().project();
            this.pool.dispatch_recv(token, more, e, driver)
        };
        match outcome {
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
            DispatchRecv::Chunk(idx, chunk) => {
                let app_outcome = {
                    let this = self.as_mut().project();
                    let now = Instant::now();
                    this.idle.arm(idx, now);
                    if let Some(slot) = this.pool.get_mut(idx) {
                        this.app.on_chunk(slot, chunk, this.aux, driver)
                    } else {
                        Outcome::Ok
                    }
                };
                match app_outcome {
                    Outcome::Ok => {
                        self.as_mut().arm_send_deadline(idx);
                        self.as_mut().maybe_close_inherent(idx, driver);
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
                }
            }
        }
        driver.release(cqe_bid);
    }

    fn pump_send(
        mut self: Pin<&mut Self>,
        token: backend::token::Token,
        e: backend::SendEvent,
        driver: &'d Driver,
    ) {
        let this = self.as_mut().project();
        let (idx, sent) = match this.pool.classify_send(token, e, driver) {
            SendOutcome::Sent { idx: i, n } => (i, n),
            SendOutcome::Close(i) => {
                Self::close_inherent(self.as_mut(), i, driver);
                return;
            }
            SendOutcome::Drop => return,
        };
        if E::Profile::SEND_DEADLINE.is_some() {
            this.idle_send.cancel(idx);
        }
        let queue_path = this
            .pool
            .get(idx)
            .map(|s| s.state.send.total_plain == 0)
            .unwrap_or(false);
        if queue_path {
            if let Some(slot) = this.pool.get_mut(idx)
                && matches!(<A::Wire as Wire>::RECLAIM, Reclaim::OnComplete)
            {
                slot.state.deferred.ack(sent);
            }
            {
                let Some(slot) = this.pool.get_mut(idx) else {
                    return;
                };
                this.app.on_send(slot, sent, this.aux, driver);
            }
            self.as_mut().commit_chunk(idx, driver);
            return;
        }
        let armed_plain = this
            .pool
            .get(idx)
            .map(|s| s.state.send.armed_plain)
            .unwrap_or(0);
        if matches!(<A::Wire as Wire>::RECLAIM, Reclaim::OnComplete) && sent != armed_plain {
            self.close_inherent(idx, driver);
            return;
        }
        let needs_more = this
            .pool
            .get(idx)
            .map(|s| s.state.send.sent_plain < s.state.send.total_plain)
            .unwrap_or(false);
        if needs_more {
            let send_ud = backend::token::Token::new(ID, idx, token.epoch());
            let armed = if let Some(slot) = this.pool.get_mut(idx) {
                let write_buf = this.aux.write_buf_raw(slot);
                slot.resume_send(write_buf, send_ud, driver);
                slot.core.is_send_inflight()
            } else {
                false
            };
            if armed && E::Profile::SEND_DEADLINE.is_some() {
                this.idle_send.arm(idx, Instant::now());
            }
            return;
        }
        {
            let Some(slot) = this.pool.get_mut(idx) else {
                return;
            };
            let total = slot.state.send.total_plain;
            slot.state.send.reset();
            // C1: the response fully drained and the SEND CQE has been observed,
            // so the SEND SQE that referenced the arena buffer by raw pointer is
            // complete and the kernel will not touch it again. Return the handle
            // to the per-thread free list now (not at close) so the arena scales
            // with concurrent in-flight writers, not live conns. Returning before
            // the CQE (while is_send_inflight()) would be a use-after-free.
            debug_assert!(
                !slot.core.is_send_inflight(),
                "arena buffer returned while a SEND SQE is still in flight"
            );
            if let Some(h) = slot.state.send.arena.take() {
                this.aux.arena.release(h);
            }
            this.app.on_send(slot, total, this.aux, driver);
        }
        self.as_mut().maybe_close_inherent(idx, driver);
    }

    fn maybe_close_inherent(
        mut self: Pin<&mut Self>,
        idx: backend::token::LocalIdx,
        driver: &'d Driver,
    ) {
        enum Step {
            Close,
            Retry,
            Idle,
        }
        let step = {
            let this = self.as_ref().project_ref();
            let Some(slot) = this.pool.get(idx) else {
                return;
            };
            let defer = this.app.defer_close(slot);
            if slot.core.is_closing() {
                if slot.core.should_close(defer) {
                    Step::Close
                } else {
                    Step::Idle
                }
            } else {
                match slot.egress() {
                    Egress::Plain | Egress::Deferred => Step::Retry,
                    Egress::Inflight | Egress::Held => Step::Idle,
                    Egress::Clear if slot.core.should_close(defer) => Step::Close,
                    Egress::Clear => Step::Idle,
                }
            }
        };
        match step {
            Step::Close => Self::close_inherent(self.as_mut(), idx, driver),
            Step::Retry => {
                let this = self.as_mut().project();
                if let Some(slot) = this.pool.get_mut(idx)
                    && slot.state.pending.mark(PEND_EGRESS)
                {
                    this.dirty.push(idx);
                }
            }
            Step::Idle => {}
        }
    }
}

pub struct ConnView<'a, T> {
    pub state: &'a mut T,
    pub inflight: bool,
}

impl<'d, const ID: u8, A, E> Listener<'d, ID, A, E>
where
    A: Application,
    E: Env<Wire = A::Wire>,
{
    fn accept_inherent(
        mut self: Pin<&mut Self>,
        token: backend::token::Token,
        more: bool,
        e: backend::AcceptEvent,
        driver: &'d Driver,
    ) {
        let (fixed_idx, overflow) = {
            let this = self.as_mut().project();
            let (fixed_fd, peer_ip) = match this.accept.on_completion(token, more, e, driver) {
                accept::Outcome::Accepted(fd, ip) => (fd, ip),
                accept::Outcome::Capped(ip) => {
                    this.app.on_capped(ip);
                    return;
                }
                accept::Outcome::Rejected => return,
            };
            let fixed_idx_raw = fixed_fd.index();
            let fixed_idx = backend::token::LocalIdx::new(fixed_idx_raw);
            let conn_slot: State<A::Conn> = State {
                peer_ip,
                ..Default::default()
            };
            this.pool.place_at(
                fixed_idx,
                Core::new(fixed_fd, <E::Transport as Transport>::KERNEL_DISCARD),
                <A::Wire as Wire>::new(this.wire_cfg),
                conn_slot,
            );
            this.pool.refresh_wake(fixed_idx, driver);
            let now = Instant::now();
            this.idle.arm(fixed_idx, now);
            if E::Profile::ABS_CONN_AGE.is_some() {
                this.idle_abs_age.arm(fixed_idx, now);
            }
            let _ = <E::Transport as Transport>::submit_quickack(
                this.pool.fd_of(fixed_idx).unwrap(),
                driver,
            );
            (*this.accept.stream_opts()).submit(fixed_idx_raw, driver);
            this.pool.arm_recv(fixed_idx, driver);
            let overflow = match this.pool.get_mut(fixed_idx) {
                Some(slot) => {
                    matches!(this.app.on_accept(slot, this.aux, driver), Outcome::Overrun)
                }
                None => false,
            };
            (fixed_idx, overflow)
        };
        if overflow {
            Self::close_inherent(self.as_mut(), fixed_idx, driver);
        }
    }

    fn drain_idle<F>(mut self: Pin<&mut Self>, now: Instant, driver: &'d Driver, project: F)
    where
        F: Fn(Pin<&mut Self>) -> &mut IdleSet,
    {
        while let Some(idx) = project(self.as_mut()).pop_expired(now) {
            Self::close_inherent(self.as_mut(), idx, driver);
        }
    }

    fn flush_after_recv(
        mut self: Pin<&mut Self>,
        idx: backend::token::LocalIdx,
        epoch: backend::token::Epoch,
        refresh_idle: bool,
        driver: &'d Driver,
    ) {
        {
            let this = self.as_mut().project();
            if refresh_idle {
                this.idle.arm(idx, Instant::now());
            }
            let ud = backend::token::Token::new(ID, idx, epoch);
            if let Some(slot) = this.pool.get_mut(idx) {
                slot.flush_pending(ud, driver);
            }
        }
        self.as_mut().maybe_close_inherent(idx, driver);
    }

    fn close_inherent(self: Pin<&mut Self>, idx: backend::token::LocalIdx, driver: &'d Driver) {
        let this = self.project();
        this.idle.cancel(idx);
        if E::Profile::SEND_DEADLINE.is_some() {
            this.idle_send.cancel(idx);
        }
        if E::Profile::ABS_CONN_AGE.is_some() {
            this.idle_abs_age.cancel(idx);
        }
        let (send_inflight, is_armed, is_closing, cancel_kind) = match this.pool.get(idx) {
            Some(s) => (
                s.core.is_send_inflight(),
                s.core.is_armed(),
                s.core.is_closing(),
                s.core.recv_cancel_kind(),
            ),
            None => return,
        };
        if send_inflight || is_armed {
            if !is_closing {
                let recv_token = this.pool.op(idx);
                if let Some(slot) = this.pool.get_mut(idx) {
                    slot.core.begin_close();
                }
                if is_armed && !send_inflight {
                    let _ = driver.push(backend::sqe::Sqe::cancel(recv_token, cancel_kind));
                }
            }
            return;
        }
        let ud = this.pool.op(idx);
        if this
            .pool
            .get_mut(idx)
            .is_some_and(|s| s.seal_graceful(ud, driver))
        {
            return;
        }
        if let Some(slot) = this.pool.get_mut(idx) {
            this.app.on_close(slot, this.aux);
            if let Some(h) = slot.state.send.arena.take() {
                this.aux.arena.release(h);
            }
            if let Some(ip) = slot.state.peer_ip.take() {
                this.accept.release_peer_ip(ip);
            }
        }
        this.pool.try_close(idx, driver);
    }
}

impl<'d, const ID: u8, A, E> Manifold<'d> for Listener<'d, ID, A, E>
where
    A: Application,
    E: Env<Wire = A::Wire>,
{
    const ID: u8 = ID;

    fn dispatch(mut self: Pin<&mut Self>, ev: backend::Event, driver: &'d Driver) {
        match ev {
            backend::Event::Recv(token, more, e) => self.as_mut().pump_recv(token, more, e, driver),
            backend::Event::Send(token, e) => self.as_mut().pump_send(token, e, driver),
            backend::Event::Accept(token, more, e) => {
                self.as_mut().accept_inherent(token, more, e, driver)
            }
            _ => {}
        }
        let this = self.as_mut().project();
        if this.accept.needs_rearm() {
            this.accept.arm(ID, driver);
        }
        this.pool.flush_rearm(driver);
        self.flush_dirty(driver);
    }

    fn on_wake(
        mut self: Pin<&mut Self>,
        target: crate::manifold::route::TypedToken<Self>,
        driver: &'d Driver,
    ) {
        let conn_id = target.token();
        let local_idx = {
            let this = self.as_mut().project();
            let Some(local_idx) = this.pool.decode_token(conn_id) else {
                return;
            };
            if let Some(slot) = this.pool.get_mut(local_idx) {
                this.app.on_wake(slot, this.aux, driver);
            }
            local_idx
        };
        self.as_mut().maybe_close_inherent(local_idx, driver);
        self.flush_dirty(driver);
    }

    fn on_shutdown(mut self: Pin<&mut Self>, driver: &'d Driver) {
        let cap = {
            let this = self.as_mut().project();
            this.accept.stop_accept(ID, driver);
            this.pool.capacity() as u32
        };
        for raw in 0..cap {
            Self::close_inherent(self.as_mut(), backend::token::LocalIdx::new(raw), driver);
        }
    }

    fn pre_park(mut self: Pin<&mut Self>, driver: &'d Driver) {
        {
            let this = self.as_mut().project();
            if this.accept.needs_rearm() {
                this.accept.arm(ID, driver);
            }
            this.pool.flush_rearm(driver);
        }
        self.as_mut().flush_dirty(driver);
        let now = Instant::now();
        self.as_mut().drain_idle(now, driver, |s| s.project().idle);
        if E::Profile::SEND_DEADLINE.is_some() {
            self.as_mut()
                .drain_idle(now, driver, |s| s.project().idle_send);
        }
        if E::Profile::ABS_CONN_AGE.is_some() {
            self.as_mut()
                .drain_idle(now, driver, |s| s.project().idle_abs_age);
        }
        self.flush_dirty(driver);
    }

    fn idle(self: Pin<&Self>) -> crate::runtime::dispatcher::Idle {
        let this = self.project_ref();
        if !this.dirty.is_empty()
            || this.accept.needs_rearm()
            || (<E::Profile as backend::profile::Profile>::HYBRID_PARK
                && this.pool.pending_recv_rearm())
        {
            return crate::runtime::dispatcher::Idle::Busy;
        }
        let deadline = [
            this.idle.earliest(),
            this.idle_send.earliest(),
            this.idle_abs_age.earliest(),
        ]
        .into_iter()
        .flatten()
        .min();
        crate::runtime::dispatcher::Idle::Park(deadline)
    }
}
