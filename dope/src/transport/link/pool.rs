use std::collections::VecDeque;
use std::marker::PhantomData;

use super::core::{Core, Outbound};
use super::slot::{RecvDecision, Slot};
use crate::slab::Slab;
use crate::transport::Transport;
use crate::transport::config::Submittable;
use crate::transport::wire::{RecvChunk, Wire};
use crate::{Drive, Driver, Lend, backend};

pub struct Pool<const ID: u8, T: Transport, W: Wire, S> {
    slab: Slab<Slot<W, S>>,
    reservation: backend::OutboundReservation,
    recv_rearm_pending: VecDeque<backend::token::LocalIdx>,
    rearm_present: Vec<bool>,
    _t: PhantomData<T>,
}

impl<const ID: u8, T: Transport, W: Wire, S> Pool<ID, T, W, S> {
    pub fn new(max_conn: usize, reservation: backend::OutboundReservation) -> Self {
        Self {
            slab: Slab::new(max_conn),
            reservation,
            recv_rearm_pending: VecDeque::new(),
            rearm_present: vec![false; max_conn],
            _t: PhantomData,
        }
    }

    fn queue_rearm(&mut self, idx: backend::token::LocalIdx) {
        let i = idx.raw() as usize;
        if i < self.rearm_present.len() && !self.rearm_present[i] {
            self.rearm_present[i] = true;
            self.recv_rearm_pending.push_back(idx);
            crate::memstats::starved_rearm_inc();
        }
    }

    pub fn capacity(&self) -> usize {
        self.slab.slot_count()
    }

    pub fn pending_recv_rearm(&self) -> bool {
        !self.recv_rearm_pending.is_empty()
    }

    pub fn fd_of(&self, local: backend::token::LocalIdx) -> Option<&backend::socket::Fd> {
        self.slab.at_index(local).map(|(v, _)| &v.core.fd)
    }

    pub fn place_at(&mut self, idx: backend::token::LocalIdx, core: Core, wire: W, state: S) {
        debug_assert!(
            self.slab.at_index(idx).is_none(),
            "Pool::place_at over occupied slot"
        );
        let park_slot = self.reservation.absolute(idx);
        self.slab.place_at(idx, |key| {
            let token = backend::token::Token::new(ID, key.index(), key.epoch());
            Slot::<W, S>::new(core, wire, token, park_slot, state)
        });
    }

    pub fn release(&mut self, idx: backend::token::LocalIdx) -> bool {
        let Some(epoch) = self.slab.epoch(idx) else {
            return false;
        };
        self.slab.remove(backend::token::Key::new(idx, epoch))
    }

    pub fn get(&self, idx: backend::token::LocalIdx) -> Option<&Slot<W, S>> {
        self.slab.at_index(idx).map(|(v, _)| v)
    }

    pub fn get_mut(&mut self, idx: backend::token::LocalIdx) -> Option<&mut Slot<W, S>> {
        self.slab.at_index_mut(idx)
    }

    pub fn get_mut_by_target(
        &mut self,
        target: backend::token::Token,
    ) -> Option<(backend::token::LocalIdx, &mut Slot<W, S>)> {
        let idx = self.decode_token(target)?;
        self.slab.at_index_mut(idx).map(|slot| (idx, slot))
    }

    fn current_epoch(&self, idx: backend::token::LocalIdx) -> backend::token::Epoch {
        self.slab
            .epoch(idx)
            .unwrap_or(backend::token::Epoch::INITIAL)
    }

    pub fn op(&self, idx: backend::token::LocalIdx) -> backend::token::Token {
        backend::token::Token::new(ID, idx, self.current_epoch(idx))
    }

    pub fn decode_token(&self, ud: backend::token::Token) -> Option<backend::token::LocalIdx> {
        debug_assert!(ud.route() == ID);
        let key = ud.key();
        self.slab.get(key).map(|_| key.index())
    }

    fn slot_of<'d>(
        &self,
        idx: backend::token::LocalIdx,
        driver: &'d Driver,
    ) -> &'d backend::park::Slot {
        backend::park::Parker::slot(driver, self.reservation.absolute(idx))
    }

    pub fn refresh_wake(&mut self, idx: backend::token::LocalIdx, driver: &Driver) {
        let target = self.op(idx);
        if self.get(idx).is_some() {
            self.slot_of(idx, driver).set_target(target);
        }
    }

    pub fn arm_recv(&mut self, idx: backend::token::LocalIdx, driver: &mut Driver) -> bool {
        let ud = self.op(idx);
        let armed = {
            let Some(slot) = self.slab.at_index_mut(idx) else {
                return false;
            };
            if slot.core.is_armed() {
                return true;
            }
            let buf_group = driver.group();
            let armed = driver
                .push(backend::sqe::Sqe::recv_multi(&slot.core.fd, buf_group, ud))
                .is_ok();
            slot.core.armed(armed);
            armed
        };
        if !armed {
            self.queue_rearm(idx);
        }
        armed
    }

    pub fn flush_rearm(&mut self, driver: &mut Driver) {
        let n = self.recv_rearm_pending.len();
        for _ in 0..n {
            let Some(idx) = self.recv_rearm_pending.pop_front() else {
                break;
            };
            let i = idx.raw() as usize;
            if i < self.rearm_present.len() {
                self.rearm_present[i] = false;
            }
            let skip = match self.get(idx) {
                Some(slot) => !slot.core.needs_arm(),
                None => true,
            };
            if skip {
                continue;
            }
            let _ = self.arm_recv(idx, driver);
        }
    }

    pub fn classify_send(
        &mut self,
        ud: backend::token::Token,
        e: backend::SendEvent,
        driver: &mut Driver,
    ) -> SendOutcome {
        let Some(idx) = self.decode_token(ud) else {
            return SendOutcome::Drop;
        };
        let Some(slot) = self.slab.at_index_mut(idx) else {
            return SendOutcome::Drop;
        };
        match e {
            backend::SendEvent::Sent(n) => slot.send_sent(n as usize, ud, idx, driver),
            backend::SendEvent::Failed(_) => slot.send_failed(idx),
        }
    }

    pub fn dispatch_recv<'a>(
        &mut self,
        ud: backend::token::Token,
        more: bool,
        e: backend::RecvEvent,
        driver: &mut Driver,
    ) -> (Option<u16>, DispatchRecv<'a>) {
        let bid = match e {
            backend::RecvEvent::Data { bid, .. } => Some(bid),
            _ => None,
        };
        let Some(idx) = self.decode_token(ud) else {
            return (bid, DispatchRecv::Drop);
        };
        let Some(slot) = self.slab.at_index_mut(idx) else {
            return (bid, DispatchRecv::Drop);
        };
        let decision = match e {
            backend::RecvEvent::Data { len, bid } => {
                // SAFETY: returned bid is released by the caller after the DispatchRecv is consumed.
                let slice = unsafe { driver.slice(len, bid) };
                slot.recv_data(more, slice)
            }
            backend::RecvEvent::Eof => slot.recv_eof(more),
            backend::RecvEvent::Cancelled => slot.recv_cancelled(more),
            backend::RecvEvent::Starved => slot.recv_starved(more),
            backend::RecvEvent::Failed(_) => slot.recv_failed(more),
        };
        let needs_rearm = match &decision {
            RecvDecision::NoChunk { needs_rearm } | RecvDecision::Chunk { needs_rearm, .. } => {
                *needs_rearm
            }
            _ => false,
        };
        if needs_rearm {
            self.queue_rearm(idx);
        }
        let outcome = match decision {
            RecvDecision::Drop => DispatchRecv::Drop,
            RecvDecision::Close => DispatchRecv::Close(idx),
            RecvDecision::NoChunk { .. } => DispatchRecv::NoChunk(idx),
            RecvDecision::Chunk { chunk, .. } => DispatchRecv::Chunk(idx, chunk),
        };
        (bid, outcome)
    }

    pub fn set_close_after(&mut self, idx: backend::token::LocalIdx) {
        if let Some(slot) = self.slab.at_index_mut(idx) {
            slot.core.set_close_after();
        }
    }

    pub fn try_close(&mut self, idx: backend::token::LocalIdx, driver: &mut Driver) {
        let Some((was_armed, send_inflight)) = self
            .get(idx)
            .map(|s| (s.core.is_armed(), s.core.is_send_inflight()))
        else {
            return;
        };
        if send_inflight {
            if let Some(slot) = self.slab.at_index_mut(idx) {
                slot.core.begin_close();
            }
            return;
        }
        if was_armed {
            let token = self.op(idx);
            let _ = driver.push(backend::sqe::Sqe::cancel(token, backend::token::kind::RECV));
        }
        let _ = self.release(idx);
    }
}

impl<const ID: u8, T: Transport, W: Wire, S> Pool<ID, T, W, S> {
    pub fn send_slot(
        &mut self,
        idx: backend::token::LocalIdx,
    ) -> Option<(&mut Slot<W, S>, backend::token::Token)> {
        let ud = self.op(idx);
        let slot = self.slab.at_index_mut(idx)?;
        if slot.core.is_closing() || slot.core.is_send_inflight() {
            return None;
        }
        Some((slot, ud))
    }

    pub fn submit_socket(
        &mut self,
        addr: &T::Addr,
        wire: W,
        state: S,
        driver: &mut Driver,
    ) -> Option<backend::token::LocalIdx> {
        let reservation = self.slab.reserve()?;
        let local = reservation.index();
        let epoch = reservation.epoch();
        let park_slot = self.reservation.try_absolute(local)?;
        let fd = backend::socket::Fd::adopt(park_slot, driver);
        let (domain, socket_type, protocol) = T::socket_params(addr);
        let ud = backend::token::Token::new(ID, local, epoch);
        let sqe = backend::sqe::Sqe::socket(domain, socket_type, protocol, &fd, ud).ok()?;
        if driver.push(sqe).is_err() {
            return None;
        }
        let slot = Slot::<W, S>::new(Core::new(fd), wire, ud, park_slot, state);
        reservation.fill(slot);
        self.refresh_wake(local, driver);
        Some(local)
    }

    pub fn drive_socket_cqe(
        &mut self,
        ud: backend::token::Token,
        e: &backend::SocketEvent,
        sock_addr: backend::socket::Addr,
        opts: &T::StreamOpts,
        driver: &mut Driver,
    ) -> SocketStep
    where
        S: Outbound,
    {
        let Some(local) = self.decode_token(ud) else {
            return SocketStep::Failed;
        };
        if let backend::SocketEvent::Failed(_) = e {
            self.release(local);
            return SocketStep::Failed;
        }
        let global = self.reservation.absolute(local);
        (*opts).submit(global.raw(), driver);
        let ud_connect = self.op(local);
        let submitted = {
            let Some(slot) = self.slab.at_index_mut(local) else {
                return SocketStep::Failed;
            };
            let (ptr, len) = slot.state.establish().begin(sock_addr);
            driver
                .push(backend::sqe::Sqe::connect(
                    &slot.core.fd,
                    ptr,
                    len,
                    ud_connect,
                ))
                .is_ok()
        };
        if submitted {
            SocketStep::Connecting { idx: local }
        } else {
            if let Some(slot) = self.slab.at_index_mut(local) {
                slot.state.establish().abort();
            }
            self.release(local);
            SocketStep::Failed
        }
    }

    pub fn drive_connect_cqe<X>(
        &mut self,
        ud: backend::token::Token,
        e: &backend::ConnectEvent,
        driver: &mut Driver,
        peek: impl FnOnce(&Slot<W, S>) -> X,
    ) -> ConnectStep<X>
    where
        S: Outbound,
    {
        let Some(idx) = self.decode_token(ud) else {
            return ConnectStep::Drop { peeked: None };
        };
        let peeked = {
            let Some(slot) = self.slab.at_index_mut(idx) else {
                return ConnectStep::Drop { peeked: None };
            };
            if !slot.state.establish().is_connecting() {
                let peeked = (!slot.state.establish().is_done()).then(|| peek(&*slot));
                return ConnectStep::Drop { peeked };
            }
            peek(&*slot)
        };
        if let backend::ConnectEvent::Failed(_) = e {
            if let Some(slot) = self.slab.at_index_mut(idx) {
                slot.state.establish().abort();
            }
            let _ = self.release(idx);
            return ConnectStep::Failed { idx, peeked };
        }
        if let Some(slot) = self.slab.at_index_mut(idx) {
            slot.state.establish().finish();
        }
        self.arm_recv(idx, driver);
        ConnectStep::Connected { idx, peeked }
    }
}

pub enum SocketStep {
    Connecting { idx: backend::token::LocalIdx },
    Failed,
}

pub enum ConnectStep<X> {
    Connected {
        idx: backend::token::LocalIdx,
        peeked: X,
    },
    Failed {
        idx: backend::token::LocalIdx,
        peeked: X,
    },
    Drop {
        peeked: Option<X>,
    },
}

pub enum DispatchRecv<'a> {
    Drop,
    Close(backend::token::LocalIdx),
    Chunk(backend::token::LocalIdx, RecvChunk<'a>),
    NoChunk(backend::token::LocalIdx),
}

pub enum SendOutcome {
    Sent {
        idx: backend::token::LocalIdx,
        n: usize,
    },
    Close(backend::token::LocalIdx),
    Drop,
}
