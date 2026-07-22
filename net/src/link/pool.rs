use std::io;
use std::marker::PhantomData;

use super::core::{Core, Outbound};
use super::slot::{RecvDecision, Slot};
use crate::Transport;
use crate::wire::{RuntimeLimits, Wire};
use dope_core::backend::Sqe;
use dope_core::driver::buffers::ProvidedBuffers;
use dope_core::driver::control::ContextControl;
use dope_core::driver::submission::Submission;
use dope_core::driver::token::{
    Epoch, KeyTag, ROUTE_FRAMEWORK, SLOT_MASK, SlotIndex, Token, TokenSlab, kind,
};
use dope_core::driver::{DriverContext, OutboundReservation};
use dope_core::io::fd::Fd;
use dope_core::io::provided::ProvidedView;
use dope_core::io::socket::addr::Addr;
use dope_core::io::{ConnectEvent, RecvEvent, SendEvent, SocketEvent};
use o3::buffer::ByteSpan;
use o3::collections::FixedQueue;

pub struct Pool<'d, const ID: u8, T: Transport, W: Wire, S> {
    slab: TokenSlab<Slot<'d, W, S>, KeyTag<ID>>,
    runtime: W::RuntimeContext,
    reservation: OutboundReservation,
    recv_rearm_pending: FixedQueue<SlotIndex>,
    rearm_epoch: Box<[Epoch]>,
    poison_route: bool,
    _t: PhantomData<T>,
}

impl<'d, const ID: u8, T: Transport, W: Wire, S> Pool<'d, ID, T, W, S> {
    pub fn new(
        max_connections: usize,
        max_retained_recv_chunks: usize,
        reservation: OutboundReservation,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        if max_connections > SLOT_MASK as usize + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope: connection limit exceeds token slots",
            ));
        }
        Ok(Self {
            slab: TokenSlab::with_capacity(max_connections),
            runtime: W::runtime_context(RuntimeLimits::new(
                max_connections,
                max_retained_recv_chunks,
                driver.buffer_len(),
            ))?,
            reservation,
            recv_rearm_pending: FixedQueue::with_capacity(max_connections),
            rearm_epoch: vec![Epoch::ZERO; max_connections].into_boxed_slice(),
            poison_route: false,
            _t: PhantomData,
        })
    }

    fn queue_rearm(&mut self, token: Token) {
        let index = token.slot().raw() as usize;
        let epoch = unsafe { self.rearm_epoch.get_unchecked_mut(index) };
        if *epoch == Epoch::ZERO {
            let Some(entry) = self.recv_rearm_pending.vacant_entry() else {
                unreachable!()
            };
            entry.push_back(token.slot());
        }
        *epoch = token.epoch();
    }

    pub fn capacity(&self) -> usize {
        self.slab.capacity()
    }

    pub fn pending_recv_rearm(&self) -> bool {
        !self.recv_rearm_pending.is_empty()
    }

    pub fn needs_route_poison(&self) -> bool {
        self.poison_route
    }

    pub fn append_io_targets(&self, targets: &mut Vec<Token>) {
        for slot in self.slab.values() {
            let token = slot.token();
            if slot.core.is_send_inflight() {
                targets.push(token.with_kind(kind::SEND));
            }
            if slot.core.is_armed() {
                targets.push(token.with_kind(slot.core.recv_cancel_kind()));
            }
        }
    }

    pub fn fd_of(&self, idx: SlotIndex) -> Option<&Fd<'d>> {
        self.slab
            .get_index(idx.raw())
            .map(|(slot, _)| &slot.core.fd)
    }

    #[must_use]
    pub fn insert(
        &mut self,
        idx: SlotIndex,
        core: Core<'d>,
        config: &W::InitConfig,
        state: S,
    ) -> bool {
        let Some((wire, send)) = W::open(config, &self.runtime) else {
            return false;
        };
        self.slab
            .insert_at_with(idx.raw(), |key| {
                let token = Token::from_key(key);
                Slot::new(core, wire, send, token, state)
            })
            .is_some()
    }

    pub fn get(&self, idx: SlotIndex) -> Option<&Slot<'d, W, S>> {
        self.slab.get_index(idx.raw()).map(|(slot, _)| slot)
    }

    pub fn get_mut(&mut self, idx: SlotIndex) -> Option<&mut Slot<'d, W, S>> {
        self.slab.get_index_mut(idx.raw()).map(|(slot, _)| slot)
    }

    pub fn by_target(&self, target: Token) -> Option<(SlotIndex, &Slot<'d, W, S>)> {
        let parts = target.parts::<KeyTag<ID>>()?;
        self.slab
            .get_parts(parts.slab())
            .map(|slot| (SlotIndex::new(parts.index()), slot))
    }

    pub fn by_target_mut(&mut self, target: Token) -> Option<(SlotIndex, &mut Slot<'d, W, S>)> {
        let parts = target.parts::<KeyTag<ID>>()?;
        self.slab
            .get_parts_mut(parts.slab())
            .map(|slot| (SlotIndex::new(parts.index()), slot))
    }

    pub fn refresh_wake(&self, idx: SlotIndex) {
        let Some((slot, key)) = self.slab.get_index(idx.raw()) else {
            return;
        };
        slot.core.fd.ready_handle().set_target(Token::from_key(key));
    }

    pub fn arm_recv(&mut self, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>) -> bool {
        let (key, armed) = {
            let Some((slot, key)) = self.slab.get_index_mut(idx.raw()) else {
                return false;
            };
            let ud = Token::from_key(key);
            (key, Self::submit_recv(slot, ud, driver))
        };
        if !armed {
            self.queue_rearm(Token::from_key(key));
        }
        armed
    }

    fn submit_recv(
        slot: &mut Slot<'d, W, S>,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        if slot.core.is_armed() {
            return true;
        }
        let remaining = slot.core.discard_remaining();
        let (sqe, discard) = if Sqe::SUPPORTS_RECV_DISCARD && remaining > 0 {
            (
                unsafe { Sqe::recv_discard(&slot.core.fd, remaining, ud) },
                true,
            )
        } else {
            let buf_group = driver.buffer_group();
            (
                unsafe { Sqe::recv_multi(&slot.core.fd, buf_group, ud) },
                false,
            )
        };
        let armed = driver.push(sqe).is_ok();
        slot.core.armed(armed, discard);
        armed
    }

    pub fn flush_rearm(&mut self, driver: &mut DriverContext<'_, 'd>) {
        let n = self.recv_rearm_pending.len();
        for _ in 0..n {
            let Some(idx) = self.recv_rearm_pending.pop_front() else {
                break;
            };
            let epoch = std::mem::replace(
                unsafe { self.rearm_epoch.get_unchecked_mut(idx.raw() as usize) },
                Epoch::ZERO,
            );
            if epoch == Epoch::ZERO {
                continue;
            }
            let token = Token::new(ID, idx, epoch);
            let Some(parts) = token.parts::<KeyTag<ID>>() else {
                continue;
            };
            let Some(slot) = self.slab.get_parts_mut(parts.slab()) else {
                continue;
            };
            if !slot.core.needs_arm() {
                continue;
            }
            let armed = Self::submit_recv(slot, token, driver);
            if !armed {
                self.queue_rearm(token);
            }
        }
    }

    pub fn classify_send(
        &mut self,
        driver: &mut DriverContext<'_, 'd>,
        ud: Token,
        e: dope_core::io::SendEvent,
    ) -> SendOutcome {
        let Some(parts) = ud.parts::<KeyTag<ID>>() else {
            return SendOutcome::Drop;
        };
        let Some(slot) = self.slab.get_parts_mut(parts.slab()) else {
            return SendOutcome::Drop;
        };
        let idx = SlotIndex::new(parts.index());
        match e {
            SendEvent::Sent(n) => slot.send_sent(driver, n as usize, ud, idx),
            SendEvent::Failed(_) => slot.send_failed(idx),
        }
    }

    pub fn dispatch_recv<'a>(
        &mut self,
        ud: Token,
        more: bool,
        e: &'a dope_core::io::RecvEvent<'d>,
    ) -> DispatchRecv<W::Recv<'a>> {
        let Some(parts) = ud.parts::<KeyTag<ID>>() else {
            return DispatchRecv::Drop;
        };
        let runtime = &self.runtime;
        let Some(slot) = self.slab.get_parts_mut(parts.slab()) else {
            return DispatchRecv::Drop;
        };
        let idx = SlotIndex::new(parts.index());
        let decision = match e {
            RecvEvent::Data(buffer) => slot.recv_data(runtime, more, buffer.as_slice()),
            RecvEvent::Discarded { len } => slot.recv_discarded(*len),
            RecvEvent::Eof => slot.recv_eof(more),
            RecvEvent::Cancelled => slot.recv_cancelled(more),
            RecvEvent::Starved => slot.recv_starved(more),
            RecvEvent::Failed(_) => slot.recv_failed(more),
        };
        let needs_rearm = match &decision {
            RecvDecision::NoChunk { needs_rearm }
            | RecvDecision::Discarded { needs_rearm }
            | RecvDecision::Chunk { needs_rearm, .. } => *needs_rearm,
            _ => false,
        };
        if needs_rearm {
            self.queue_rearm(ud);
        }
        match decision {
            RecvDecision::Drop => DispatchRecv::Drop,
            RecvDecision::Close => DispatchRecv::Close(idx),
            RecvDecision::Discarded { .. } => DispatchRecv::Discarded(idx),
            RecvDecision::NoChunk { .. } => DispatchRecv::NoChunk(idx),
            RecvDecision::Chunk { chunk, .. } => DispatchRecv::Chunk(idx, chunk),
        }
    }

    pub fn dispatch_retained_recv(
        &mut self,
        ud: Token,
        more: bool,
        event: dope_core::io::RecvEvent<'d>,
    ) -> DispatchRecv<Option<ProvidedView<'d>>> {
        let dispatch = match self.dispatch_recv(ud, more, &event) {
            DispatchRecv::Drop => DispatchRecv::Drop,
            DispatchRecv::Close(idx) => DispatchRecv::Close(idx),
            DispatchRecv::NoChunk(idx) => DispatchRecv::NoChunk(idx),
            DispatchRecv::Discarded(idx) => DispatchRecv::Discarded(idx),
            DispatchRecv::Chunk(idx, chunk) => {
                let range = match &event {
                    RecvEvent::Data(lease) => lease.range_of(chunk.as_slice()),
                    _ => None,
                };
                DispatchRecv::Chunk(idx, range)
            }
        };
        match dispatch {
            DispatchRecv::Drop => DispatchRecv::Drop,
            DispatchRecv::Close(idx) => DispatchRecv::Close(idx),
            DispatchRecv::NoChunk(idx) => DispatchRecv::NoChunk(idx),
            DispatchRecv::Discarded(idx) => DispatchRecv::Discarded(idx),
            DispatchRecv::Chunk(idx, range) => {
                let view = match (event, range) {
                    (RecvEvent::Data(lease), Some((offset, len))) => {
                        lease.into_view(offset, len).ok()
                    }
                    _ => None,
                };
                DispatchRecv::Chunk(idx, view)
            }
        }
    }

    pub fn set_close_after(&mut self, idx: SlotIndex) {
        if let Some((slot, _)) = self.slab.get_index_mut(idx.raw()) {
            slot.core.set_close_after();
        }
    }

    pub fn try_close(&mut self, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>) {
        let Some((slot, target)) = self.slab.remove_index_with(idx.raw(), |slot, key| {
            if slot.core.is_send_inflight() {
                slot.core.begin_close();
                return None;
            }
            Some(
                slot.core
                    .is_armed()
                    .then(|| Token::from_key(key).with_kind(slot.core.recv_cancel_kind())),
            )
        }) else {
            return;
        };
        if let Some(target) = target {
            self.poison_route |= driver.quiesce(&[target]);
        }
        slot.close(driver);
    }
}

impl<'d, const ID: u8, T: Transport, W: Wire, S> Pool<'d, ID, T, W, S> {
    pub fn send_slot(&mut self, idx: SlotIndex) -> Option<(&mut Slot<'d, W, S>, Token)> {
        let (slot, key) = self.slab.get_index_mut(idx.raw())?;
        let ud = Token::from_key(key);
        if slot.core.is_closing() || slot.core.is_send_inflight() {
            return None;
        }
        Some((slot, ud))
    }

    pub fn append_outbound_targets(&mut self, targets: &mut Vec<Token>)
    where
        S: Outbound,
    {
        for slot in self.slab.values_mut() {
            let token = slot.token();
            let establish = slot.state.establish();
            if establish.is_connecting() {
                targets.push(token.with_kind(kind::CONNECT));
            } else if !establish.is_done() {
                targets.push(
                    Token::new(
                        ROUTE_FRAMEWORK,
                        SlotIndex::new(slot.core.fd.index()),
                        Epoch::ZERO,
                    )
                    .with_kind(kind::CREATE),
                );
            }
        }
    }

    pub fn submit_socket_with_state(
        &mut self,
        socket_params: (i32, i32, i32),
        config: &W::InitConfig,
        make_state: impl FnOnce(SlotIndex) -> S,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<SlotIndex> {
        let reservation = self.slab.vacant_entry()?;
        let (wire, send) = W::open(config, &self.runtime)?;
        let key = reservation.key();
        let idx = SlotIndex::new(key.index());
        let outbound_slot = self.reservation.slot(idx)?;
        let fd = unsafe { Fd::from_raw_slot(outbound_slot.fd(), driver.driver_ref()) };
        let (domain, socket_type, protocol) = socket_params;
        let ud = Token::from_key(key);
        let sqe = match Sqe::socket(domain, socket_type, protocol, &fd, ud) {
            Ok(sqe) => sqe,
            Err(_) => {
                drop(driver.guard(fd));
                return None;
            }
        };
        if driver.push(sqe).is_err() {
            drop(driver.guard(fd));
            return None;
        }
        let state = make_state(idx);
        let slot = Slot::<W, S>::new(Core::new(fd, T::KERNEL_DISCARD), wire, send, ud, state);
        reservation.insert(slot);
        self.refresh_wake(idx);
        Some(idx)
    }

    pub fn drive_socket_cqe<X>(
        &mut self,
        ud: Token,
        e: &dope_core::io::SocketEvent,
        driver: &mut DriverContext<'_, 'd>,
        prepare: impl FnOnce(&Slot<'d, W, S>) -> (X, Option<(Addr, T::StreamConfig)>),
    ) -> SocketStep<X>
    where
        S: Outbound,
    {
        let Some(parts) = ud.parts::<KeyTag<ID>>() else {
            return SocketStep::Failed { peeked: None };
        };
        let idx = SlotIndex::new(parts.index());
        let (peeked, submitted) = {
            let Some(slot) = self.slab.get_parts_mut(parts.slab()) else {
                return SocketStep::Failed { peeked: None };
            };
            let (peeked, prepared) = prepare(&*slot);
            let submitted = if let (SocketEvent::Created, Some((sock_addr, config))) = (e, prepared)
            {
                T::submit_stream_config(driver, config, &slot.core.fd);
                let (ptr, len) = slot.state.establish().begin(sock_addr);
                let submitted = driver
                    .push(Sqe::connect(&slot.core.fd, ptr, len, ud))
                    .is_ok();
                if !submitted {
                    slot.state.establish().abort();
                }
                submitted
            } else {
                false
            };
            (peeked, submitted)
        };
        if submitted {
            let _ = (idx, peeked);
            SocketStep::Connecting
        } else {
            if let Some(slot) = self.slab.remove_parts(parts.slab()) {
                slot.close(driver);
            }
            SocketStep::Failed {
                peeked: Some(peeked),
            }
        }
    }

    pub fn drive_connect_cqe<X>(
        &mut self,
        ud: Token,
        e: &dope_core::io::ConnectEvent,
        driver: &mut DriverContext<'_, 'd>,
        peek: impl FnOnce(&Slot<'d, W, S>) -> X,
    ) -> ConnectStep<X>
    where
        S: Outbound,
    {
        let Some(parts) = ud.parts::<KeyTag<ID>>() else {
            return ConnectStep::Drop { peeked: None };
        };
        let idx = SlotIndex::new(parts.index());
        let failed = matches!(e, ConnectEvent::Failed(_));
        let (peeked, armed) = {
            let Some(slot) = self.slab.get_parts_mut(parts.slab()) else {
                return ConnectStep::Drop { peeked: None };
            };
            if !slot.state.establish().is_connecting() {
                let peeked = (!slot.state.establish().is_done()).then(|| peek(&*slot));
                return ConnectStep::Drop { peeked };
            }
            let peeked = peek(&*slot);
            if failed {
                slot.state.establish().abort();
                (peeked, false)
            } else {
                slot.state.establish().finish();
                let armed = Self::submit_recv(slot, ud, driver);
                (peeked, armed)
            }
        };
        if failed {
            if let Some(slot) = self.slab.remove_parts(parts.slab()) {
                slot.close(driver);
            }
            let _ = idx;
            return ConnectStep::Failed { peeked };
        }
        if !armed {
            self.queue_rearm(ud);
        }
        ConnectStep::Connected { idx, peeked }
    }
}

pub enum SocketStep<X> {
    Connecting,
    Failed { peeked: Option<X> },
}

pub enum ConnectStep<X> {
    Connected { idx: SlotIndex, peeked: X },
    Failed { peeked: X },
    Drop { peeked: Option<X> },
}

pub enum DispatchRecv<C> {
    Drop,
    Close(SlotIndex),
    Chunk(SlotIndex, C),
    NoChunk(SlotIndex),
    Discarded(SlotIndex),
}

pub enum SendOutcome {
    Sent { idx: SlotIndex, n: usize },
    Close(SlotIndex),
    Drop,
}
