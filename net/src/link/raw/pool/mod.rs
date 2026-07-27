use std::io::{self, Error, ErrorKind};
use std::marker::PhantomData;

use self::rearm::Rearm;
use super::core::{Core, Outbound};
use super::event::{ConnectStep, DispatchRecv, SendOutcome, SocketStep};
use crate::Transport;
use crate::link::slot::{RecvDecision, Slot};
use crate::wire::{OpenReservation, RuntimeLimits, Wire};
use dope_core::backend::{RawSqe, Sqe};
use dope_core::driver::buffers::ProvidedBuffers;
use dope_core::driver::control::ContextControl;
use dope_core::driver::submission::Submission;
use dope_core::driver::submission::raw::Submission as _;
use dope_core::driver::token::kind::{CONNECT, CREATE, SEND};
use dope_core::driver::token::{
    Epoch, KeyTag, ROUTE_FRAMEWORK, SLOT_MASK, SlotIndex, Token, TokenSlab,
};
use dope_core::driver::{DriverContext, OutboundReservation};
use dope_core::io::fd::Fd;
use dope_core::io::provided::ProvidedView;
use dope_core::io::socket::addr::Addr;
use dope_core::io::{ConnectEvent, RecvEvent, SendEvent, SocketEvent};
use o3::buffer::ByteSpan;
use std::mem::replace;

mod rearm;

pub struct Pool<'d, const ID: u8, T: Transport, W: Wire, S> {
    slab: TokenSlab<Slot<'d, W, S>, KeyTag<ID>>,
    runtime: W::RuntimeContext,
    reservation: OutboundReservation,
    rearm: Rearm<ID>,
    poison_route: bool,
    _t: PhantomData<T>,
}

impl<'d, const ID: u8, T: Transport, W: Wire, S> Pool<'d, ID, T, W, S> {
    pub fn new(
        max_connections: usize,
        max_retained_recv_chunks: usize,
        reservation: OutboundReservation,
        wire_config: W::InitConfig,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        if max_connections > SLOT_MASK as usize + 1 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "dope: connection limit exceeds token slots",
            ));
        }
        let runtime = W::runtime_context(
            RuntimeLimits::new(
                max_connections,
                max_retained_recv_chunks,
                driver.buffer_len(),
            ),
            wire_config,
        )?;
        Ok(Self {
            slab: TokenSlab::with_capacity(max_connections),
            runtime,
            reservation,
            rearm: Rearm::with_capacity(max_connections),
            poison_route: false,
            _t: PhantomData,
        })
    }

    pub fn capacity(&self) -> usize {
        self.slab.capacity()
    }

    pub fn wire_runtime(&mut self) -> &mut W::RuntimeContext {
        &mut self.runtime
    }

    pub fn pending_recv_rearm(&self) -> bool {
        !self.rearm.pending.is_empty()
    }

    pub fn needs_route_poison(&self) -> bool {
        self.poison_route
    }

    pub fn append_io_targets(&self, targets: &mut Vec<Token>) {
        for slot in self.slab.values() {
            let token = slot.token();
            if slot.core.is_send_inflight() {
                targets.push(token.with_kind(SEND));
            }
            if slot.core.is_armed() {
                targets.push(token.with_kind(slot.core.recv_cancel_kind()));
            }
        }
    }

    pub fn fd_of(&self, idx: SlotIndex) -> Option<&Fd<'d>> {
        self.get(idx).map(|slot| &slot.core.fd)
    }

    #[must_use]
    pub fn insert(
        &mut self,
        idx: SlotIndex,
        core: Core<'d>,
        state: S,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        let Some(reservation) = self.slab.vacant_entry_at(idx.raw()) else {
            drop(driver.guard(core.into_fd()));
            return false;
        };
        let Some(open) = W::prepare_open(&mut self.runtime) else {
            drop(driver.guard(core.into_fd()));
            return false;
        };
        let (wire, send) = open.commit();
        let token = Token::from_key(reservation.key());
        reservation.insert(Slot::new(core, wire, send, token, state));
        true
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
            self.rearm.queue(Token::from_key(key));
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
            (RawSqe::recv_discard(&slot.core.fd, remaining, ud), true)
        } else {
            let buf_group = driver.buffer_group();
            (RawSqe::recv_multi(&slot.core.fd, buf_group, ud), false)
        };
        // SAFETY: the slot owns the registered fd through completion; receive
        // memory comes from the driver's provided-buffer ring.
        let armed = unsafe { driver.push_raw(sqe) }.is_ok();
        slot.core.armed(armed, discard);
        armed
    }

    pub fn flush_rearm(&mut self, driver: &mut DriverContext<'_, 'd>) {
        let count = self.rearm.pending.len();
        for _ in 0..count {
            let Some(idx) = self.rearm.pending.pop_front() else {
                break;
            };
            // SAFETY: every queued index was admitted by `Rearm::queue`.
            let epoch = replace(
                unsafe { self.rearm.epochs.get_unchecked_mut(idx.raw() as usize) },
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
            if !Self::submit_recv(slot, token, driver) {
                self.rearm.queue(token);
            }
        }
    }

    pub fn classify_send(
        &mut self,
        driver: &mut DriverContext<'_, 'd>,
        ud: Token,
        e: SendEvent,
    ) -> SendOutcome {
        let Some((idx, slot)) = self.by_target_mut(ud) else {
            return SendOutcome::Drop;
        };
        match e {
            SendEvent::Sent(n) => slot.send_sent(driver, n as usize, ud, idx),
            SendEvent::Failed(_) => slot.send_failed(idx),
        }
    }

    pub fn dispatch_recv<'a>(
        &mut self,
        ud: Token,
        more: bool,
        e: &'a RecvEvent<'d>,
    ) -> DispatchRecv<W::Recv<'a>> {
        let Some(parts) = ud.parts::<KeyTag<ID>>() else {
            return DispatchRecv::Drop;
        };
        let runtime = &mut self.runtime;
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
            self.rearm.queue(ud);
        }
        match decision {
            RecvDecision::Drop => DispatchRecv::Drop,
            RecvDecision::Close => DispatchRecv::Close(idx),
            RecvDecision::Discarded { .. } => DispatchRecv::Discarded(idx),
            RecvDecision::NoChunk { .. } => DispatchRecv::NoChunk(idx),
            RecvDecision::Chunk { chunk, .. } => DispatchRecv::Chunk(idx, chunk),
        }
    }

    fn map_dispatch<C, X>(dispatch: DispatchRecv<C>, map: impl FnOnce(C) -> X) -> DispatchRecv<X> {
        match dispatch {
            DispatchRecv::Drop => DispatchRecv::Drop,
            DispatchRecv::Close(idx) => DispatchRecv::Close(idx),
            DispatchRecv::Chunk(idx, chunk) => DispatchRecv::Chunk(idx, map(chunk)),
            DispatchRecv::NoChunk(idx) => DispatchRecv::NoChunk(idx),
            DispatchRecv::Discarded(idx) => DispatchRecv::Discarded(idx),
        }
    }

    pub fn dispatch_retained_recv(
        &mut self,
        ud: Token,
        more: bool,
        event: RecvEvent<'d>,
    ) -> DispatchRecv<Option<ProvidedView<'d>>> {
        let dispatch =
            Self::map_dispatch(self.dispatch_recv(ud, more, &event), |chunk| match &event {
                RecvEvent::Data(lease) => lease.range_of(chunk.as_slice()),
                _ => None,
            });
        Self::map_dispatch(dispatch, |range| match (event, range) {
            (RecvEvent::Data(lease), Some(range)) => lease.into_view(range).ok(),
            _ => None,
        })
    }

    pub fn set_close_after(&mut self, idx: SlotIndex) {
        if let Some(slot) = self.get_mut(idx) {
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
                targets.push(token.with_kind(CONNECT));
            } else if !establish.is_done() {
                targets.push(
                    Token::new(
                        ROUTE_FRAMEWORK,
                        SlotIndex::new(slot.core.fd.index()),
                        Epoch::ZERO,
                    )
                    .with_kind(CREATE),
                );
            }
        }
    }

    pub fn submit_socket_with_state(
        &mut self,
        socket_params: (i32, i32, i32),
        make_state: impl FnOnce(SlotIndex) -> S,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<SlotIndex> {
        let reservation = self.slab.vacant_entry()?;
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
        let Some(open) = W::prepare_open(&mut self.runtime) else {
            drop(driver.guard(fd));
            return None;
        };
        if driver.push(sqe).is_err() {
            drop(driver.guard(fd));
            return None;
        }
        let (wire, send) = open.commit();
        let state = make_state(idx);
        let slot = Slot::<W, S>::new(Core::new(fd, T::KERNEL_DISCARD), wire, send, ud, state);
        reservation.insert(slot);
        self.refresh_wake(idx);
        Some(idx)
    }

    pub fn drive_socket_cqe<X>(
        &mut self,
        ud: Token,
        e: &SocketEvent,
        driver: &mut DriverContext<'_, 'd>,
        prepare: impl FnOnce(&Slot<'d, W, S>) -> (X, Option<(Addr, T::StreamConfig)>),
    ) -> SocketStep<X>
    where
        S: Outbound,
    {
        let Some(parts) = ud.parts::<KeyTag<ID>>() else {
            return SocketStep::Failed { peeked: None };
        };
        let (peeked, submitted) = {
            let Some(slot) = self.slab.get_parts_mut(parts.slab()) else {
                return SocketStep::Failed { peeked: None };
            };
            let (peeked, prepared) = prepare(&*slot);
            let submitted = if let (SocketEvent::Created, Some((sock_addr, config))) = (e, prepared)
            {
                if T::submit_stream_tuning(driver, config, &slot.core.fd) {
                    let (ptr, len) = slot.state.establish().begin(sock_addr);
                    // SAFETY: `Establish` owns the address through completion or rollback.
                    let submitted =
                        unsafe { driver.push_raw(RawSqe::connect(&slot.core.fd, ptr, len, ud)) }
                            .is_ok();
                    if !submitted {
                        slot.state.establish().abort();
                    }
                    submitted
                } else {
                    false
                }
            } else {
                false
            };
            (peeked, submitted)
        };
        if submitted {
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
        e: &ConnectEvent,
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
            return ConnectStep::Failed { peeked };
        }
        if !armed {
            self.rearm.queue(ud);
        }
        ConnectStep::Connected { idx, peeked }
    }
}
