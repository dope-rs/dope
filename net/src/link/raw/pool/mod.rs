use std::io::{self, Error, ErrorKind};
use std::marker::PhantomData;
use std::mem::replace;

use self::deferred::{DeferredRecv, DeferredRecvs};
use self::rearm::Rearm;
use super::core::Core;
use super::event::{DispatchRecv, SendOutcome};
use crate::Transport;
use crate::link::slot::{RecvDecision, Slot};
use crate::wire::{OpenReservation, RecvCredit, RuntimeLimits, Wire};
use dope_core::backend::{RawSqe, RetainedSqe, Sqe, StableSqeSource};
use dope_core::driver::buffers::ProvidedBuffers;
use dope_core::driver::control::ContextControl;
use dope_core::driver::submission::Submission;
use dope_core::driver::token::kind::SEND;
use dope_core::driver::token::{KeyTag, SlotIndex, Token, TokenCapacity, TokenSlab};
use dope_core::driver::{DriverContext, OutboundReservation};
use dope_core::io::fd::Fd;
use dope_core::io::{RecvEvent, SendEvent};
mod deferred;
pub mod outbound;
pub mod prepare;
pub(in crate::link) mod rearm;

struct RecvSubmission<'a, 'd> {
    fd: &'a Fd<'d>,
    remaining: Option<u64>,
    buf_group: u16,
    ud: Token,
}

// SAFETY: Pool retains every armed slot and its fixed fd through completion;
// receive storage is static scratch or the driver's retained provided ring.
unsafe impl StableSqeSource for RecvSubmission<'_, '_> {
    fn into_raw(self) -> RawSqe {
        match self.remaining {
            Some(remaining) => RawSqe::recv_discard(self.fd, remaining, self.ud),
            None => RawSqe::recv_multi(self.fd, self.buf_group, self.ud),
        }
    }
}

pub struct Pool<'d, const ID: u8, T: Transport, W: Wire, S> {
    slab: TokenSlab<Slot<'d, W, S>, KeyTag<ID>>,
    runtime: W::RuntimeContext<'d>,
    reservation: OutboundReservation<'d>,
    rearm: Rearm<ID>,
    deferred_recv: DeferredRecvs<'d>,
    recv_credit: bool,
    poison_route: bool,
    _t: PhantomData<T>,
}

impl<'d, const ID: u8, T: Transport, W: Wire, S> Pool<'d, ID, T, W, S> {
    pub fn new(
        max_connections: usize,
        max_retained_recv_chunks: usize,
        reservation: OutboundReservation<'d>,
        wire_config: W::InitConfig<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        let capacity = TokenCapacity::new(max_connections).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "dope: connection limit exceeds token slots",
            )
        })?;
        Self::prepare(capacity, max_retained_recv_chunks, wire_config, driver)
            .map(|prepared| prepared.bind(reservation))
    }

    #[doc(hidden)]
    pub fn prepare(
        capacity: TokenCapacity,
        max_retained_recv_chunks: usize,
        wire_config: W::InitConfig<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<prepare::PreparedPool<'d, ID, T, W, S>> {
        Self::prepare_with_recv_credit(
            capacity,
            max_retained_recv_chunks,
            false,
            wire_config,
            driver,
        )
    }

    #[doc(hidden)]
    pub fn prepare_with_recv_credit(
        capacity: TokenCapacity,
        max_retained_recv_chunks: usize,
        recv_credit: bool,
        wire_config: W::InitConfig<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<prepare::PreparedPool<'d, ID, T, W, S>> {
        let max_connections = capacity.get();
        let recv_credit = recv_credit && W::RECV_CREDIT;
        let deferred_recv_slots = if recv_credit {
            driver
                .buffer_count()
                .checked_add(max_connections)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        "dope: deferred receive capacity overflow",
                    )
                })?
        } else {
            0
        };
        let slab = TokenSlab::with_capacity(capacity);
        let limits = RuntimeLimits::new(
            max_connections,
            max_retained_recv_chunks,
            driver.buffer_len(),
        );
        let limits = if recv_credit {
            limits.with_recv_credit()
        } else {
            limits
        };
        let runtime = W::runtime_context(limits, wire_config)?;
        Ok(prepare::PreparedPool(Self {
            slab,
            runtime,
            reservation: OutboundReservation::empty(),
            rearm: Rearm::with_capacity(max_connections),
            deferred_recv: DeferredRecvs::with_capacity(
                if recv_credit { max_connections } else { 0 },
                deferred_recv_slots,
            ),
            recv_credit,
            poison_route: false,
            _t: PhantomData,
        }))
    }

    pub fn capacity(&self) -> TokenCapacity {
        self.slab.capacity()
    }

    pub fn wire_runtime(&mut self) -> &mut W::RuntimeContext<'d> {
        &mut self.runtime
    }

    pub fn pending_recv_rearm(&self) -> bool {
        !self.rearm.is_empty()
    }

    pub fn needs_route_poison(&self) -> bool {
        self.poison_route
    }

    #[doc(hidden)]
    pub fn take_outbound_reservation(&mut self) -> OutboundReservation<'d> {
        replace(&mut self.reservation, OutboundReservation::empty())
    }

    pub fn for_each_io_target(&self, mut visit: impl FnMut(Token)) {
        for slot in self.slab.values() {
            let token = slot.token();
            if slot.core.is_send_inflight() {
                visit(token.with_kind(SEND));
            }
            if slot.core.is_armed() {
                visit(token.with_kind(slot.core.recv_cancel_kind()));
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
        let Some(token) = self.rearm.bind(reservation.token()) else {
            drop(driver.guard(core.into_fd()));
            return false;
        };
        let Some(open) = W::prepare_open(&mut self.runtime) else {
            drop(driver.guard(core.into_fd()));
            return false;
        };
        let (wire, send) = open.commit();
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
        self.slab.get_parts(parts).map(|slot| (parts.slot(), slot))
    }

    pub fn by_target_mut(&mut self, target: Token) -> Option<(SlotIndex, &mut Slot<'d, W, S>)> {
        let parts = target.parts::<KeyTag<ID>>()?;
        self.slab
            .get_parts_mut(parts)
            .map(|slot| (parts.slot(), slot))
    }

    pub fn refresh_wake(&self, idx: SlotIndex) {
        let Some((slot, key)) = self.slab.get_index(idx.raw()) else {
            return;
        };
        slot.core.fd.ready_handle().set_target(Token::from_key(key));
    }

    pub fn arm_recv(&mut self, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>) -> bool {
        let (token, armed) = {
            let Some((slot, _)) = self.slab.get_index_mut(idx.raw()) else {
                return false;
            };
            let token = slot.rearm_token();
            (token, Self::submit_recv(slot, token.token(), driver))
        };
        if !armed {
            self.rearm.queue(token);
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
        if slot.core.recv_paused() {
            return false;
        }
        let remaining = slot.core.discard_remaining();
        let (remaining, discard) = if Sqe::SUPPORTS_RECV_DISCARD && remaining > 0 {
            (Some(remaining), true)
        } else {
            (None, false)
        };
        let source = RecvSubmission {
            fd: &slot.core.fd,
            remaining,
            buf_group: driver.buffer_group(),
            ud,
        };
        let armed = driver
            .push_retained(RetainedSqe::from_stable(source))
            .is_ok();
        slot.core.armed(armed, discard);
        armed
    }

    pub fn flush_rearm(&mut self, driver: &mut DriverContext<'_, 'd>) {
        let count = self.rearm.len();
        for _ in 0..count {
            let Some(token) = self.rearm.pop_front() else {
                continue;
            };
            let Some(parts) = token.parts::<KeyTag<ID>>() else {
                continue;
            };
            let Some(slot) = self.slab.get_parts_mut(parts) else {
                continue;
            };
            if slot.core.needs_recv_cancel() {
                let kind = slot.core.recv_cancel_kind();
                if Core::push_retry(driver, || Sqe::cancel(token, kind)) {
                    slot.core.recv_cancel_submitted();
                } else {
                    self.rearm.queue(slot.rearm_token());
                }
                continue;
            }
            if !slot.core.needs_arm() {
                continue;
            }
            if !Self::submit_recv(slot, token, driver) {
                self.rearm.queue(slot.rearm_token());
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
            SendEvent::Sent(n) => slot.send_sent(driver, n, ud, idx),
            SendEvent::Failed(_) => slot.send_failed(idx),
        }
    }

    fn finish_recv<C>(
        rearm: &mut Rearm<ID>,
        idx: SlotIndex,
        slot: &Slot<'d, W, S>,
        decision: RecvDecision<C>,
    ) -> DispatchRecv<C> {
        let needs_rearm = match &decision {
            RecvDecision::NoChunk { needs_rearm }
            | RecvDecision::Discarded { needs_rearm }
            | RecvDecision::Chunk { needs_rearm, .. } => *needs_rearm,
            _ => false,
        };
        if needs_rearm {
            rearm.queue(slot.rearm_token());
        }
        match decision {
            RecvDecision::Drop => DispatchRecv::Drop,
            RecvDecision::Close => DispatchRecv::Close(idx),
            RecvDecision::Discarded { .. } => DispatchRecv::Discarded(idx),
            RecvDecision::NoChunk { .. } => DispatchRecv::NoChunk(idx),
            RecvDecision::Chunk { chunk, .. } => DispatchRecv::Chunk(idx, chunk),
        }
    }

    pub fn dispatch_recv<'a>(
        &mut self,
        ud: Token,
        more: bool,
        e: &'a mut RecvEvent<'d>,
    ) -> DispatchRecv<W::RecvBatch<'a>> {
        let Some(parts) = ud.parts::<KeyTag<ID>>() else {
            return DispatchRecv::Drop;
        };
        let runtime = &mut self.runtime;
        let Some(slot) = self.slab.get_parts_mut(parts) else {
            return DispatchRecv::Drop;
        };
        let idx = parts.slot();
        let decision = match e {
            RecvEvent::Data(buffer) => slot.recv_data(runtime, more, buffer.as_mut_slice()),
            RecvEvent::Discarded { len } => slot.recv_discarded(*len),
            RecvEvent::Eof => slot.recv_eof(more),
            RecvEvent::Cancelled => slot.recv_cancelled(more),
            RecvEvent::Starved => slot.recv_starved(more),
            RecvEvent::Failed(_) => slot.recv_failed(more),
        };
        Self::finish_recv(&mut self.rearm, idx, slot, decision)
    }

    pub fn dispatch_retained_recv(
        &mut self,
        ud: Token,
        more: bool,
        event: RecvEvent<'d>,
    ) -> DispatchRecv<W::RetainedRecv<'d>> {
        let Some(parts) = ud.parts::<KeyTag<ID>>() else {
            return DispatchRecv::Drop;
        };
        let runtime = &mut self.runtime;
        let Some(slot) = self.slab.get_parts_mut(parts) else {
            return DispatchRecv::Drop;
        };
        let idx = parts.slot();
        if W::RECV_CREDIT && self.recv_credit && slot.core.recv_paused() {
            return match self.deferred_recv.push(idx, ud, more, event) {
                Ok(()) => DispatchRecv::Drop,
                Err(_) => DispatchRecv::Close(idx),
            };
        }
        let mut decision = match event {
            RecvEvent::Data(buffer) => slot.recv_retained_data(runtime, more, buffer),
            RecvEvent::Discarded { len } => slot.recv_discarded(len),
            RecvEvent::Eof => slot.recv_eof(more),
            RecvEvent::Cancelled => slot.recv_cancelled(more),
            RecvEvent::Starved => slot.recv_starved(more),
            RecvEvent::Failed(_) => slot.recv_failed(more),
        };
        if W::RECV_CREDIT
            && self.recv_credit
            && let RecvDecision::Chunk { chunk, .. } = &mut decision
        {
            let credit = RecvCredit::new(slot.driver(), slot.ready_key(), slot.token());
            if W::bind_recv_credit(chunk, credit).is_ok() {
                slot.core.pause_recv();
                if slot.core.needs_recv_cancel() {
                    self.rearm.queue(slot.rearm_token());
                }
            }
        }
        Self::finish_recv(&mut self.rearm, idx, slot, decision)
    }

    /// Resumes a connection whose retained receive credit was released.
    /// Returns `true` only for a paused, still-live connection.
    #[doc(hidden)]
    pub fn resume_recv(&mut self, target: Token) -> bool {
        let rearm = {
            let Some((_, slot)) = self.by_target_mut(target) else {
                return false;
            };
            if !slot
                .driver()
                .take_recv_credit(slot.ready_key(), slot.token())
                || slot.core.is_closing()
                || !slot.core.recv_paused()
            {
                return false;
            }
            slot.core.resume_recv().then(|| slot.rearm_token())
        };
        if let Some(rearm) = rearm {
            self.rearm.queue(rearm);
        }
        true
    }

    /// Pops the next completion deferred for a resumed connection.
    #[doc(hidden)]
    pub fn pop_resumed_recv(&mut self, target: Token) -> Option<(Token, bool, RecvEvent<'d>)> {
        let (idx, slot) = self.by_target(target)?;
        if slot.core.recv_paused() {
            return None;
        }
        let DeferredRecv { token, more, event } = self.deferred_recv.pop(idx)?;
        Some((token, more, event))
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
        self.deferred_recv.clear(idx);
        slot.core.fd.ready_handle().set_target(slot.token());
        if let Some(target) = target {
            self.poison_route |= driver.quiesce(&[target]);
        }
        slot.close(driver);
    }
}
