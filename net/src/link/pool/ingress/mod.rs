use std::{marker, process};

use dope_core::{
    driver::{self, ops, retained, route, schedule},
    io::event::{receiving, send},
};

use crate::{
    link::{
        self, event,
        pool::{self, input, pending},
        slot::{self, reception},
    },
    wire::{self, batch, receive},
};

pub mod acceptance;
pub(super) mod recvs;

pub struct Ingress<
    'a,
    'd,
    const ID: u8,
    T: crate::Transport,
    W: wire::Wire,
    S,
    M,
    B,
    const IOV: usize,
> {
    pool: &'a mut pool::Connections<'d, ID, T, W, S, M, B, IOV>,
}

pub struct Data<'a, 'd, const ID: u8, W: wire::Wire, M> {
    prepared: reception::Prepared<'a, 'd, ID, W>,
    rearm: &'a mut link::Rearm<'d, ID>,
    key: pool::Key<'d, ID>,
    input: marker::PhantomData<M>,
}

struct Park<'a, 'd, const ID: u8, W: wire::Wire> {
    deferred: &'a mut recvs::Recvs<'d>,
    rearm: &'a mut link::Rearm<'d, ID>,
    blocked: reception::Blocked<'a, 'd, ID, W>,
    completion: receiving::DataCompletion<'d>,
}

enum Credit<'d, const ID: u8> {
    Absent,
    Open,
    Closing(pool::Key<'d, ID>),
}

impl<'a, 'd, const ID: u8, W: wire::Wire> Park<'a, 'd, ID, W> {
    fn park(self) -> Result<(), pool::Key<'d, ID>> {
        let blocked = self.blocked;
        let key = blocked.rearm;
        let Some(registration) = receive::Wait::register(blocked.block.0, blocked.credit) else {
            return Err(key);
        };
        let completion = self.completion.into_completion();
        if self.deferred.push(key.lane(), completion).is_err() {
            return Err(key);
        }
        receive::Registration::commit(registration);
        blocked.recv.block(blocked.more);
        if blocked.recv.needs_cancel() {
            self.rearm.queue(blocked.rearm);
        }
        Ok(())
    }
}

impl<'a, 'd, const ID: u8, W: wire::Wire, M> Data<'a, 'd, ID, W, M> {
    pub(super) fn finish<C>(
        rearm: &mut link::Rearm<'d, ID>,
        key: pool::Key<'d, ID>,
        decision: slot::Decision<C>,
    ) -> event::DispatchRecv<'d, ID, C> {
        let needs_rearm = match &decision {
            slot::Decision::Overrun { needs_rearm }
            | slot::Decision::NoChunk { needs_rearm }
            | slot::Decision::Discarded { needs_rearm }
            | slot::Decision::Chunk { needs_rearm, .. } => *needs_rearm,
            slot::Decision::Drop | slot::Decision::Close => false,
        };
        if needs_rearm {
            rearm.queue(key);
        }
        match decision {
            slot::Decision::Drop => event::DispatchRecv::Drop,
            slot::Decision::Close => event::DispatchRecv::Close(key),
            slot::Decision::Overrun { .. } => event::DispatchRecv::Overrun(key),
            slot::Decision::Discarded { .. } => event::DispatchRecv::Discarded(key),
            slot::Decision::NoChunk { .. } => event::DispatchRecv::NoChunk(key),
            slot::Decision::Chunk { chunk, .. } => event::DispatchRecv::Chunk(key, chunk),
        }
    }
}

impl<'a, 'd, const ID: u8, W: wire::Wire> Data<'a, 'd, ID, W, input::Borrowed> {
    pub fn dispatch<'bytes>(
        self,
        completion: &'bytes mut receiving::DataCompletion<'d>,
        capacity: &batch::Capacity<W>,
    ) -> event::DispatchRecv<'d, ID, W::RecvBatch<'bytes>> {
        let Self {
            prepared,
            rearm,
            key,
            input: _,
        } = self;
        let decision = prepared.data(completion.bytes_mut(), capacity);
        Self::finish(rearm, key, decision)
    }
}

impl<'a, 'd, const ID: u8, W: wire::Wire> Data<'a, 'd, ID, W, input::Retained> {
    pub fn dispatch_retained(
        mut self,
        completion: receiving::DataCompletion<'d>,
    ) -> event::DispatchRecv<'d, ID, W::RetainedRecv<'d>> {
        let mut decision = self.prepared.retained(completion.into_buffer());
        if W::RECV_CREDIT
            && let slot::Decision::Chunk { chunk, .. } = &mut decision
            && self.prepared.bind_recv_credit(chunk)
        {
            self.rearm.queue(self.key);
        }
        let Self {
            prepared: _,
            rearm,
            key,
            input: _,
        } = self;
        Self::finish(rearm, key, decision)
    }
}

impl<'a, 'd, const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize>
    Ingress<'a, 'd, ID, T, W, S, M, B, IOV>
where
    M: input::Mode<W>,
{
    pub(super) fn new(pool: &'a mut pool::Connections<'d, ID, T, W, S, M, B, IOV>) -> Self {
        Self { pool }
    }

    pub fn arm(
        &mut self,
        key: pool::Key<'d, ID>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> bool {
        let (token, armed) = {
            let pool::Prepared { flights, slab, .. } = &mut self.pool.prepared;
            let Some(slot) = slab.entries_mut().at_parts(key.parts()) else {
                return false;
            };
            let token = slot.key();
            (
                token,
                pool::Connections::<ID, T, W, S, M, B, IOV>::submit_recv(
                    slot,
                    flights,
                    token.target(),
                    driver,
                ),
            )
        };
        if !armed {
            self.pool.prepared.scheduling.rearm.queue(token);
        }
        armed
    }

    pub fn flush(
        &mut self,
        work: schedule::Maintenance<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let count = self.pool.prepared.scheduling.rearm.len();
        for _ in 0..count {
            if !work.take() {
                break;
            }
            let keys = self.pool.keys;
            let pool::Prepared {
                flights,
                slab,
                scheduling,
                ..
            } = &mut self.pool.prepared;
            let Some(key) = scheduling.rearm.pop_front(keys) else {
                continue;
            };
            let Some(slot) = slab.entries_mut().at_parts(key.parts()) else {
                continue;
            };
            if slot.engine.recv.needs_cancel() {
                let target = key.target().operation(slot.engine.recv.cancel_kind());
                let engine = &mut slot.engine;
                let submitted =
                    engine
                        .recv
                        .cancel_flight(&mut engine.flights)
                        .is_some_and(|flight| {
                            ops::Submit::cancel(driver.driver(), flight, target).is_ok()
                        });
                if submitted {
                    slot.engine.recv.cancel_submitted();
                } else {
                    scheduling.rearm.queue(slot.key());
                }
                continue;
            }
            if !slot
                .engine
                .recv
                .needs_arm(slot.engine.lifecycle.is_closing())
            {
                continue;
            }
            if !pool::Connections::<ID, T, W, S, M, B, IOV>::submit_recv(
                slot,
                flights,
                key.target(),
                driver,
            ) {
                scheduling.rearm.queue(slot.key());
            }
        }
    }

    pub fn reserve_data(
        self,
        completion: receiving::DataCompletion<'d>,
    ) -> event::DataReservation<'d, ID, Data<'a, 'd, ID, W, M::Kind>, receiving::DataCompletion<'d>>
    {
        let pool = self.pool;
        let target = completion.token();
        let more = completion.more();
        let Some(key) = pool.keys.parse(target) else {
            return event::DataReservation::Drop;
        };
        let index = key.lane();
        let storage = &mut pool.prepared;
        let deferred = &mut storage.deferred_recv;
        let rearm = &mut storage.scheduling.rearm;
        let Some(slot) = storage.slab.entries_mut().at_parts(key.parts()) else {
            return event::DataReservation::Drop;
        };
        if slot.engine.lifecycle.is_closing() {
            return if slot.receiving().settle_closing_data(more) {
                event::DataReservation::Parked(event::ParkRecv::Close(key))
            } else {
                event::DataReservation::Drop
            };
        }
        if M::DEFERS && slot.engine.recv.is_paused() {
            return match deferred.push(index, completion.into_completion()) {
                Ok(()) => event::DataReservation::Parked(event::ParkRecv::Deferred),
                Err(_) => event::DataReservation::Parked(event::ParkRecv::Close(key)),
            };
        }
        let prepared = match slot.receiving().reserve(&mut storage.runtime, more) {
            reception::Reservation::Drop => {
                return event::DataReservation::Drop;
            }
            reception::Reservation::Blocked(blocked) => {
                let parked = Park {
                    deferred,
                    rearm,
                    blocked,
                    completion,
                }
                .park();
                return match parked {
                    Ok(()) => event::DataReservation::Parked(event::ParkRecv::Deferred),
                    Err(key) => event::DataReservation::Parked(event::ParkRecv::Close(key)),
                };
            }
            reception::Reservation::Ready(prepared) => prepared,
        };
        event::DataReservation::Ready {
            prepared: Data {
                prepared,
                rearm,
                key,
                input: marker::PhantomData,
            },
            completion,
        }
    }

    pub fn dispatch_control<C>(
        &mut self,
        completion: receiving::ControlCompletion,
    ) -> event::ControlDispatch<'d, ID, C> {
        let target = completion.token();
        let more = completion.more();
        let Some(key) = self.pool.keys.parse(target) else {
            return event::ControlDispatch::Ready(event::DispatchRecv::Drop);
        };
        let Some(slot) = self.pool.prepared.slab.entries_mut().at_parts(key.parts()) else {
            return event::ControlDispatch::Ready(event::DispatchRecv::Drop);
        };
        if slot.engine.recv.is_paused() && !slot.engine.lifecycle.is_closing() {
            return match self
                .pool
                .prepared
                .deferred_recv
                .push(key.lane(), completion.into_completion())
            {
                Ok(()) => event::ControlDispatch::Parked(event::ParkRecv::Deferred),
                Err(_) => event::ControlDispatch::Parked(event::ParkRecv::Close(key)),
            };
        }
        let decision = match completion.event() {
            receiving::Control::Eof => slot.receiving().eof(more),
            receiving::Control::Cancelled => slot.receiving().cancelled(more),
            receiving::Control::BufferExhausted => {
                let decision: slot::Decision<C> = slot.receiving().buffer_exhausted(more);
                let needs_rearm = matches!(decision, slot::Decision::NoChunk { needs_rearm: true });
                if needs_rearm {
                    let target = key.target().dispatch();
                    let armed = slot.io().ready_handle().arm_recv_buffer(target);
                    if !armed {
                        process::abort();
                    }
                }
                let dispatch = match decision {
                    slot::Decision::NoChunk { .. } => event::DispatchRecv::NoChunk(key),
                    slot::Decision::Drop => event::DispatchRecv::Drop,
                    slot::Decision::Close => event::DispatchRecv::Close(key),
                    _ => process::abort(),
                };
                return event::ControlDispatch::Ready(dispatch);
            }
            receiving::Control::Starved => slot.receiving().starved(more),
            receiving::Control::Failed(_) => slot.receiving().failed(more),
        };
        event::ControlDispatch::Ready(Data::<ID, W, M>::finish(
            &mut self.pool.prepared.scheduling.rearm,
            key,
            decision,
        ))
    }

    pub fn classify_send(
        &mut self,
        driver: &mut retained::Context<'_, '_, 'd>,
        completion: send::Completion,
    ) -> event::SendOutcome<'d, ID> {
        use dope_core::io::SendEvent;
        use wire::send;
        let (token, event) = completion.into_parts();
        let Some((flights, key, slot)) = self.pool.by_target_submit_mut(token) else {
            use crate::link::event::SendOutcome;
            return SendOutcome::Drop;
        };
        let (outcome, availability) = match event {
            SendEvent::Sent(bytes) => slot.sending().sent(flights, driver, bytes, key),
            SendEvent::Failed(_) => (slot.sending().failed(key), send::Availability::Unchanged),
        };
        if <W::Receive as receive::Strategy<W>>::BACKPRESSURE && availability.is_released() {
            <W::Receive as receive::Strategy<W>>::send_released(&mut self.pool.prepared.runtime);
        }
        outcome
    }

    fn settle_credit(&mut self, target: route::Token) -> Credit<'d, ID> {
        let (wake, rearm, closing, key) = {
            let Some((key, slot)) = self.pool.by_target_mut(target) else {
                return Credit::Absent;
            };
            let connection_target = key.target().dispatch();
            let Some(wake) = slot.io().ready_handle().take_recv_credit(connection_target) else {
                return Credit::Absent;
            };
            let closing = slot.engine.lifecycle.is_closing();
            let rearm = slot.engine.recv.resume(closing).then_some(key);
            (wake, rearm, closing, key)
        };
        match wake {
            driver::RecvCreditWake::ResourceReturned => {
                <W::Receive as receive::Strategy<W>>::recv_released(
                    &mut self.pool.prepared.runtime,
                );
            }
            driver::RecvCreditWake::WaiterRetry => {}
        }
        if let Some(rearm) = rearm {
            self.pool.prepared.scheduling.rearm.queue(rearm);
        }
        if closing {
            Credit::Closing(key)
        } else {
            Credit::Open
        }
    }

    #[doc(hidden)]
    pub fn resume(&mut self, target: route::Token) -> bool {
        if self.resume_buffer(target) {
            return true;
        }
        match self.settle_credit(target) {
            Credit::Absent => false,
            Credit::Open => true,
            Credit::Closing(key) => {
                if let Some(handle) = pending::Pending::of(self.pool).at(key) {
                    handle.mark(pending::Action::Close);
                }
                true
            }
        }
    }

    fn resume_buffer(&mut self, target: route::Token) -> bool {
        let Some((key, slot)) = self.pool.by_target_mut(target) else {
            return false;
        };
        let connection_target = key.target().dispatch();
        let Some(credit) = slot.io().ready_handle().take_recv_buffer(connection_target) else {
            return false;
        };
        if slot
            .engine
            .recv
            .needs_arm(slot.engine.lifecycle.is_closing())
        {
            self.pool.prepared.scheduling.rearm.queue(key);
            credit.consume();
        }
        true
    }

    #[doc(hidden)]
    pub fn pop_resumed(&mut self, target: route::Token) -> Option<receiving::Completion<'d>> {
        let (key, slot) = self.pool.by_target(target)?;
        if slot.engine.recv.is_paused() {
            return None;
        }
        self.pool.prepared.deferred_recv.pop(key.lane())
    }

    pub fn has_resumed(&self, target: route::Token) -> bool {
        let Some((key, slot)) = self.pool.by_target(target) else {
            return false;
        };
        !slot.engine.recv.is_paused() && self.pool.prepared.deferred_recv.has(key.lane())
    }

    pub fn set_close_after(&mut self, key: pool::Key<'d, ID>) {
        if let Some(slot) = self.pool.get_mut(key) {
            slot.engine.lifecycle.set_close_after();
        }
    }
}
