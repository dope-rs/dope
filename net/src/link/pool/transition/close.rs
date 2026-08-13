//! Closing transitions for occupied connection slots.

use std::mem;

use dope_core::{
    driver::{
        self, flight,
        ops::Submit as _,
        retained,
        route::{self, kind},
        schedule,
    },
    io::event::receiving,
};

use crate::{
    link::{
        egress, pool,
        pool::{ingress, pending},
        setup,
        slot::{self, types},
    },
    wire::{self, receive},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
#[repr(u8)]
pub enum Decision {
    Ready,
    Runnable,
    Waiting,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Phase {
    Prepare,
    Release,
    Retire,
}

#[doc(hidden)]
pub trait Close<'d, const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize> {
    fn close(
        &mut self,
        key: pool::Key<'d, ID>,
        work: schedule::Maintenance<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
        owner: impl FnMut(
            Phase,
            &flight::Slots<'d, route::KeyTag<ID>>,
            &mut types::Connection<'d, ID, W, S>,
            &mut retained::Context<'_, '_, 'd>,
        ) -> Decision,
    );
}

impl<'d, const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize>
    Close<'d, ID, T, W, S, M, B, IOV> for pool::Connections<'d, ID, T, W, S, M, B, IOV>
{
    fn close(
        &mut self,
        key: pool::Key<'d, ID>,
        work: schedule::Maintenance<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
        mut owner: impl FnMut(
            Phase,
            &flight::Slots<'d, route::KeyTag<ID>>,
            &mut types::Connection<'d, ID, W, S>,
            &mut retained::Context<'_, '_, 'd>,
        ) -> Decision,
    ) {
        let runnable = 'close: {
            let Some(mut entry) = self.prepared.slab.occupied_entry_parts(key.parts()) else {
                break 'close false;
            };
            let index = key.lane();
            let connection = key.target();
            let target = route::Token::from(connection);
            let recv_target = connection.dispatch();
            entry.get_mut().engine.lifecycle.begin_close();
            {
                let slot = entry.get_mut();
                slot.io().ready_handle().cancel_recv_buffer(recv_target);
            }
            <W::Receive as receive::Strategy<W>>::cancel(
                &mut self.prepared.runtime,
                wire::RecvCreditId::new(recv_target),
            );
            let cancellation = entry.get_mut().engine.establish.cancel(driver.driver());
            match cancellation {
                setup::Cancellation::Blocked => break 'close true,
                setup::Cancellation::Pending => break 'close false,
                setup::Cancellation::Idle => {}
            }
            let resumed = {
                let slot = entry.get_mut();
                slot.io()
                    .ready_handle()
                    .take_recv_credit(recv_target)
                    .map(|wake| {
                        let rearm = slot.engine.recv.resume(true).then_some(key);
                        (wake, rearm)
                    })
            };
            if let Some((wake, rearm)) = resumed {
                match wake {
                    driver::RecvCreditWake::ResourceReturned => {
                        <W::Receive as receive::Strategy<W>>::recv_released(
                            &mut self.prepared.runtime,
                        );
                    }
                    driver::RecvCreditWake::WaiterRetry => {}
                }
                if let Some(rearm) = rearm {
                    self.prepared.scheduling.rearm.queue(rearm);
                }
                break 'close true;
            }
            if let Some(completion) = self.prepared.deferred_recv.pop(index) {
                match completion.classify() {
                    receiving::Classification::Data(data) => {
                        let completion_target = data.token();
                        let more = data.more();
                        drop(data.into_buffer());
                        if completion_target.same_target(target) {
                            entry.get_mut().receiving().settle_closing_data(more);
                        }
                    }
                    receiving::Classification::Control(control) => {
                        let completion_target = control.token();
                        let more = control.more();
                        if completion_target.same_target(target) {
                            let slot = entry.get_mut();
                            let token = key;
                            let decision: slot::Decision<()> = match control.event() {
                                receiving::Control::Eof => slot.receiving().eof(more),
                                receiving::Control::Cancelled => slot.receiving().cancelled(more),
                                receiving::Control::BufferExhausted => {
                                    slot.receiving().buffer_exhausted(more)
                                }
                                receiving::Control::Starved => slot.receiving().starved(more),
                                receiving::Control::Failed(_) => slot.receiving().failed(more),
                            };
                            let _ = ingress::Data::<ID, W, M>::finish(
                                &mut self.prepared.scheduling.rearm,
                                token,
                                decision,
                            );
                        }
                    }
                }
                break 'close true;
            }
            let cancel = {
                let slot = entry.get_mut();
                if slot.engine.sending.is_inflight() {
                    if slot.engine.lifecycle.is_aborted()
                        && !slot.engine.lifecycle.send_cancel_requested()
                    {
                        let send = key.target().operation(kind::SEND);
                        let engine = &mut slot.engine;
                        let Some(flight) = engine.sending.cancel_flight(&mut engine.flights) else {
                            break 'close true;
                        };
                        if driver.driver().cancel(flight, send).is_err() {
                            break 'close true;
                        }
                        slot.engine.lifecycle.mark_send_cancel_requested();
                    }
                    break 'close false;
                }
                if slot.io().ready_handle().has_recv_credit(recv_target) {
                    break 'close false;
                }
                slot.engine.recv.pause();
                slot.engine
                    .recv
                    .needs_cancel()
                    .then(|| key.target().operation(slot.engine.recv.cancel_kind()))
            };
            if let Some(cancel) = cancel {
                let slot = entry.get_mut();
                let engine = &mut slot.engine;
                let Some(flight) = engine.recv.cancel_flight(&mut engine.flights) else {
                    break 'close true;
                };
                if driver.driver().cancel(flight, cancel).is_err() {
                    break 'close true;
                }
                entry.get_mut().engine.recv.cancel_submitted();
                break 'close false;
            }
            let prepare = owner(
                Phase::Prepare,
                &self.prepared.flights,
                &mut entry.get_mut().connection,
                driver,
            );
            match prepare {
                Decision::Runnable => break 'close true,
                Decision::Waiting => break 'close false,
                Decision::Ready => {}
            }
            let released = entry.get().engine.lifecycle.owner_released();
            if !released {
                let release = owner(
                    Phase::Release,
                    &self.prepared.flights,
                    &mut entry.get_mut().connection,
                    driver,
                );
                match release {
                    Decision::Runnable => break 'close true,
                    Decision::Waiting => break 'close false,
                    Decision::Ready => {}
                }
                entry.get_mut().engine.lifecycle.release_owner();
            }
            match self
                .prepared
                .egress
                .clear_step(&mut entry.get_mut().egress, work, driver)
            {
                egress::ClearProgress::Done => {}
                egress::ClearProgress::Retry => break 'close true,
                egress::ClearProgress::Waiting => break 'close false,
            }
            let retire = owner(
                Phase::Retire,
                &self.prepared.flights,
                &mut entry.get_mut().connection,
                driver,
            );
            match retire {
                Decision::Runnable => break 'close true,
                Decision::Waiting => break 'close false,
                Decision::Ready => {}
            }
            let retained = {
                let slot = entry.get();
                slot.engine.sending.is_inflight()
                    || slot.engine.establish.is_connecting()
                    || slot.engine.establish.is_tuning()
                    || slot.engine.recv.has_inflight()
                    || !slot.engine.lifecycle.owner_released()
                    || slot.io().ready_handle().has_recv_credit(recv_target)
                    || self.prepared.deferred_recv.has(index)
            };
            if retained {
                break 'close false;
            }
            let slot = entry.remove();
            slot.engine
                .ready_handle()
                .set_target(key.target().dispatch());
            self.close_removed(slot, driver.driver());
            false
        };
        if runnable && let Some(handle) = pending::Pending::of(self).at(key) {
            handle.mark(pending::Action::Close);
        }
    }
}

const _: () = assert!(mem::size_of::<Decision>() == 1);
const _: () = assert!(mem::size_of::<Phase>() == 1);
