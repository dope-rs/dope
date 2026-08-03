use std::pin::Pin;

use dope_core::driver::token::{SlotIndex, Token};
use dope_net::link::egress::queue::Queue;
use dope_net::link::slot::{PEND_CLOSE, PEND_EGRESS, SendBuffer, Slot};
use dope_net::wire::{Reclaim, Wire};
use o3::buffer::{Pooled, Shared};

use super::Listener;
use super::application::{Application, ApplicationHooks};
use super::idle::IdlePhase;
use super::send::DirectFlight;
use super::state::{State, WriteBuf, WriteStorage};
use crate::DriverContext;
use crate::manifold::env::Env;
use crate::runtime::profile::RuntimeProfile;

pub(super) enum Egress {
    Clear,
    Inflight,
    Plain,
    Stalled,
    Held,
    Deferred,
}

pub(super) trait SlotFlow<'d> {
    fn egress(&self, queue: &Queue<'_, 'd, '_, 32, dope_net::link::slot::SendBuffer>) -> Egress;
    fn egress_with_deferred(&self, deferred: bool) -> Egress;
    fn owes_egress(&self, queue: &Queue<'_, 'd, '_, 32, dope_net::link::slot::SendBuffer>) -> bool;
    fn after_handoff(
        &mut self,
        queue: &Queue<'_, 'd, '_, 32, dope_net::link::slot::SendBuffer>,
    ) -> (bool, bool);
    fn adopt_deferred_close(&mut self);
    fn accept_write(&mut self, write_buf_cap: usize, written: usize) -> bool;
    fn hand_plain(
        &mut self,
        flight: Pin<&mut DirectFlight>,
        len: usize,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    );
    fn hand_split(
        &mut self,
        flight: Pin<&mut DirectFlight>,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    );
    fn submit_split(
        &mut self,
        hdr_written: usize,
        write_buf: WriteBuf<'_, 'd, '_>,
        source: SendBuffer,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool;
    fn resume_send(
        &mut self,
        flight: Pin<&mut DirectFlight>,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    );
}

pub trait SlotEgress<'d> {
    fn submit_buffered(
        &mut self,
        write_buf: WriteBuf<'_, 'd, '_>,
        written: usize,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool;
    fn submit_split_static(
        &mut self,
        write_buf: WriteBuf<'_, 'd, '_>,
        hdr_written: usize,
        body: &'static [u8],
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool;
    fn submit_split_shared(
        &mut self,
        write_buf: WriteBuf<'_, 'd, '_>,
        hdr_written: usize,
        body: Shared,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool;
    fn submit_split_pooled(
        &mut self,
        write_buf: WriteBuf<'_, 'd, '_>,
        hdr_written: usize,
        body: Pooled,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool;
}

impl<'d, W: Wire, C: Default + 'static> SlotFlow<'d> for Slot<'d, W, State<C>> {
    fn egress(&self, queue: &Queue<'_, 'd, '_, 32, dope_net::link::slot::SendBuffer>) -> Egress {
        self.egress_with_deferred(!self.state.deferred.is_idle(queue))
    }

    fn egress_with_deferred(&self, deferred: bool) -> Egress {
        if self.is_send_inflight() {
            Egress::Inflight
        } else if self.state.send.consumed_plain < self.state.send.total_plain {
            if matches!(W::RECLAIM, Reclaim::OnSubmit)
                && self.state.send.inflight_plain == 0
                && !self.holds_plain()
            {
                Egress::Stalled
            } else {
                Egress::Plain
            }
        } else if self.holds_plain() {
            Egress::Held
        } else if deferred {
            Egress::Deferred
        } else {
            Egress::Clear
        }
    }

    fn owes_egress(&self, queue: &Queue<'_, 'd, '_, 32, dope_net::link::slot::SendBuffer>) -> bool {
        !matches!(self.egress(queue), Egress::Clear)
    }

    fn after_handoff(
        &mut self,
        queue: &Queue<'_, 'd, '_, 32, dope_net::link::slot::SendBuffer>,
    ) -> (bool, bool) {
        let armed = self.is_send_inflight();
        let restage = matches!(self.egress(queue), Egress::Plain | Egress::Deferred);
        (armed, restage)
    }

    fn adopt_deferred_close(&mut self) {
        if self.state.deferred.close_after() {
            self.set_close_after();
        }
    }

    fn accept_write(&mut self, write_buf_cap: usize, written: usize) -> bool {
        if written > write_buf_cap {
            self.set_close_after();
            return false;
        }
        self.state.send.write_buf_len = written;
        true
    }

    fn hand_plain(
        &mut self,
        mut flight: Pin<&mut DirectFlight>,
        len: usize,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let was_inflight = self.is_send_inflight();
        let consumed = self.submit_plain(driver, flight.as_mut().plain(len), ud);
        let armed = self.is_send_inflight() && !was_inflight;
        self.state.send.record_handoff(consumed, armed);
    }

    fn hand_split(
        &mut self,
        mut flight: Pin<&mut DirectFlight>,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let was_inflight = self.is_send_inflight();
        let consumed = self.submit_vectored(
            flight.as_mut().vectored(
                self.state.send.consumed_plain,
                self.state.send.write_buf_len,
            ),
            ud,
            driver,
        );
        let armed = self.is_send_inflight() && !was_inflight;
        self.state.send.record_handoff(consumed, armed);
    }

    fn submit_split(
        &mut self,
        hdr_written: usize,
        write_buf: WriteBuf<'_, 'd, '_>,
        source: SendBuffer,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        let (storage, mut queue) = write_buf.into_parts();
        if self.owes_egress(&queue) {
            let bytes = storage.as_slice();
            let n = hdr_written.min(bytes.len());
            let body = (!source.as_ref().is_empty()).then_some(source);
            let staged = self.state.deferred.stage_copy_pair(
                driver.region_token(),
                &mut queue,
                &bytes[..n],
                body,
                false,
            );
            if !staged {
                self.set_close_after();
            }
            return staged;
        }
        let WriteStorage::Direct(mut flight) = storage else {
            unreachable!("direct listener send must retain its write buffer")
        };
        if !self.accept_write(flight.as_ref().header().len(), hdr_written) {
            return false;
        }
        let total = hdr_written + source.as_ref().len();
        self.state.send.begin(total);
        flight.as_mut().begin(Some(source));
        self.resume_send(flight, ud, driver);
        true
    }

    fn resume_send(
        &mut self,
        flight: Pin<&mut DirectFlight>,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.hand_split(flight, ud, driver);
    }
}

impl<'d, W: Wire, C: Default + 'static> SlotEgress<'d> for Slot<'d, W, State<C>> {
    fn submit_buffered(
        &mut self,
        write_buf: WriteBuf<'_, 'd, '_>,
        written: usize,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        let (storage, mut egress) = write_buf.into_parts();
        if self.owes_egress(&egress) {
            let bytes = storage.as_slice();
            let n = written.min(bytes.len());
            let staged = self.state.deferred.stage_copy(
                driver.region_token(),
                &mut egress,
                &bytes[..n],
                false,
            );
            if !staged {
                self.set_close_after();
            }
            return staged;
        }
        let WriteStorage::Direct(mut flight) = storage else {
            unreachable!("direct listener send must retain its write buffer")
        };
        if !self.accept_write(flight.as_ref().header().len(), written) {
            return false;
        }
        self.state.send.begin(written);
        flight.as_mut().begin(None);
        self.hand_plain(flight, written, ud, driver);
        true
    }

    fn submit_split_static(
        &mut self,
        write_buf: WriteBuf<'_, 'd, '_>,
        hdr_written: usize,
        body: &'static [u8],
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        self.submit_split(hdr_written, write_buf, SendBuffer::Static(body), ud, driver)
    }

    fn submit_split_shared(
        &mut self,
        write_buf: WriteBuf<'_, 'd, '_>,
        hdr_written: usize,
        body: Shared,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        self.submit_split(hdr_written, write_buf, body.into(), ud, driver)
    }

    fn submit_split_pooled(
        &mut self,
        write_buf: WriteBuf<'_, 'd, '_>,
        hdr_written: usize,
        body: Pooled,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        self.submit_split(hdr_written, write_buf, body.into(), ud, driver)
    }
}

pub(super) trait EgressPhase<'d, const ID: u8, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    fn flush_dirty(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>);

    fn commit_chunk(self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>);

    fn maybe_close_inherent(
        self: Pin<&mut Self>,
        idx: SlotIndex,
        driver: &mut DriverContext<'_, 'd>,
    );
}

impl<'pool, 'd, const ID: u8, A, E> EgressPhase<'d, ID, A, E> for Listener<'pool, 'd, ID, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
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
            if flags & PEND_CLOSE != 0 {
                Self::close_inherent(self.as_mut(), idx, driver);
                continue;
            }
            if flags & PEND_EGRESS != 0 {
                self.as_mut().commit_chunk(idx, driver);
            }
        }
    }

    fn commit_chunk(mut self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>) {
        let (armed_send, restage) = {
            let this = self.as_mut().project();
            let Some(slot) = this.pool.get_mut(idx) else {
                return;
            };
            let send_ud = slot.token();
            if slot.is_closing() {
                (false, false)
            } else {
                let mut egress = this.egress_arena.queue_for(idx.raw() as usize);
                match slot.egress(&egress) {
                    Egress::Inflight => (false, false),
                    Egress::Plain => {
                        let flight = this.aux.direct_flight(idx);
                        slot.resume_send(flight, send_ud, driver);
                        slot.after_handoff(&egress)
                    }
                    Egress::Stalled | Egress::Held | Egress::Clear => {
                        slot.adopt_deferred_close();
                        slot.flush_pending(driver, send_ud);
                        (false, false)
                    }
                    Egress::Deferred => {
                        slot.adopt_deferred_close();
                        slot.submit_egress(&mut egress, send_ud, driver);
                        slot.after_handoff(&egress)
                    }
                }
            }
        };
        if armed_send && E::Profile::SEND_DEADLINE.is_some() {
            self.as_mut()
                .project()
                .idle_send
                .arm(idx, driver.turn_now());
        }
        if restage {
            let this = self.as_mut().project();
            if let Some(slot) = this.pool.get(idx) {
                this.dirty.mark(idx, &slot.state.pending, PEND_EGRESS);
            }
        }
        self.maybe_close_inherent(idx, driver);
    }

    fn maybe_close_inherent(
        mut self: Pin<&mut Self>,
        idx: SlotIndex,
        driver: &mut DriverContext<'_, 'd>,
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
            let defer = A::Hooks::defer_close(this.app, slot);
            if slot.is_closing() {
                if slot.should_close(defer) {
                    Step::Close
                } else {
                    Step::Idle
                }
            } else {
                let deferred = this.egress_arena.bytes(idx.raw() as usize) != 0;
                match slot.egress_with_deferred(deferred) {
                    Egress::Plain | Egress::Deferred => Step::Retry,
                    Egress::Inflight | Egress::Stalled | Egress::Held => Step::Idle,
                    Egress::Clear if slot.should_close(defer) => Step::Close,
                    Egress::Clear => Step::Idle,
                }
            }
        };
        match step {
            Step::Close => Self::close_inherent(self.as_mut(), idx, driver),
            Step::Retry => {
                let this = self.as_mut().project();
                if let Some(slot) = this.pool.get(idx) {
                    this.dirty.mark(idx, &slot.state.pending, PEND_EGRESS);
                }
            }
            Step::Idle => {}
        }
    }
}
