use std::pin::Pin;

use o3::buffer::{Pooled, Shared};

use super::Listener;
use super::application::{Application, ApplicationHooks};
use super::idle::IdlePhase;
use super::raw::submission::Submission;
use super::send::SendSource;
use super::state::State;
use super::state::WriteBuf;
use crate::DriverContext;
use crate::manifold::env::Env;
use crate::runtime::profile::RuntimeProfile;
use dope_core::driver::token::{SlotIndex, Token};
use dope_core::io::socket::msg::IoVec;
use dope_net::link::egress::queue::Queue;
use dope_net::link::slot::{PEND_CLOSE, PEND_EGRESS, Slot};
use dope_net::wire::send::{Plain, StablePlainSource};
use dope_net::wire::{Reclaim, Wire};

struct PlainSource<'a>(&'a [u8]);

// SAFETY: PlainSource is private and constructed only from the listener's
// per-slot boxed arena. Send state prevents reuse until completion.
unsafe impl<'a> StablePlainSource<'a> for PlainSource<'a> {
    fn into_slice(self) -> &'a [u8] {
        self.0
    }
}

pub(super) enum Egress {
    Clear,
    Inflight,
    Plain,
    Stalled,
    Held,
    Deferred,
}

pub(super) trait SlotFlow<'d> {
    fn egress(&self, queue: &Queue<'_, '_, 32, dope_net::link::slot::SendBuffer>) -> Egress;
    fn egress_with_deferred(&self, deferred: bool) -> Egress;
    fn owes_egress(&self, queue: &Queue<'_, '_, 32, dope_net::link::slot::SendBuffer>) -> bool;
    fn after_handoff(
        &mut self,
        queue: &Queue<'_, '_, 32, dope_net::link::slot::SendBuffer>,
    ) -> (bool, bool);
    fn adopt_deferred_close(&mut self);
    fn accept_write(&mut self, write_buf_cap: usize, written: usize) -> bool;
    fn hand_plain(&mut self, plain: &[u8], ud: Token, driver: &mut DriverContext<'_, 'd>);
    fn hand_split(&mut self, iovs: [IoVec; 2], ud: Token, driver: &mut DriverContext<'_, 'd>);
    fn submit_split(
        &mut self,
        queue: &mut Queue<'_, '_, 32, dope_net::link::slot::SendBuffer>,
        write_buf: &mut [u8],
        hdr_written: usize,
        source: SendSource,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool;
    fn resume_send(&mut self, write_buf: &[u8], ud: Token, driver: &mut DriverContext<'_, 'd>);
}

pub trait SlotEgress<'d> {
    fn submit_buffered(
        &mut self,
        write_buf: WriteBuf<'_, '_>,
        written: usize,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool;
    fn submit_split_static(
        &mut self,
        write_buf: WriteBuf<'_, '_>,
        hdr_written: usize,
        body: &'static [u8],
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool;
    fn submit_split_shared(
        &mut self,
        write_buf: WriteBuf<'_, '_>,
        hdr_written: usize,
        body: Shared,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool;
    fn submit_split_pooled(
        &mut self,
        write_buf: WriteBuf<'_, '_>,
        hdr_written: usize,
        body: Pooled,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool;
}

impl<'d, W: Wire, C: Default + 'static> SlotFlow<'d> for Slot<'d, W, State<C>> {
    fn egress(&self, queue: &Queue<'_, '_, 32, dope_net::link::slot::SendBuffer>) -> Egress {
        self.egress_with_deferred(!self.state.deferred.is_idle(queue))
    }

    fn egress_with_deferred(&self, deferred: bool) -> Egress {
        if self.core.is_send_inflight() {
            Egress::Inflight
        } else if self.state.send.consumed_plain < self.state.send.total_plain {
            if matches!(W::RECLAIM, Reclaim::OnSubmit)
                && self.state.send.inflight_plain == 0
                && !W::holds_plain(&self.wire, &self.send)
            {
                Egress::Stalled
            } else {
                Egress::Plain
            }
        } else if W::holds_plain(&self.wire, &self.send) {
            Egress::Held
        } else if deferred {
            Egress::Deferred
        } else {
            Egress::Clear
        }
    }

    fn owes_egress(&self, queue: &Queue<'_, '_, 32, dope_net::link::slot::SendBuffer>) -> bool {
        !matches!(self.egress(queue), Egress::Clear)
    }

    fn after_handoff(
        &mut self,
        queue: &Queue<'_, '_, 32, dope_net::link::slot::SendBuffer>,
    ) -> (bool, bool) {
        let armed = self.core.is_send_inflight();
        let restage = matches!(self.egress(queue), Egress::Plain | Egress::Deferred);
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

    fn hand_plain(&mut self, plain: &[u8], ud: Token, driver: &mut DriverContext<'_, 'd>) {
        let was_inflight = self.core.is_send_inflight();
        let consumed = self.submit_plain(driver, Plain::from_stable(PlainSource(plain)), ud);
        let armed = self.core.is_send_inflight() && !was_inflight;
        self.state.send.record_handoff(consumed, armed);
    }

    fn hand_split(&mut self, iovs: [IoVec; 2], ud: Token, driver: &mut DriverContext<'_, 'd>) {
        let was_inflight = self.core.is_send_inflight();
        let consumed = Submission::new(self, &iovs, ud, driver).submit();
        let armed = self.core.is_send_inflight() && !was_inflight;
        self.state.send.record_handoff(consumed, armed);
    }

    fn submit_split(
        &mut self,
        queue: &mut Queue<'_, '_, 32, dope_net::link::slot::SendBuffer>,
        write_buf: &mut [u8],
        hdr_written: usize,
        source: SendSource,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        if self.owes_egress(queue) {
            let n = hdr_written.min(write_buf.len());
            let body = source.into_buffer();
            let staged = self
                .state
                .deferred
                .stage_copy_pair(queue, &write_buf[..n], body, false);
            if !staged {
                self.core.set_close_after();
            }
            return staged;
        }
        if !self.accept_write(write_buf.len(), hdr_written) {
            return false;
        }
        self.state
            .send
            .begin(hdr_written + source.body().len(), source);
        self.resume_send(write_buf, ud, driver);
        true
    }

    fn resume_send(&mut self, write_buf: &[u8], ud: Token, driver: &mut DriverContext<'_, 'd>) {
        let sent = self.state.send.consumed_plain;
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
        let iovs = [IoVec::from_slice(hdr_rem), IoVec::from_slice(body_rem)];
        self.hand_split(iovs, ud, driver);
    }
}

impl<'d, W: Wire, C: Default + 'static> SlotEgress<'d> for Slot<'d, W, State<C>> {
    fn submit_buffered(
        &mut self,
        write_buf: WriteBuf<'_, '_>,
        written: usize,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        let WriteBuf { bytes, mut egress } = write_buf;
        if self.owes_egress(&egress) {
            let n = written.min(bytes.len());
            let staged = self
                .state
                .deferred
                .stage_copy(&mut egress, &bytes[..n], false);
            if !staged {
                self.core.set_close_after();
            }
            return staged;
        }
        if !self.accept_write(bytes.len(), written) {
            return false;
        }
        self.state.send.begin(written, SendSource::None);
        self.hand_plain(&bytes[..written], ud, driver);
        true
    }

    fn submit_split_static(
        &mut self,
        write_buf: WriteBuf<'_, '_>,
        hdr_written: usize,
        body: &'static [u8],
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        let WriteBuf { bytes, mut egress } = write_buf;
        self.submit_split(
            &mut egress,
            bytes,
            hdr_written,
            SendSource::Static(body),
            ud,
            driver,
        )
    }

    fn submit_split_shared(
        &mut self,
        write_buf: WriteBuf<'_, '_>,
        hdr_written: usize,
        body: Shared,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        let WriteBuf { bytes, mut egress } = write_buf;
        self.submit_split(
            &mut egress,
            bytes,
            hdr_written,
            SendSource::Shared(body),
            ud,
            driver,
        )
    }

    fn submit_split_pooled(
        &mut self,
        write_buf: WriteBuf<'_, '_>,
        hdr_written: usize,
        body: Pooled,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        let WriteBuf { bytes, mut egress } = write_buf;
        self.submit_split(
            &mut egress,
            bytes,
            hdr_written,
            SendSource::Pooled(body),
            ud,
            driver,
        )
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
            if slot.core.is_closing() {
                (false, false)
            } else {
                let mut egress = this.egress_arena.queue_for(idx.raw() as usize);
                match slot.egress(&egress) {
                    Egress::Inflight => (false, false),
                    Egress::Plain => {
                        let write_buf = this.aux.write_buf_raw(slot);
                        slot.resume_send(write_buf, send_ud, driver);
                        slot.after_handoff(&egress)
                    }
                    Egress::Stalled | Egress::Held | Egress::Clear => {
                        slot.adopt_deferred_close();
                        slot.flush_pending(driver, send_ud);
                        (false, false)
                    }
                    Egress::Deferred => {
                        slot.adopt_deferred_close();
                        let vectored = slot
                            .state
                            .deferred
                            .prepare_send(&mut egress, u32::MAX as usize);
                        let consumed = Slot::<A::Wire, State<A::Conn>>::submit_wire_vectored(
                            &mut slot.core,
                            &mut slot.wire,
                            &mut slot.send,
                            vectored,
                            send_ud,
                            driver,
                        );
                        if matches!(<A::Wire as Wire>::RECLAIM, Reclaim::OnSubmit)
                            && !slot.state.deferred.try_ack(&mut egress, consumed)
                        {
                            slot.core.begin_close();
                            return;
                        }
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
            if slot.core.is_closing() {
                if slot.core.should_close(defer) {
                    Step::Close
                } else {
                    Step::Idle
                }
            } else {
                let deferred = this.egress_arena.bytes(idx.raw() as usize) != 0;
                match slot.egress_with_deferred(deferred) {
                    Egress::Plain | Egress::Deferred => Step::Retry,
                    Egress::Inflight | Egress::Stalled | Egress::Held => Step::Idle,
                    Egress::Clear if slot.core.should_close(defer) => Step::Close,
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
