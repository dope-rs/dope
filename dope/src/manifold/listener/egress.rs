use std::pin::Pin;

use o3::buffer::{Pooled, Shared};

use super::Listener;
use super::application::Application;
use super::idle::IdlePhase;
use super::send::SendSource;
use super::state::State;
use super::state::WriteBuf;
use crate::DriverContext;
use crate::manifold::env::Env;
use crate::runtime::profile::RuntimeProfile;
use dope_core::driver::token::{SlotIndex, Token};
use dope_core::io::socket::msg::IoVec;
use dope_net::Transport;
use dope_net::link::slot::{PEND_CLOSE, PEND_EGRESS, PEND_SHUTDOWN, Slot};
use dope_net::wire::send::Vectored;
use dope_net::wire::{Reclaim, Wire};

pub(super) enum Egress {
    Clear,
    Inflight,
    Plain,
    Stalled,
    Held,
    Deferred,
}

pub(super) trait SlotFlow<'d> {
    fn egress(&self) -> Egress;
    fn owes_egress(&self) -> bool;
    fn after_handoff(&mut self) -> (bool, bool);
    fn adopt_deferred_close(&mut self);
    fn accept_write(&mut self, write_buf_cap: usize, written: usize) -> bool;
    fn hand_plain(&mut self, plain: &[u8], ud: Token, driver: &mut DriverContext<'_, 'd>);
    fn hand_split(&mut self, iovs: [IoVec; 2], ud: Token, driver: &mut DriverContext<'_, 'd>);
    fn submit_split(
        &mut self,
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
        write_buf: WriteBuf<'_>,
        written: usize,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool;
    fn submit_split_static(
        &mut self,
        write_buf: WriteBuf<'_>,
        hdr_written: usize,
        body: &'static [u8],
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool;
    fn submit_split_shared(
        &mut self,
        write_buf: WriteBuf<'_>,
        hdr_written: usize,
        body: Shared,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool;
    fn submit_split_pooled(
        &mut self,
        write_buf: WriteBuf<'_>,
        hdr_written: usize,
        body: Pooled,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool;
}

impl<'d, W: Wire, C: Default + 'static> SlotFlow<'d> for Slot<'d, W, State<C>> {
    fn egress(&self) -> Egress {
        if self.core.is_send_inflight() {
            Egress::Inflight
        } else if self.state.send.consumed_plain < self.state.send.total_plain {
            if matches!(W::RECLAIM, Reclaim::OnSubmit)
                && self.state.send.inflight_plain == 0
                && !self.wire.holds_plain(&self.send)
            {
                Egress::Stalled
            } else {
                Egress::Plain
            }
        } else if self.wire.holds_plain(&self.send) {
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

    fn after_handoff(&mut self) -> (bool, bool) {
        let armed = self.core.is_send_inflight();
        let restage = matches!(self.egress(), Egress::Plain | Egress::Deferred);
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
        let consumed = self.submit_plain(driver, plain, ud);
        let armed = self.core.is_send_inflight() && !was_inflight;
        self.state.send.record_handoff(consumed, armed);
    }

    fn hand_split(&mut self, iovs: [IoVec; 2], ud: Token, driver: &mut DriverContext<'_, 'd>) {
        let was_inflight = self.core.is_send_inflight();
        let vectored = Vectored::new(
            &iovs,
            &mut self.state.send.pending_iovs,
            &mut self.state.send.pending_msghdr,
        );
        let consumed = Slot::<W, State<C>>::submit_wire_vectored(
            &mut self.core,
            &mut self.wire,
            &mut self.send,
            vectored,
            ud,
            driver,
        );
        let armed = self.core.is_send_inflight() && !was_inflight;
        self.state.send.record_handoff(consumed, armed);
    }

    fn submit_split(
        &mut self,
        write_buf: &mut [u8],
        hdr_written: usize,
        source: SendSource,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        if self.owes_egress() {
            let n = hdr_written.min(write_buf.len());
            let body = source.into_buffer();
            let staged = self
                .state
                .deferred
                .stage_copy_pair(&write_buf[..n], body, false);
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
        write_buf: WriteBuf<'_>,
        written: usize,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        if self.owes_egress() {
            let n = written.min(write_buf.len());
            let staged = self.state.deferred.stage_copy(&write_buf[..n], false);
            if !staged {
                self.core.set_close_after();
            }
            return staged;
        }
        if !self.accept_write(write_buf.len(), written) {
            return false;
        }
        self.state.send.begin(written, SendSource::None);
        self.hand_plain(&write_buf[..written], ud, driver);
        true
    }

    fn submit_split_static(
        &mut self,
        mut write_buf: WriteBuf<'_>,
        hdr_written: usize,
        body: &'static [u8],
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        self.submit_split(
            &mut write_buf,
            hdr_written,
            SendSource::Static(body),
            ud,
            driver,
        )
    }

    fn submit_split_shared(
        &mut self,
        mut write_buf: WriteBuf<'_>,
        hdr_written: usize,
        body: Shared,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        self.submit_split(
            &mut write_buf,
            hdr_written,
            SendSource::Shared(body),
            ud,
            driver,
        )
    }

    fn submit_split_pooled(
        &mut self,
        mut write_buf: WriteBuf<'_>,
        hdr_written: usize,
        body: Pooled,
        ud: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        self.submit_split(
            &mut write_buf,
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

impl<'d, const ID: u8, A, E> EgressPhase<'d, ID, A, E> for Listener<'d, ID, A, E>
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
            if flags & PEND_SHUTDOWN != 0 {
                let this = self.as_mut().project();
                let how = this
                    .pool
                    .get(idx)
                    .map(|s| s.state.pending.shutdown_how())
                    .unwrap_or(0);
                if let Some(fd) = this.pool.fd_of(idx) {
                    let _ = <E::Transport as Transport>::submit_shutdown(driver, fd, how);
                }
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
                match slot.egress() {
                    Egress::Inflight => (false, false),
                    Egress::Plain => {
                        let write_buf = this.aux.write_buf_raw(slot);
                        slot.resume_send(write_buf, send_ud, driver);
                        slot.after_handoff()
                    }
                    Egress::Stalled | Egress::Held | Egress::Clear => {
                        slot.adopt_deferred_close();
                        slot.flush_pending(driver, send_ud);
                        (false, false)
                    }
                    Egress::Deferred => {
                        slot.adopt_deferred_close();
                        let vectored = slot.state.deferred.prepare_send(u32::MAX as usize);
                        let consumed = Slot::<A::Wire, State<A::Conn>>::submit_wire_vectored(
                            &mut slot.core,
                            &mut slot.wire,
                            &mut slot.send,
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
