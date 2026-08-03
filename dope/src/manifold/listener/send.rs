use std::pin::Pin;

use dope_core::driver::token::{SlotIndex, Token};
use dope_core::io::SendEvent;
use dope_core::io::socket::msg::{IoVec, MsgHdr};
use dope_net::link::raw::event::SendOutcome;
use dope_net::link::slot::SendBuffer;
use dope_net::wire::send::{Plain, StablePlainSource, StableVectoredSource, Vectored};
use dope_net::wire::{Reclaim, Wire};
use pin_project::pin_project;

use super::Listener;
use super::application::{Application, ApplicationHooks};
use super::egress::{EgressPhase, SlotFlow};
use super::idle::IdlePhase;
use super::state::EgressCtx;
use crate::DriverContext;
use crate::manifold::env::Env;
use crate::runtime::profile::RuntimeProfile;

pub(super) const WRITE_BUF_CAP: usize = 16 * 1024;

#[repr(transparent)]
pub(super) struct Buf([u8; WRITE_BUF_CAP]);

impl Default for Buf {
    fn default() -> Self {
        Self([0; WRITE_BUF_CAP])
    }
}

impl Buf {
    pub(super) fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub(super) fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

/// One listener slot's direct-send retention.
///
/// Header bytes, body ownership, and kernel descriptors share this pinned
/// allocation, so a submitted direct send has no borrow into slot state.
#[pin_project(!Unpin)]
pub(super) struct DirectFlight {
    buf: Buf,
    source: Option<SendBuffer>,
    iovs: [IoVec; 2],
    iov_storage: [IoVec; 2],
    msghdr_storage: MsgHdr,
}

impl DirectFlight {
    pub(super) fn new() -> Self {
        Self {
            buf: Buf::default(),
            source: None,
            iovs: [IoVec::empty(); 2],
            iov_storage: [IoVec::empty(); 2],
            msghdr_storage: MsgHdr::empty(),
        }
    }

    pub(super) fn header(self: Pin<&Self>) -> &[u8] {
        self.get_ref().buf.as_slice()
    }

    pub(super) fn header_mut(self: Pin<&mut Self>) -> &mut [u8] {
        self.project().buf.as_mut_slice()
    }

    pub(super) fn begin(self: Pin<&mut Self>, source: Option<SendBuffer>) {
        let this = self.project();
        debug_assert!(this.source.is_none());
        *this.source = source;
    }

    pub(super) fn clear(self: Pin<&mut Self>) {
        *self.project().source = None;
    }

    pub(super) fn plain<'a>(self: Pin<&'a mut Self>, len: usize) -> Plain<'a> {
        let this = self.project();
        let bytes = &this.buf.as_slice()[..len.min(WRITE_BUF_CAP)];
        Plain::from_stable(DirectPlain { bytes })
    }

    pub(super) fn vectored(self: Pin<&mut Self>, sent: usize, header_len: usize) -> Vectored<'_> {
        let this = self.project();
        let header_len = header_len.min(WRITE_BUF_CAP);
        let header = if sent < header_len {
            &this.buf.as_slice()[sent..header_len]
        } else {
            &[]
        };
        let body = this.source.as_ref().map_or(&[][..], AsRef::as_ref);
        let body_off = sent.saturating_sub(header_len);
        let body = body.get(body_off..).unwrap_or_default();
        this.iovs[0] = IoVec::from_slice(header);
        this.iovs[1] = IoVec::from_slice(body);
        Vectored::from_stable(DirectVectored {
            iovs: this.iovs,
            iov_storage: this.iov_storage,
            msghdr_storage: this.msghdr_storage,
        })
    }
}

struct DirectPlain<'a> {
    bytes: &'a [u8],
}

// SAFETY: DirectFlight is pinned in Aux; listener flow never reuses it before
// the slot observes its terminal send completion.
unsafe impl<'a> StablePlainSource<'a> for DirectPlain<'a> {
    fn into_slice(self) -> &'a [u8] {
        self.bytes
    }
}

struct DirectVectored<'a> {
    iovs: &'a [IoVec],
    iov_storage: &'a mut [IoVec],
    msghdr_storage: &'a mut MsgHdr,
}

// SAFETY: DirectFlight pins its bytes and descriptor storage; listener flow
// retains them unchanged through the terminal completion.
unsafe impl<'a> StableVectoredSource<'a> for DirectVectored<'a> {
    fn into_parts(self) -> (&'a [IoVec], &'a mut [IoVec], &'a mut MsgHdr) {
        (self.iovs, self.iov_storage, self.msghdr_storage)
    }
}

pub(super) struct State {
    pub write_buf_len: usize,
    pub inflight_plain: usize,
    pub consumed_plain: usize,
    pub total_plain: usize,
}

impl Default for State {
    fn default() -> Self {
        Self {
            write_buf_len: 0,
            inflight_plain: 0,
            consumed_plain: 0,
            total_plain: 0,
        }
    }
}

impl State {
    pub(super) fn begin(&mut self, total_plain: usize) {
        self.total_plain = total_plain;
        self.consumed_plain = 0;
        self.inflight_plain = 0;
    }

    pub(super) fn reset(&mut self) {
        self.write_buf_len = 0;
        self.total_plain = 0;
        self.consumed_plain = 0;
        self.inflight_plain = 0;
    }

    pub(super) fn record_handoff(&mut self, consumed: usize, armed: bool) {
        if armed {
            self.inflight_plain = consumed;
        }
        self.consumed_plain += consumed;
    }

    pub(super) fn complete_handoff(&mut self, sent: usize) -> bool {
        if sent > self.inflight_plain {
            return false;
        }
        self.consumed_plain -= self.inflight_plain - sent;
        self.inflight_plain = 0;
        true
    }
}

pub(super) trait SendPhase<'d, const ID: u8, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    fn arm_send_deadline(self: Pin<&mut Self>, idx: SlotIndex, driver: &DriverContext<'_, 'd>);

    fn pump_send(
        self: Pin<&mut Self>,
        token: Token,
        event: SendEvent,
        driver: &mut DriverContext<'_, 'd>,
    );
}

impl<'pool, 'd, const ID: u8, A, E> SendPhase<'d, ID, A, E> for Listener<'pool, 'd, ID, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    fn arm_send_deadline(self: Pin<&mut Self>, idx: SlotIndex, driver: &DriverContext<'_, 'd>) {
        if E::Profile::SEND_DEADLINE.is_none() {
            return;
        }
        let this = self.project();
        let inflight = this
            .pool
            .get(idx)
            .map(|slot| slot.is_send_inflight())
            .unwrap_or(false);
        if inflight {
            this.idle_send.arm(idx, driver.turn_now());
        }
    }

    fn pump_send(
        mut self: Pin<&mut Self>,
        token: Token,
        e: SendEvent,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let mut this = self.as_mut().project();
        let (idx, completion) = match this.pool.classify_send(driver, token, e) {
            SendOutcome::Sent { idx, completion } => (idx, completion),
            SendOutcome::Close { idx, completion } => {
                if let Some(slot) = this.pool.get_mut(idx) {
                    let mut queue = this.egress_arena.queue_for(idx.raw() as usize);
                    slot.abort_egress(&mut queue, completion);
                }
                Self::close_inherent(self.as_mut(), idx, driver);
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
            let sent = this.pool.get_mut(idx).and_then(|slot| {
                slot.complete_egress(
                    &mut this.egress_arena.queue_for(idx.raw() as usize),
                    driver.region_token(),
                    completion,
                )
                .ok()
            });
            let Some(sent) = sent else {
                self.close_inherent(idx, driver);
                return;
            };
            {
                let Some(slot) = this.pool.get_mut(idx) else {
                    return;
                };
                let egress = EgressCtx::for_slot(this.aux, this.egress_arena, idx);
                A::Hooks::send(this.app.as_mut(), slot, egress, sent, driver);
            }
            self.as_mut().commit_chunk(idx, driver);
            return;
        }
        let sent = completion.bytes();
        if matches!(<A::Wire as Wire>::RECLAIM, Reclaim::OnComplete) {
            let valid = this
                .pool
                .get_mut(idx)
                .map(|s| s.state.send.complete_handoff(sent))
                .unwrap_or(false);
            if !valid {
                self.close_inherent(idx, driver);
                return;
            }
        }
        let needs_more = this
            .pool
            .get(idx)
            .map(|s| s.state.send.consumed_plain < s.state.send.total_plain)
            .unwrap_or(false);
        if needs_more {
            let send_ud = token.with_kind(0);
            let armed = if let Some(slot) = this.pool.get_mut(idx) {
                let flight = this.aux.direct_flight(idx);
                slot.resume_send(flight, send_ud, driver);
                slot.is_send_inflight()
            } else {
                false
            };
            if armed && E::Profile::SEND_DEADLINE.is_some() {
                this.idle_send.arm(idx, driver.turn_now());
            }
            return;
        }
        {
            let Some(slot) = this.pool.get_mut(idx) else {
                return;
            };
            let total = slot.state.send.total_plain;
            slot.state.send.reset();
            this.aux.clear_direct(idx);
            debug_assert!(
                !slot.is_send_inflight(),
                "arena buffer reused while a SEND SQE is still in flight"
            );
            let egress = EgressCtx::for_slot(this.aux, this.egress_arena, idx);
            A::Hooks::send(this.app.as_mut(), slot, egress, total, driver);
        }
        self.as_mut().maybe_close_inherent(idx, driver);
    }
}
