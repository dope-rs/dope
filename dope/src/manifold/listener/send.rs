use std::pin::Pin;

use o3::buffer::{Pooled, Shared};

use super::Listener;
use super::application::Application;
use super::egress::{EgressPhase, SlotFlow};
use super::idle::IdlePhase;
use crate::DriverContext;
use crate::manifold::env::Env;
use crate::runtime::profile::RuntimeProfile;
use dope_core::driver::token::{SlotIndex, Token};
use dope_core::io::SendEvent;
use dope_core::io::socket::msg::{IoVec, MsgHdr};
use dope_net::link::raw::pool::SendOutcome;
use dope_net::link::slot::SendBuffer;
use dope_net::wire::{Reclaim, Wire};

pub(super) const WRITE_BUF_CAP: usize = 16 * 1024;

#[repr(transparent)]
pub(super) struct Buf([u8; WRITE_BUF_CAP]);

impl Default for Buf {
    fn default() -> Self {
        Self([0; WRITE_BUF_CAP])
    }
}

impl Buf {
    pub(super) fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

#[derive(Default)]
pub(super) enum SendSource {
    #[default]
    None,
    Static(&'static [u8]),
    Shared(Shared),
    Pooled(Pooled),
}

impl SendSource {
    pub(super) fn body(&self) -> &[u8] {
        match self {
            Self::None => &[],
            Self::Static(s) => s,
            Self::Shared(s) => s.as_ref(),
            Self::Pooled(s) => s.as_ref(),
        }
    }

    pub(super) fn into_buffer(self) -> Option<SendBuffer> {
        match self {
            Self::None => None,
            Self::Static(bytes) => (!bytes.is_empty()).then_some(SendBuffer::Static(bytes)),
            Self::Shared(bytes) => (!bytes.is_empty()).then(|| bytes.into()),
            Self::Pooled(bytes) => (!bytes.is_empty()).then(|| bytes.into()),
        }
    }
}

pub(super) struct State {
    pub write_buf_len: usize,
    pub inflight_plain: usize,
    pub consumed_plain: usize,
    pub total_plain: usize,
    pub source: SendSource,
    pub pending_iovs: [IoVec; 4],
    pub pending_msghdr: MsgHdr,
}

impl Default for State {
    fn default() -> Self {
        Self {
            write_buf_len: 0,
            inflight_plain: 0,
            consumed_plain: 0,
            total_plain: 0,
            source: SendSource::None,
            pending_iovs: [IoVec::empty(); 4],
            pending_msghdr: MsgHdr::empty(),
        }
    }
}

impl State {
    pub(super) fn begin(&mut self, total_plain: usize, source: SendSource) {
        self.total_plain = total_plain;
        self.consumed_plain = 0;
        self.inflight_plain = 0;
        self.source = source;
    }

    pub(super) fn reset(&mut self) {
        self.write_buf_len = 0;
        self.total_plain = 0;
        self.consumed_plain = 0;
        self.inflight_plain = 0;
        self.source = SendSource::None;
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

impl<'d, const ID: u8, A, E> SendPhase<'d, ID, A, E> for Listener<'d, ID, A, E>
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
            .map(|s| s.core.is_send_inflight())
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
        let (idx, sent) = match this.pool.classify_send(driver, token, e) {
            SendOutcome::Sent { idx: i, n } => (i, n),
            SendOutcome::Close(i) => {
                Self::close_inherent(self.as_mut(), i, driver);
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
            if let Some(slot) = this.pool.get_mut(idx)
                && matches!(<A::Wire as Wire>::RECLAIM, Reclaim::OnComplete)
            {
                slot.state.deferred.ack(sent);
            }
            {
                let Some(slot) = this.pool.get_mut(idx) else {
                    return;
                };
                this.app.as_mut().send(slot, sent, this.aux, driver);
            }
            self.as_mut().commit_chunk(idx, driver);
            return;
        }
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
            let send_ud = Token::new(ID, idx, token.epoch());
            let armed = if let Some(slot) = this.pool.get_mut(idx) {
                let write_buf = this.aux.write_buf_raw(slot);
                slot.resume_send(write_buf, send_ud, driver);
                slot.core.is_send_inflight()
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
            debug_assert!(
                !slot.core.is_send_inflight(),
                "arena buffer reused while a SEND SQE is still in flight"
            );
            this.app.as_mut().send(slot, total, this.aux, driver);
        }
        self.as_mut().maybe_close_inherent(idx, driver);
    }
}
