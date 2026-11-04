use std::{collections, time};

use o3::buffer::{self, storage::Shared};

pub(in crate::conn) mod storage;

use crate::proto::path::Peer;
use crate::{conn, stream, wire::varint};
use conn::stream::Streams;
use conn::{output, wire};
use output::{control, recovery};
use path::reset;
use recovery::timer;
use wire::{path, tls};

pub(crate) enum Reset {
    Ignore,
    Accept,
}

#[must_use]
pub(in crate::conn) struct CloseChange;

impl CloseChange {
    pub(in crate::conn) fn effects(self) -> conn::Effects {
        conn::Effects::from_bits(conn::Effects::LIFECYCLE)
    }
}

pub(crate) struct Machine<const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment = Shared> {
    pub(in crate::conn) egress: output::Egress,
    pub(in crate::conn) control: control::Pending,
    pub(in crate::conn) handshake: tls::Handshake<DOMAIN>,
    pub(in crate::conn) path: path::State,
    pub(in crate::conn) streams: Streams<B, D>,
    pub(in crate::conn) receive: conn::ReceiveState<B>,
    pub(in crate::conn) scratch: conn::Scratch,
    pub(in crate::conn) peer: conn::PeerState,
}

impl<const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment> Machine<DOMAIN, B, D> {
    /// Called only by mux installation, before publishing the admitted slot.
    pub(crate) fn try_open(
        &mut self,
        now: time::Instant,
        lifetime: Option<o3::base::time::Positive>,
    ) -> Result<(), conn::Timing> {
        self.egress.lifecycle.deadline = lifetime
            .map(|lifetime| conn::Timing::try_deadline(now, lifetime.get()))
            .transpose()?;
        Ok(())
    }

    pub(in crate::conn) fn deadline(&self) -> Option<time::Instant> {
        if self.is_closing() {
            None
        } else {
            self.peer.application_deadline
        }
    }

    pub(in crate::conn) fn is_closing(&self) -> bool {
        matches!(
            self.egress.lifecycle.state,
            conn::State::Closing | conn::State::Draining | conn::State::Closed
        )
    }

    fn discard_queued(&mut self) {
        if self.is_closing() {
            return;
        }
        self.streams.state.receive.map.discard();
        self.receive.datagrams = collections::VecDeque::new();
        self.egress.datagrams.clear();
    }

    pub(in crate::conn) fn close_now(&mut self) {
        self.discard_queued();
        self.egress.lifecycle.state = conn::State::Closed;
        self.egress.lifecycle.pending_close = None;
        self.set_deadline(None);
        self.peer.reconcile.set_ready(false);
    }

    /// Enforces the admission deadline or admits fixed terminal retention.
    pub(in crate::conn) fn try_terminal(
        &mut self,
        now: time::Instant,
    ) -> Result<bool, conn::Timing> {
        let deadline = match self.egress.lifecycle.state {
            conn::State::Handshaking => match self.egress.lifecycle.deadline {
                Some(deadline) => deadline,
                None => return Ok(false),
            },
            conn::State::Established => return Ok(false),
            conn::State::Closed => return Ok(true),
            conn::State::Closing | conn::State::Draining => match self.egress.lifecycle.deadline {
                Some(deadline) => deadline,
                None => {
                    let deadline = timer::Timer::new(self)
                        .try_terminal(now)
                        .map_err(|error| self.fail_timing(error))?;
                    self.egress.lifecycle.deadline = Some(deadline);
                    self.peer.reconcile.insert(conn::Effects::LIFECYCLE);
                    deadline
                }
            },
        };
        if now >= deadline {
            self.close_now();
            self.peer.reconcile.insert(conn::Effects::LIFECYCLE);
        }
        Ok(self.is_closing())
    }

    pub(in crate::conn) fn drain(&mut self) -> Option<CloseChange> {
        if matches!(
            self.egress.lifecycle.state,
            conn::State::Draining | conn::State::Closed
        ) {
            return None;
        }
        if !self.is_closing() {
            self.egress.lifecycle.deadline = None;
        }
        self.discard_queued();
        self.egress.lifecycle.state = conn::State::Draining;
        self.egress.lifecycle.pending_close = None;
        self.set_deadline(None);
        self.peer.reconcile.set_ready(false);
        Some(CloseChange)
    }

    pub(in crate::conn) fn set_deadline(&mut self, due: Option<time::Instant>) -> conn::Mutation {
        let due = due.filter(|_| !self.is_closing());
        if self.peer.application_deadline == due && !self.peer.application_timeout {
            return conn::Mutation::Unchanged;
        }
        self.peer.application_deadline = due;
        self.peer.application_timeout = false;
        if due.is_some() {
            conn::Mutation::Replaced
        } else {
            conn::Mutation::Cleared
        }
    }

    pub(in crate::conn) fn try_pto(
        &mut self,
    ) -> Result<(time::Duration, time::Duration), conn::Timing> {
        let delay = match &self.peer.transport_params {
            Some(params) => time::Duration::from_millis(u64::from(params.timing().ack_ms())),
            None => time::Duration::ZERO,
        };
        self.egress
            .recovery
            .rtt
            .try_pto(delay)
            .map(|pto| (delay, pto))
            .map_err(|error| self.fail_timing(error))
    }

    pub(in crate::conn) fn try_timers(
        &mut self,
        now: time::Instant,
        changed: bool,
        admitted: Option<(time::Duration, time::Duration)>,
    ) -> Result<(), conn::Timing> {
        if self.try_terminal(now)? {
            return Ok(());
        }
        let timer = timer::Timer::new(self)
            .try_prepare(now, changed, admitted)
            .map_err(|error| self.fail_timing(error))?;
        self.egress.recovery.timer = timer;
        Ok(())
    }

    pub(in crate::conn) fn try_idle(&mut self) -> Result<(), conn::Timing> {
        if self.is_closing() {
            return Ok(());
        }
        let idle = timer::Timer::new(self)
            .try_activity(self.egress.activity.last_activity, None)
            .map_err(|error| self.fail_timing(error))?;
        self.egress.recovery.timer.idle = idle;
        Ok(())
    }

    pub(in crate::conn) fn fail_timing(&mut self, error: conn::Timing) -> conn::Timing {
        self.close_now();
        self.peer.reconcile.insert(conn::Effects::LIFECYCLE);
        error
    }

    pub(in crate::conn) fn record<const RECONCILE: bool>(&mut self, bits: u8) {
        if RECONCILE {
            self.peer.reconcile.insert(bits);
        }
    }

    pub(in crate::conn) fn reset(&mut self, source: Option<Peer>, token: reset::Token) -> Reset {
        if self.egress.lifecycle.state == conn::State::Handshaking
            || !self.path.ingress().has_reset(source, token)
        {
            return Reset::Ignore;
        }
        let _ = self.drain();
        self.control.queue().reset();
        Reset::Accept
    }

    pub(in crate::conn) fn close(&mut self, error: &conn::Error) -> Option<CloseChange> {
        if let conn::Error::Timing(error) = error {
            let changed = self.egress.lifecycle.state != conn::State::Closed;
            self.fail_timing(*error);
            return changed.then_some(CloseChange);
        }
        if matches!(
            error,
            conn::Error::AeadLimitReached | conn::Error::ConnectionIdUnavailable
        ) {
            let changed = self.egress.lifecycle.state != conn::State::Closed;
            self.close_now();
            return changed.then_some(CloseChange);
        }
        let code = error.close_code()?;
        self.queue_close(output::PendingClose::transport(code, varint::VarInt::ZERO))
    }

    pub(in crate::conn) fn queue_close(
        &mut self,
        close: output::PendingClose,
    ) -> Option<CloseChange> {
        if self.is_closing() {
            return None;
        }
        self.discard_queued();
        self.egress.lifecycle.pending_close = Some(close);
        self.egress.lifecycle.deadline = None;
        self.egress.lifecycle.state = conn::State::Closing;
        self.set_deadline(None);
        self.peer.reconcile.set_ready(false);
        Some(CloseChange)
    }
}
