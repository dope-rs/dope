use std::{ops, task::Poll};

use dope_core::{
    driver::{
        receive::{Absent, Attempt, Registration, Stale},
        result,
        retained::{self, flight},
        route::Identity,
    },
    io::event::net::recv::ControlKind,
};
use receive::discard;

use crate::{
    link::{
        pool,
        pool::recv::Resident,
        slot::{
            self,
            recv::{Capture, Retirement, Submit},
            transmit,
            transmit::direct,
        },
    },
    wire::{self, receive, terminal},
};

/// Connection-owned control work, ordered independently of endpoint policy.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Demand {
    /// Tear the connection down without further wire work.
    Discard,
    /// Submit the retained terminal record.
    Terminal,
    /// Wait for the submitted terminal record to complete.
    Barrier,
    /// Close the connection after outstanding work completes.
    Close,
    /// Continue ordinary connection work.
    Continue,
}

/// Application gate that controls when a draining connection may close.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CloseGate {
    /// Close as soon as all connection-owned work settles.
    Immediate,
    /// Keep a draining connection open while application work is held.
    Deferred,
}

const _: () = assert!(std::mem::size_of::<CloseGate>() == 1);

/// Exact close-readiness result produced by the connection lifecycle.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Readiness {
    /// The connection may perform its close transition.
    Due,
    /// Connection-owned or application-owned work still prevents closing.
    Waiting,
}

const _: () = assert!(std::mem::size_of::<Readiness>() == 1);

pub struct Control<'w, 'd, ID: Identity, W: wire::Spec + 'w>
where
    'd: 'w,
{
    pub(in crate::link) engine: slot::Engine<'d, ID, W>,
    /// Wire terminal state retained until both directions settle.
    pub terminal: terminal::State<terminal::Error<'w, W>>,
}

pub struct Connection<'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w, S>
where
    'd: 'w,
{
    pub(in crate::link) control: Control<'w, 'd, ID, W>,
    pub(in crate::link) wire: W::Connection<'w, ID>,
    pub(in crate::link) send: W::SendStorage<'w>,
    pub state: S,
    pub(in crate::link) key: pool::Key<'d, ID>,
}

impl<'w, 'd, ID: Identity, W: wire::Spec + 'w> Control<'w, 'd, ID, W>
where
    'd: 'w,
{
    pub(in crate::link) fn new(
        engine: slot::Engine<'d, ID, W>,
        terminal: terminal::State<terminal::Error<'w, W>>,
    ) -> Self {
        Self { engine, terminal }
    }

    pub fn is_closing(&self) -> bool {
        self.engine.operations.lifecycle.is_closing()
    }

    pub(in crate::link) fn is_recv_done(&self) -> bool {
        self.is_closing() || self.terminal.is_read_done()
    }

    /// Reports a receive continuation still owned by the application.
    pub const fn is_app_held(&self) -> bool {
        self.engine.operations.recv.is_app_held()
    }

    pub fn is_connected(&self) -> bool {
        self.engine.establish.established().is_some()
    }

    pub fn close_due(&self, gate: CloseGate) -> Readiness {
        if self.terminal.is_pending()
            || (self.is_draining()
                && <<W::Receive as receive::Policy<W>>::Discard as discard::Policy>::is_pending(
                    &self.engine.discard,
                ))
        {
            return Readiness::Waiting;
        }
        self.engine
            .operations
            .lifecycle
            .close_due(self.engine.operations.sending.is_inflight(), gate)
    }

    pub fn discard(&mut self) {
        self.engine.discard(&mut self.terminal);
    }

    pub fn is_discarded(&self) -> bool {
        self.engine.operations.lifecycle.close.is_discarded()
    }

    /// Classifies the next connection-owned control operation.
    pub fn demand(&self) -> Demand {
        if self.is_discarded() {
            Demand::Discard
        } else if self.terminal.has_send() {
            Demand::Terminal
        } else if self.terminal.kind().is_some() {
            Demand::Barrier
        } else if self.is_closing() {
            Demand::Close
        } else {
            Demand::Continue
        }
    }

    pub fn close_after(&mut self) {
        self.engine.operations.lifecycle.drain();
    }

    pub fn is_draining(&self) -> bool {
        self.engine.operations.lifecycle.is_draining()
    }

    /// Ends a raw discard when receive EOF or an explicit deadline cancels it.
    pub fn end_discard(&mut self) {
        self.engine.discard =
            <<W::Receive as receive::Policy<W>>::Discard as discard::Policy>::STATE;
    }

    pub fn shutdown(&mut self) {
        self.engine.operations.lifecycle.close.shutdown();
    }

    /// Borrows the connection's readiness and lifecycle capabilities.
    pub fn io(&self) -> slot::Io<'_, 'd, ID, W> {
        slot::Io::new(&self.engine)
    }
}

impl<'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w, S> Connection<'w, 'd, ID, W, S>
where
    'd: 'w,
{
    pub fn key(&self) -> pool::Key<'d, ID> {
        self.key
    }

    pub fn send_status(&self) -> direct::Status<'_, 'w, 'd, ID, W, S> {
        direct::Status::new(self)
    }

    pub fn sending(&mut self) -> transmit::Sending<'_, 'w, 'd, ID, W, S> {
        transmit::Sending::new(self)
    }

    /// Borrows application state and the disjoint direct-send capability.
    pub fn split_send(&mut self) -> (&mut S, direct::raw::Sending<'_, 'w, 'd, ID, W>) {
        let target = self.key.target();
        let send = direct::raw::Sending::new(
            &mut self.control.engine,
            &mut self.wire,
            &mut self.send,
            &mut self.control.terminal,
            target,
        );
        (&mut self.state, send)
    }
    /// Starts discarding receive bytes when the wire policy supports it.
    pub fn try_discard(&mut self, prefix: discard::Prefix) -> discard::Start {
        if self.engine.operations.lifecycle.is_closing() {
            return discard::Start::Closing;
        }
        <<W::Receive as receive::Policy<W>>::Discard as discard::Policy>::begin(
            &mut self.engine.discard,
            prefix,
        )
    }
}

impl<'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w, S> ops::Deref for Connection<'w, 'd, ID, W, S>
where
    'd: 'w,
{
    type Target = Control<'w, 'd, ID, W>;

    fn deref(&self) -> &Self::Target {
        &self.control
    }
}

impl<'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w, S> ops::DerefMut
    for Connection<'w, 'd, ID, W, S>
where
    'd: 'w,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.control
    }
}

const _: () = assert!(std::mem::size_of::<Demand>() == 1);

enum Probe<'d> {
    Ready(Attempt<'d>),
    Waiting,
    Absent,
}

impl<'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w, S> Connection<'w, 'd, ID, W, S>
where
    'd: 'w,
{
    pub(in crate::link) fn resident(&self) -> Resident<'_, 'd, ID> {
        Resident::new(&self.key)
    }

    pub(in crate::link) fn submit_recv(
        &mut self,
        slots: &flight::Slots<'d, ID>,
        ctx: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<Submit, result::SubmitError> {
        let conn = self;
        if conn.control.engine.operations.recv.is_armed() {
            return Ok(Submit::Armed);
        }
        if conn.is_recv_done() || conn.control.engine.operations.recv.is_paused() {
            return Ok(Submit::Deferred);
        }
        let mut exhaust = conn.control.engine.operations.recv.exhaustion();
        let probe = match exhaust.bind() {
            Some(resume) => {
                let target = conn.control.engine.establish.ready_handle().target();
                match Registration::try_acquire(target) {
                    Ok(reg) => match reg.resume() {
                        Ok(Poll::Ready(probe)) => {
                            resume.commit();
                            Probe::Ready(probe)
                        }
                        Ok(Poll::Pending) => {
                            resume.wait();
                            Probe::Waiting
                        }
                        Err(Stale) => {
                            resume.cancel();
                            Probe::Absent
                        }
                    },
                    Err(Absent) => {
                        resume.cancel();
                        Probe::Absent
                    }
                }
            }
            None => Probe::Absent,
        };
        if matches!(probe, Probe::Waiting) {
            return Ok(Submit::Waiting);
        }
        let target = conn.key.target();
        let Some(fd) = conn.control.engine.fd() else {
            return Ok(Submit::Deferred);
        };
        let op = match slots.recv(ctx.driver().reborrow(), fd, target) {
            Ok(op) => op,
            Err(error) => match error.cause() {
                result::SubmitCause::Full => return Ok(Submit::Deferred),
                result::SubmitCause::IdentityExhausted => {
                    conn.discard();
                    return Err(error);
                }
            },
        };
        conn.control.engine.operations.arm_recv(op);
        match probe {
            Probe::Ready(probe) => probe.commit(),
            Probe::Waiting | Probe::Absent => {}
        }
        Ok(Submit::Armed)
    }

    pub(in crate::link) fn recv_eof(&mut self) -> slot::Control
    where
        <W::Receive as receive::Policy<W>>::Eof: Capture<W>,
    {
        if self.control.terminal.is_read_done() {
            return slot::Control::NoChunk { needs_rearm: false };
        }
        <<W::Receive as receive::Policy<W>>::Eof as Capture<W>>::capture(self);
        slot::Control::Eof
    }

    pub(in crate::link) fn cancelled(&mut self, end: bool, arm: bool) -> slot::Control {
        let close = self.control.engine.operations.lifecycle.is_closing();
        if end && close {
            slot::Control::Cancelled
        } else {
            slot::Control::NoChunk { needs_rearm: arm }
        }
    }

    pub(in crate::link) fn recv_failed(&mut self, error: dope_core::Errno) -> slot::Control {
        self.discard();
        slot::Control::Failed(error)
    }

    pub(in crate::link) fn retire_recv(&mut self, kind: ControlKind<'d>) -> Retirement<'d>
    where
        <W::Receive as receive::Policy<W>>::Eof: Capture<W>,
    {
        match kind {
            ControlKind::Eof
                if self.control.terminal.is_done() || self.control.terminal.is_read_done() =>
            {
                Retirement::Ready
            }
            ControlKind::Eof => {
                <<W::Receive as receive::Policy<W>>::Eof as Capture<W>>::capture(self);
                Retirement::Ready
            }
            ControlKind::Cancelled => Retirement::Ready,
            ControlKind::Exhausted(proof) => Retirement::Exhausted(proof),
            ControlKind::Starved(error) => Retirement::Starved(error),
            ControlKind::Failed(error) => {
                self.discard();
                Retirement::Failed(error)
            }
        }
    }
}
