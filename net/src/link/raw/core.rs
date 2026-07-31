use crate::wire::send::{Payload, Plain, Prepared, Sent, Vectored};
use dope_core::backend::{RawSqe, RetainedSqe, Sqe, StableSqeSource};
use dope_core::driver::DriverContext;
use dope_core::driver::submission::Submission;
use dope_core::driver::token::Token;
use dope_core::driver::token::kind::{RECV, RECV_DISCARD};
use dope_core::io::fd::Fd;
use dope_core::io::socket::addr::Addr;
use libc::sockaddr;

enum Phase {
    Open,
    Draining,
    Closing,
}

enum RecvArm {
    Disarmed,
    Armed { discard: bool, flow: RecvFlow },
    Exhausted { paused: bool },
}

#[derive(Clone, Copy)]
enum RecvFlow {
    Active,
    PausedPending,
    PausedInflight,
    ResumedInflight,
}

trait StableSend {
    fn retained_sqe(&self, fd: &Fd<'_>, ud: Token) -> RetainedSqe;
}

struct PlainSubmission(RawSqe);

// SAFETY: Plain carries the byte-retention proof and Core retains the fixed fd
// until its send state observes terminal completion.
unsafe impl StableSqeSource for PlainSubmission {
    fn into_raw(self) -> RawSqe {
        self.0
    }
}

struct VectoredSubmission(RawSqe);

// SAFETY: Vectored carries the installed message-retention proof and Core
// retains the fixed fd until its send state observes terminal completion.
unsafe impl StableSqeSource for VectoredSubmission {
    fn into_raw(self) -> RawSqe {
        self.0
    }
}

impl StableSend for Plain<'_> {
    fn retained_sqe(&self, fd: &Fd<'_>, ud: Token) -> RetainedSqe {
        RetainedSqe::from_stable(PlainSubmission(RawSqe::send(fd, self.as_slice(), ud)))
    }
}

impl StableSend for Vectored<'_> {
    fn retained_sqe(&self, fd: &Fd<'_>, ud: Token) -> RetainedSqe {
        RetainedSqe::from_stable(VectoredSubmission(RawSqe::send_msg(
            fd,
            self.msghdr().raw(),
            ud,
        )))
    }
}

#[derive(Default)]
pub enum Establish {
    #[default]
    Idle,
    Connecting(Addr),
    Done,
}

impl Establish {
    pub fn begin(&mut self, addr: Addr) -> (*const sockaddr, u32) {
        *self = Self::Connecting(addr);
        let Self::Connecting(pinned) = self else {
            unreachable!()
        };
        (pinned.ptr(), pinned.socklen())
    }

    pub fn finish(&mut self) {
        *self = Self::Done;
    }

    pub fn abort(&mut self) {
        *self = Self::Idle;
    }

    pub fn is_connecting(&self) -> bool {
        matches!(self, Self::Connecting(_))
    }

    pub fn is_done(&self) -> bool {
        matches!(self, Self::Done)
    }
}

pub trait Outbound {
    fn establish(&mut self) -> &mut Establish;
}

pub(in crate::link) enum RecvError {
    Closed,
    Live { needs_rearm: bool },
}

pub(in crate::link) enum Submit {
    Submitted(usize),
    Rejected(usize),
    Idle(usize),
}

const ABORTED: u8 = 1 << 0;
const GRACEFUL_REQUESTED: u8 = 1 << 1;
const GRACEFUL_SEALED: u8 = 1 << 2;
const KERNEL_DISCARD: u8 = 1 << 3;

pub struct Core<'d> {
    pub(in crate::link) fd: Fd<'d>,
    recv: RecvArm,
    phase: Phase,
    send_limit: u32,
    flags: u8,
    discard_remaining: u64,
}

impl<'d> Core<'d> {
    pub fn new(fd: Fd<'d>, kernel_discard: bool) -> Self {
        Self {
            fd,
            recv: RecvArm::Disarmed,
            phase: Phase::Open,
            send_limit: 0,
            flags: if kernel_discard { KERNEL_DISCARD } else { 0 },
            discard_remaining: 0,
        }
    }

    pub fn mark_aborted(&mut self) {
        self.set_flag(ABORTED);
    }

    pub(in crate::link) fn into_fd(self) -> Fd<'d> {
        self.fd
    }

    pub(in crate::link) fn request_graceful(&mut self) -> bool {
        if self.has_flag(ABORTED | GRACEFUL_REQUESTED) {
            return false;
        }
        self.set_flag(GRACEFUL_REQUESTED);
        true
    }

    pub(in crate::link) fn take_graceful(&mut self) -> bool {
        if self.is_send_inflight()
            || !self.has_flag(GRACEFUL_REQUESTED)
            || self.has_flag(GRACEFUL_SEALED)
        {
            return false;
        }
        self.set_flag(GRACEFUL_SEALED);
        true
    }

    fn has_flag(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }

    fn set_flag(&mut self, flag: u8) {
        self.flags |= flag;
    }

    pub(in crate::link) fn armed(&mut self, pushed: bool, discard: bool) {
        self.recv = if pushed {
            RecvArm::Armed {
                discard,
                flow: RecvFlow::Active,
            }
        } else {
            RecvArm::Exhausted { paused: false }
        };
    }

    pub fn is_armed(&self) -> bool {
        matches!(self.recv, RecvArm::Armed { .. })
    }

    pub(in crate::link) fn needs_arm(&self) -> bool {
        matches!(self.recv, RecvArm::Exhausted { paused: false }) && !self.is_closing()
    }

    pub(in crate::link) fn recv_paused(&self) -> bool {
        matches!(
            self.recv,
            RecvArm::Armed {
                flow: RecvFlow::PausedPending | RecvFlow::PausedInflight,
                ..
            } | RecvArm::Exhausted { paused: true }
        )
    }

    pub(in crate::link) fn pause_recv(&mut self) {
        match &mut self.recv {
            RecvArm::Armed { flow, .. } => match flow {
                RecvFlow::Active => *flow = RecvFlow::PausedPending,
                RecvFlow::ResumedInflight => *flow = RecvFlow::PausedInflight,
                RecvFlow::PausedPending | RecvFlow::PausedInflight => {}
            },
            RecvArm::Exhausted { paused } => *paused = true,
            RecvArm::Disarmed => {}
        }
    }

    pub(in crate::link) fn needs_recv_cancel(&self) -> bool {
        matches!(
            self.recv,
            RecvArm::Armed {
                flow: RecvFlow::PausedPending,
                ..
            }
        )
    }

    pub(in crate::link) fn recv_cancel_submitted(&mut self) {
        let RecvArm::Armed { flow, .. } = &mut self.recv else {
            return;
        };
        if matches!(flow, RecvFlow::PausedPending) {
            *flow = RecvFlow::PausedInflight;
        }
    }

    pub(in crate::link) fn resume_recv(&mut self) -> bool {
        match &mut self.recv {
            RecvArm::Armed { flow, .. } => match flow {
                RecvFlow::PausedPending => *flow = RecvFlow::Active,
                RecvFlow::PausedInflight => *flow = RecvFlow::ResumedInflight,
                RecvFlow::Active | RecvFlow::ResumedInflight => return false,
            },
            RecvArm::Exhausted { paused } if *paused => *paused = false,
            RecvArm::Disarmed | RecvArm::Exhausted { .. } => return false,
        }
        self.needs_arm()
    }

    fn settle_recv(&mut self, more: bool) -> bool {
        if more {
            return false;
        }
        let paused = self.recv_paused();
        self.recv = RecvArm::Exhausted { paused };
        self.needs_arm()
    }

    pub fn recv_data(&mut self, more: bool) -> bool {
        self.settle_recv(more)
    }

    pub fn kernel_discard(&self) -> bool {
        self.has_flag(KERNEL_DISCARD)
    }

    pub fn begin_discard(&mut self, n: u64) {
        self.discard_remaining = self.discard_remaining.saturating_add(n);
    }

    pub fn discard_remaining(&self) -> u64 {
        self.discard_remaining
    }

    pub(in crate::link) fn is_discard_armed(&self) -> bool {
        matches!(self.recv, RecvArm::Armed { discard: true, .. })
    }

    pub fn recv_cancel_kind(&self) -> u8 {
        if self.is_discard_armed() {
            RECV_DISCARD
        } else {
            RECV
        }
    }

    pub(in crate::link) fn consume_discard(&mut self, len: usize) -> usize {
        if self.discard_remaining == 0 {
            return 0;
        }
        let take = (len as u64).min(self.discard_remaining) as usize;
        self.discard_remaining -= take as u64;
        take
    }

    pub(in crate::link) fn recv_discarded(&mut self, n: u32) -> bool {
        self.discard_remaining = self.discard_remaining.saturating_sub(n as u64);
        self.settle_recv(false)
    }

    pub fn recv_eof(&mut self, more: bool) {
        self.begin_close();
        self.settle_recv(more);
    }

    pub(in crate::link) fn recv_cancelled(&mut self, more: bool) -> RecvError {
        let needs_rearm = self.settle_recv(more);
        if !more && self.is_closing() {
            RecvError::Closed
        } else {
            RecvError::Live { needs_rearm }
        }
    }

    pub fn recv_starved(&mut self, more: bool) -> bool {
        self.settle_recv(more)
    }

    pub fn recv_failed(&mut self, more: bool) {
        self.set_flag(ABORTED);
        self.begin_close();
        self.settle_recv(more);
    }

    pub fn is_closing(&self) -> bool {
        matches!(self.phase, Phase::Closing)
    }

    pub fn begin_close(&mut self) {
        self.phase = Phase::Closing;
    }

    pub fn close_after(&self) -> bool {
        matches!(self.phase, Phase::Draining)
    }

    pub fn set_close_after(&mut self) {
        if matches!(self.phase, Phase::Open) {
            self.phase = Phase::Draining;
        }
    }

    pub fn should_close(&self, defer: bool) -> bool {
        if self.is_send_inflight() {
            return false;
        }
        match self.phase {
            Phase::Open => false,
            Phase::Draining => !defer,
            Phase::Closing => true,
        }
    }

    pub fn is_send_inflight(&self) -> bool {
        self.send_limit != 0
    }

    pub(in crate::link) fn complete_send(&mut self, bytes: u32) -> Option<Sent> {
        if bytes > self.send_limit {
            self.set_flag(ABORTED);
            self.begin_close();
            self.send_done();
            return None;
        }
        self.send_done();
        Some(Sent::new(bytes))
    }

    pub(in crate::link) fn send_done(&mut self) {
        self.send_limit = 0;
    }

    pub(in crate::link) fn push_retry(
        driver: &mut DriverContext<'_, 'd>,
        mut build: impl FnMut() -> Sqe,
    ) -> bool {
        if driver.push(build()).is_ok() {
            return true;
        }
        if driver.flush_submissions() {
            return driver.push(build()).is_ok();
        }
        false
    }

    fn push_send_retry(
        driver: &mut DriverContext<'_, 'd>,
        fd: &Fd<'d>,
        ud: Token,
        send: &impl StableSend,
    ) -> bool {
        if driver.push_retained(send.retained_sqe(fd, ud)).is_ok() {
            return true;
        }
        if driver.flush_submissions() {
            return driver.push_retained(send.retained_sqe(fd, ud)).is_ok();
        }
        false
    }

    pub(in crate::link) fn submit_prepared(
        &mut self,
        driver: &mut DriverContext<'_, 'd>,
        ud: Token,
        prepared: Prepared<'_>,
    ) -> Submit {
        let (payload, consumed, close_after) = prepared.into_parts();
        if close_after {
            self.set_close_after();
        }
        if self.is_send_inflight() {
            return Submit::Idle(consumed);
        }
        let (submitted, send_limit) = match payload {
            Payload::Empty => return Submit::Idle(consumed),
            Payload::Single(buf) if buf.is_empty() => return Submit::Idle(consumed),
            Payload::Single(buf) => {
                let Ok(send_limit) = u32::try_from(buf.len()) else {
                    return Submit::Rejected(consumed);
                };
                (
                    Self::push_send_retry(driver, &self.fd, ud, &buf),
                    send_limit,
                )
            }
            Payload::Vectored(vectored) if vectored.is_empty() => {
                return Submit::Idle(consumed);
            }
            Payload::Vectored(mut vectored) => {
                let send_limit = vectored.bytes().min(u32::MAX as usize) as u32;
                vectored.install();
                (
                    Self::push_send_retry(driver, &self.fd, ud, &vectored),
                    send_limit,
                )
            }
        };
        if submitted {
            self.send_limit = send_limit;
            Submit::Submitted(consumed)
        } else {
            Submit::Rejected(consumed)
        }
    }
}
