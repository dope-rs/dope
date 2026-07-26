use crate::wire::send::Vectored;
use crate::wire::send::{Payload, Prepared};
use dope_core::backend::Sqe;
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
    Armed { discard: bool },
    Exhausted,
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

pub(super) enum RecvError {
    Closed,
    Live { needs_rearm: bool },
}

pub(super) enum Submit {
    Submitted(usize),
    Rejected(usize),
    Idle(usize),
}

pub struct Core<'d> {
    pub(super) fd: Fd<'d>,
    recv: RecvArm,
    phase: Phase,
    send_in_flight: bool,
    aborted: bool,
    graceful_requested: bool,
    graceful_sealed: bool,
    kernel_discard: bool,
    discard_remaining: u64,
}

impl<'d> Core<'d> {
    pub fn new(fd: Fd<'d>, kernel_discard: bool) -> Self {
        Self {
            fd,
            recv: RecvArm::Disarmed,
            phase: Phase::Open,
            send_in_flight: false,
            aborted: false,
            graceful_requested: false,
            graceful_sealed: false,
            kernel_discard,
            discard_remaining: 0,
        }
    }

    pub fn mark_aborted(&mut self) {
        self.aborted = true;
    }

    pub(super) fn into_fd(self) -> Fd<'d> {
        self.fd
    }

    pub(super) fn request_graceful(&mut self) -> bool {
        if self.aborted || self.graceful_requested {
            return false;
        }
        self.graceful_requested = true;
        true
    }

    pub(super) fn take_graceful(&mut self) -> bool {
        if self.send_in_flight || !self.graceful_requested || self.graceful_sealed {
            return false;
        }
        self.graceful_sealed = true;
        true
    }

    pub(super) fn armed(&mut self, pushed: bool, discard: bool) {
        self.recv = if pushed {
            RecvArm::Armed { discard }
        } else {
            RecvArm::Exhausted
        };
    }

    pub fn is_armed(&self) -> bool {
        matches!(self.recv, RecvArm::Armed { .. })
    }

    pub(super) fn needs_arm(&self) -> bool {
        matches!(self.recv, RecvArm::Exhausted) && !self.is_closing()
    }

    fn settle_recv(&mut self, more: bool) -> bool {
        if more {
            return false;
        }
        self.recv = RecvArm::Exhausted;
        self.needs_arm()
    }

    pub fn recv_data(&mut self, more: bool) -> bool {
        self.settle_recv(more)
    }

    pub fn kernel_discard(&self) -> bool {
        self.kernel_discard
    }

    pub fn begin_discard(&mut self, n: u64) {
        self.discard_remaining = self.discard_remaining.saturating_add(n);
    }

    pub fn discard_remaining(&self) -> u64 {
        self.discard_remaining
    }

    pub(super) fn is_discard_armed(&self) -> bool {
        matches!(self.recv, RecvArm::Armed { discard: true })
    }

    pub fn recv_cancel_kind(&self) -> u8 {
        if self.is_discard_armed() {
            RECV_DISCARD
        } else {
            RECV
        }
    }

    pub(super) fn consume_discard(&mut self, len: usize) -> usize {
        if self.discard_remaining == 0 {
            return 0;
        }
        let take = (len as u64).min(self.discard_remaining) as usize;
        self.discard_remaining -= take as u64;
        take
    }

    pub(super) fn recv_discarded(&mut self, n: u32) -> bool {
        self.discard_remaining = self.discard_remaining.saturating_sub(n as u64);
        self.settle_recv(false)
    }

    pub fn recv_eof(&mut self, more: bool) {
        self.begin_close();
        self.settle_recv(more);
    }

    pub(super) fn recv_cancelled(&mut self, more: bool) -> RecvError {
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
        self.aborted = true;
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
        if self.send_in_flight {
            return false;
        }
        match self.phase {
            Phase::Open => false,
            Phase::Draining => !defer,
            Phase::Closing => true,
        }
    }

    pub fn is_send_inflight(&self) -> bool {
        self.send_in_flight
    }

    pub(super) fn send_done(&mut self) {
        self.send_in_flight = false;
    }

    pub(super) fn push_retry(
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

    fn submit_single(&mut self, driver: &mut DriverContext<'_, 'd>, ud: Token, buf: &[u8]) -> bool {
        let fd = &self.fd;
        let submitted = Self::push_retry(driver, || Sqe::send(fd, buf, ud));
        if submitted {
            self.send_in_flight = true;
        }
        submitted
    }

    fn submit_vectored(
        &mut self,
        driver: &mut DriverContext<'_, 'd>,
        ud: Token,
        mut vectored: Vectored<'_>,
    ) -> bool {
        vectored.install();
        let fd = &self.fd;
        let msg = vectored.msghdr().raw();
        let submitted = Self::push_retry(driver, || Sqe::send_msg(fd, msg, ud));
        if submitted {
            self.send_in_flight = true;
        }
        submitted
    }

    pub(super) fn submit_prepared(
        &mut self,
        driver: &mut DriverContext<'_, 'd>,
        ud: Token,
        prepared: Prepared<'_>,
    ) -> Submit {
        let (payload, consumed, close_after) = prepared.into_parts();
        if close_after {
            self.set_close_after();
        }
        if self.send_in_flight {
            return Submit::Idle(consumed);
        }
        let submitted = match payload {
            Payload::Empty => return Submit::Idle(consumed),
            Payload::Single([]) => return Submit::Idle(consumed),
            Payload::Single(buf) => self.submit_single(driver, ud, buf),
            Payload::Vectored(vectored) if vectored.is_empty() => {
                return Submit::Idle(consumed);
            }
            Payload::Vectored(vectored) => self.submit_vectored(driver, ud, vectored),
        };
        if submitted {
            Submit::Submitted(consumed)
        } else {
            Submit::Rejected(consumed)
        }
    }
}
