use crate::transport::wire::Vectored;
use crate::{Drive, Driver, backend};

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
    Connecting(backend::socket::Addr),
    Done,
}

impl Establish {
    pub fn begin(&mut self, addr: backend::socket::Addr) -> (*const libc::sockaddr, u32) {
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

pub enum RecvError {
    Closed,
    Live { needs_rearm: bool },
}

pub struct Core<'d> {
    pub(super) fd: backend::socket::Fd<'d>,
    recv: RecvArm,
    phase: Phase,
    send_in_flight: bool,
    aborted: bool,
    graceful_sealed: bool,
    kernel_discard: bool,
    discard_remaining: u64,
}

impl<'d> Core<'d> {
    pub fn new(fd: backend::socket::Fd<'d>, kernel_discard: bool) -> Self {
        Self {
            fd,
            recv: RecvArm::Disarmed,
            phase: Phase::Open,
            send_in_flight: false,
            aborted: false,
            graceful_sealed: false,
            kernel_discard,
            discard_remaining: 0,
        }
    }

    pub fn mark_aborted(&mut self) {
        self.aborted = true;
    }

    pub fn is_aborted(&self) -> bool {
        self.aborted
    }

    pub fn seal_graceful(&mut self) -> bool {
        if self.aborted || self.graceful_sealed {
            return false;
        }
        self.graceful_sealed = true;
        true
    }

    pub fn armed(&mut self, pushed: bool, discard: bool) {
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

    pub(crate) fn kernel_discard(&self) -> bool {
        self.kernel_discard
    }

    pub(crate) fn begin_discard(&mut self, n: u64) {
        self.discard_remaining = self.discard_remaining.saturating_add(n);
    }

    pub(crate) fn discard_remaining(&self) -> u64 {
        self.discard_remaining
    }

    pub(super) fn is_discard_armed(&self) -> bool {
        matches!(self.recv, RecvArm::Armed { discard: true })
    }

    pub(crate) fn recv_cancel_kind(&self) -> u8 {
        if self.is_discard_armed() {
            backend::token::kind::RECV_DISCARD
        } else {
            backend::token::kind::RECV
        }
    }

    /// Returns how many of `len` bytes the discard budget swallowed.
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

    pub fn recv_cancelled(&mut self, more: bool) -> RecvError {
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

    pub fn send_done(&mut self) {
        if self.send_in_flight {
            crate::memstats::send_release();
        }
        self.send_in_flight = false;
    }

    pub(super) fn push_retry(
        driver: &'d Driver,
        mut build: impl FnMut() -> backend::sqe::Sqe,
    ) -> bool {
        if driver.push(build()).is_ok() {
            return true;
        }
        if driver.submit_to_drain() {
            return driver.push(build()).is_ok();
        }
        false
    }

    pub fn submit_single(&mut self, ud: backend::token::Token, buf: &[u8], driver: &'d Driver) {
        let fd = &self.fd;
        if Self::push_retry(driver, || backend::sqe::Sqe::send(fd, buf, ud)) {
            crate::memstats::send_borrow();
            self.send_in_flight = true;
        }
    }

    pub fn submit_vectored(
        &mut self,
        ud: backend::token::Token,
        mut vectored: Vectored<'_>,
        driver: &'d Driver,
    ) {
        vectored.install_into_msghdr();
        let fd = &self.fd;
        let msg = vectored.msghdr_storage().raw();
        if Self::push_retry(driver, || backend::sqe::Sqe::send_msg(fd, msg, ud)) {
            crate::memstats::send_borrow();
            self.send_in_flight = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DriverConfig;
    use crate::backend::profile::Production;
    use crate::backend::token::{Epoch, LocalIdx, Token};

    fn driver() -> Driver {
        let cfg = <crate::DriverCfg as DriverConfig>::for_tcp_profile::<Production>(1);
        Driver::new(cfg).expect("driver")
    }

    fn token() -> Token {
        Token::new(1, LocalIdx::new(0), Epoch::ZERO.bump())
    }

    #[test]
    fn submit_single_happy_path_marks_inflight_once() {
        let drv = driver();
        let fd = backend::socket::Fd::adopt(backend::socket::FdSlot::new(0), &drv);
        let mut core = Core::new(fd, false);
        assert!(!core.is_send_inflight());
        core.submit_single(token(), b"hello", &drv);
        assert!(
            core.is_send_inflight(),
            "happy path must queue the send and mark inflight"
        );
        core.send_done();
        assert!(!core.is_send_inflight());
    }
}
