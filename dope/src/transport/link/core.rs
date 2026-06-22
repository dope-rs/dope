use crate::transport::wire::Vectored;
use crate::{Drive, Driver, backend};

enum Phase {
    Open,
    Draining,
    Closing,
}

enum RecvArm {
    Disarmed,
    Armed,
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

pub struct Core {
    pub(super) fd: backend::socket::Fd,
    recv: RecvArm,
    phase: Phase,
    send_in_flight: bool,
}

impl Core {
    pub fn new(fd: backend::socket::Fd) -> Self {
        Self {
            fd,
            recv: RecvArm::Disarmed,
            phase: Phase::Open,
            send_in_flight: false,
        }
    }

    pub fn armed(&mut self, pushed: bool) {
        self.recv = if pushed {
            RecvArm::Armed
        } else {
            RecvArm::Exhausted
        };
    }

    pub fn is_armed(&self) -> bool {
        matches!(self.recv, RecvArm::Armed)
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
        self.send_in_flight = false;
    }

    pub fn submit_single(&mut self, ud: backend::token::Token, buf: &[u8], driver: &mut Driver) {
        if driver
            .push(backend::sqe::Sqe::send(&self.fd, buf, ud))
            .is_ok()
        {
            self.send_in_flight = true;
        }
    }

    pub fn submit_vectored(
        &mut self,
        ud: backend::token::Token,
        mut vectored: Vectored<'_>,
        driver: &mut Driver,
    ) {
        vectored.install_into_msghdr();
        if driver
            .push(backend::sqe::Sqe::send_msg(
                &self.fd,
                vectored.msghdr_storage().raw(),
                ud,
            ))
            .is_ok()
        {
            self.send_in_flight = true;
        }
    }
}
