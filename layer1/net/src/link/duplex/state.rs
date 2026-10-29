use std::{cell, mem};

use crate::link::slot::types::{CloseGate, Readiness};

#[derive(Clone, Copy)]
enum Phase {
    Open,
    Draining,
    Closing,
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum Fact {
    Discarded = 1 << 0,
    GracefulRequested = 1 << 1,
    GracefulSealed = 1 << 2,
    SendCancelRejected = 1 << 3,
    SendCancelRequested = 1 << 4,
    WriteShutdownRequested = 1 << 5,
    WriteShutdownSubmitted = 1 << 6,
    WriteShutdownComplete = 1 << 7,
}

#[repr(transparent)]
struct Flags(u8);

pub(in crate::link) struct State {
    phase: cell::Cell<Phase>,
    pub(in crate::link) close: Close,
    waiting: bool,
}

impl State {
    pub(super) fn new() -> Self {
        Self {
            phase: cell::Cell::new(Phase::Open),
            close: Close::new(),
            waiting: false,
        }
    }

    pub(in crate::link) fn discard(&mut self) {
        self.waiting = false;
        self.close.flags.set(Fact::Discarded);
        self.phase.set(Phase::Closing);
    }

    pub(in crate::link) fn is_waiting(&self) -> bool {
        self.waiting
    }

    pub(in crate::link) fn wait_send(&mut self, waiting: bool) {
        self.waiting = waiting;
    }

    pub(in crate::link) fn is_closing(&self) -> bool {
        matches!(self.phase.get(), Phase::Closing)
    }

    pub(in crate::link) fn close(&self) {
        self.phase.set(Phase::Closing);
    }

    pub(in crate::link) fn is_draining(&self) -> bool {
        matches!(self.phase.get(), Phase::Draining)
    }

    pub(in crate::link) fn drain(&mut self) {
        if matches!(self.phase.get(), Phase::Open) {
            self.phase.set(Phase::Draining);
        }
    }

    pub(in crate::link) fn close_due(&self, sending: bool, gate: CloseGate) -> Readiness {
        if sending {
            return Readiness::Waiting;
        }
        match self.phase.get() {
            Phase::Open => Readiness::Waiting,
            Phase::Draining => match gate {
                CloseGate::Immediate => Readiness::Due,
                CloseGate::Deferred => Readiness::Waiting,
            },
            Phase::Closing => Readiness::Due,
        }
    }
}

pub(in crate::link) struct Close {
    flags: Flags,
    pub(in crate::link) owner: Ownership,
}

pub(in crate::link) enum Claim {
    Ready(Authority),
    Unavailable,
}

pub(in crate::link) enum Ownership {
    Held,
    Released,
    Claimed,
}

#[must_use = "claimed owner authority must retire the slot or be restored"]
pub(in crate::link) struct Authority(());

#[must_use]
pub(in crate::link) enum Request {
    Ready,
    Discarded,
    Duplicate,
}

#[must_use]
pub(in crate::link) enum Seal {
    Ready,
    Missing,
    Duplicate,
}

#[must_use]
pub(in crate::link) enum Shutdown {
    Pending,
    Ready,
    Submitted,
    Complete,
}

impl Close {
    const fn new() -> Self {
        Self {
            flags: Flags::EMPTY,
            owner: Ownership::Held,
        }
    }

    pub(in crate::link) fn is_discarded(&self) -> bool {
        self.flags.contains(Fact::Discarded)
    }

    pub(in crate::link) fn can_cancel(&self) -> bool {
        self.flags.contains(Fact::Discarded)
            && !self.flags.contains(Fact::SendCancelRequested)
            && !self.flags.contains(Fact::SendCancelRejected)
    }

    pub(in crate::link) fn deny_cancel(&mut self) {
        self.flags.set(Fact::SendCancelRejected);
    }

    pub(in crate::link) fn mark_cancel(&mut self) {
        self.flags.set(Fact::SendCancelRequested);
    }

    pub(in crate::link) fn request(&mut self) -> Request {
        if self.flags.contains(Fact::Discarded) {
            return Request::Discarded;
        }
        if self.flags.contains(Fact::GracefulRequested) {
            return Request::Duplicate;
        }
        self.flags.set(Fact::GracefulRequested);
        Request::Ready
    }

    pub(in crate::link) fn check_seal(&self) -> Seal {
        if !self.flags.contains(Fact::GracefulRequested) {
            return Seal::Missing;
        }
        if self.flags.contains(Fact::GracefulSealed) {
            return Seal::Duplicate;
        }
        Seal::Ready
    }

    pub(in crate::link) fn seal(&mut self) {
        self.flags.set(Fact::GracefulSealed);
    }

    pub(in crate::link) fn shutdown(&mut self) {
        if self.flags.contains(Fact::Discarded) || self.flags.contains(Fact::WriteShutdownRequested)
        {
            return;
        }
        self.flags.set(Fact::WriteShutdownRequested);
    }

    pub(in crate::link) fn is_shutting(&self) -> bool {
        self.flags.contains(Fact::WriteShutdownRequested)
            && !self.flags.contains(Fact::WriteShutdownComplete)
    }

    pub(in crate::link) fn has_shutdown(&self) -> bool {
        self.flags.contains(Fact::WriteShutdownRequested)
    }

    pub(in crate::link) fn take_write(&self) -> Shutdown {
        if !self.flags.contains(Fact::WriteShutdownRequested)
            || !self.flags.contains(Fact::GracefulSealed)
        {
            return Shutdown::Pending;
        }
        if self.flags.contains(Fact::WriteShutdownComplete) {
            return Shutdown::Complete;
        }
        if self.flags.contains(Fact::WriteShutdownSubmitted) {
            return Shutdown::Submitted;
        }
        Shutdown::Ready
    }

    pub(in crate::link) fn arm_write(&mut self) {
        self.flags.set(Fact::WriteShutdownSubmitted);
    }

    pub(in crate::link) fn finish_write(&mut self) {
        self.flags.set(Fact::WriteShutdownComplete);
    }
}

impl Flags {
    const EMPTY: Self = Self(0);

    fn contains(&self, fact: Fact) -> bool {
        self.0 & fact as u8 != 0
    }

    fn set(&mut self, fact: Fact) {
        self.0 |= fact as u8;
    }
}

impl Ownership {
    pub(in crate::link) fn release(&mut self) {
        if matches!(self, Ownership::Held) {
            *self = Ownership::Released;
        }
    }

    pub(in crate::link) fn claim(&mut self) -> Claim {
        match mem::replace(self, Ownership::Claimed) {
            Ownership::Released => Claim::Ready(Authority(())),
            state => {
                *self = state;
                Claim::Unavailable
            }
        }
    }

    pub(in crate::link) fn restore(&mut self, proof: Authority) {
        let Authority(()) = proof;
        *self = Ownership::Released;
    }
}

const _: () = {
    use std::mem;

    assert!(mem::size_of::<Close>() == mem::size_of::<Flags>() + mem::size_of::<Ownership>());
    assert!(mem::align_of::<Close>() == mem::align_of::<u8>());
    assert!(
        mem::size_of::<State>()
            == mem::size_of::<cell::Cell<Phase>>()
                + mem::size_of::<Close>()
                + mem::size_of::<bool>()
    );
    assert!(mem::align_of::<State>() == mem::align_of::<u8>());
    assert!(mem::size_of::<Request>() == 1);
    assert!(mem::size_of::<Seal>() == 1);
    assert!(mem::size_of::<Shutdown>() == 1);
};
