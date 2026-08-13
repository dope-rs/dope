enum Phase {
    Open,
    Draining,
    Closing,
}

const ABORTED: u8 = 1 << 0;
const GRACEFUL_REQUESTED: u8 = 1 << 1;
const GRACEFUL_SEALED: u8 = 1 << 2;
const OWNER_RELEASED: u8 = 1 << 3;
const SEND_CANCEL_REQUESTED: u8 = 1 << 4;

pub(in crate::link) struct Lifecycle {
    phase: Phase,
    flags: u8,
}

impl Lifecycle {
    pub(super) fn new() -> Self {
        Self {
            phase: Phase::Open,
            flags: 0,
        }
    }

    pub(in crate::link) fn abort(&mut self) {
        self.set_flag(ABORTED);
        self.phase = Phase::Closing;
    }

    pub(in crate::link) fn is_aborted(&self) -> bool {
        self.has_flag(ABORTED)
    }

    pub(in crate::link) fn send_cancel_requested(&self) -> bool {
        self.has_flag(SEND_CANCEL_REQUESTED)
    }

    pub(in crate::link) fn mark_send_cancel_requested(&mut self) {
        self.set_flag(SEND_CANCEL_REQUESTED);
    }

    pub(in crate::link) fn request_graceful(&mut self) -> bool {
        if self.has_flag(ABORTED | GRACEFUL_REQUESTED) {
            return false;
        }
        self.set_flag(GRACEFUL_REQUESTED);
        true
    }

    pub(in crate::link) fn take_graceful(&mut self, send_inflight: bool) -> bool {
        if send_inflight || !self.has_flag(GRACEFUL_REQUESTED) || self.has_flag(GRACEFUL_SEALED) {
            return false;
        }
        self.set_flag(GRACEFUL_SEALED);
        true
    }

    pub(in crate::link) fn is_closing(&self) -> bool {
        matches!(self.phase, Phase::Closing)
    }

    pub(in crate::link) fn begin_close(&mut self) {
        self.phase = Phase::Closing;
    }

    pub(in crate::link) fn owner_released(&self) -> bool {
        self.has_flag(OWNER_RELEASED)
    }

    pub(in crate::link) fn release_owner(&mut self) {
        self.set_flag(OWNER_RELEASED);
    }

    pub(in crate::link) fn close_after(&self) -> bool {
        matches!(self.phase, Phase::Draining)
    }

    pub(in crate::link) fn set_close_after(&mut self) {
        if matches!(self.phase, Phase::Open) {
            self.phase = Phase::Draining;
        }
    }

    pub(in crate::link) fn should_close(&self, send_inflight: bool, defer: bool) -> bool {
        if send_inflight {
            return false;
        }
        match self.phase {
            Phase::Open => false,
            Phase::Draining => !defer,
            Phase::Closing => true,
        }
    }

    fn has_flag(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }

    fn set_flag(&mut self, flag: u8) {
        self.flags |= flag;
    }
}
