use crate::wire::send;

const CLOSE_AFTER: u8 = 1;
const RELEASED: u8 = 1 << 1;
const WAITING: u8 = 1 << 2;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(super) struct Control(u8);

impl Control {
    pub(super) const fn new() -> Self {
        Self(0)
    }

    pub(super) const fn end(self) -> send::End {
        if self.0 & CLOSE_AFTER == 0 {
            send::End::Keep
        } else {
            send::End::Close
        }
    }

    pub(super) const fn availability(self) -> send::Availability {
        if self.0 & RELEASED == 0 {
            send::Availability::Unchanged
        } else {
            send::Availability::Released
        }
    }

    pub(super) fn mark_close(&mut self) {
        self.0 |= CLOSE_AFTER;
    }

    pub(super) const fn is_waiting(self) -> bool {
        self.0 & WAITING != 0
    }

    pub(super) fn mark_waiting(&mut self) {
        self.0 |= WAITING;
    }

    pub(super) fn mark_free(&mut self, free: send::Availability) {
        match free {
            send::Availability::Unchanged => {}
            send::Availability::Released => self.0 |= RELEASED,
        }
    }
}
