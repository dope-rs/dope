const CONTINUATION: u8 = 1 << 0;
const REQUEST_DRAIN: u8 = 1 << 1;
const REQUEST_PENDING: u8 = 1 << 2;
const RESUME_FIRST: u8 = 1 << 3;

#[repr(transparent)]
pub(in crate::client::conn) struct Work(u8);

impl Work {
    pub(super) const fn new() -> Self {
        Self(0)
    }

    pub(in crate::client::conn) const fn has_next(&self) -> bool {
        self.0 & CONTINUATION != 0
    }

    pub(in crate::client::conn) fn request_next(&mut self) {
        self.0 |= CONTINUATION;
    }

    pub(in crate::client::conn) fn clear_next(&mut self) {
        self.0 &= !CONTINUATION;
    }

    pub(in crate::client::conn) fn drain(&mut self) {
        self.0 |= REQUEST_DRAIN;
    }

    pub(in crate::client::conn) const fn has_drain(&self) -> bool {
        self.0 & REQUEST_DRAIN != 0
    }

    pub(in crate::client::conn) const fn has_requests(&self) -> bool {
        self.0 & REQUEST_PENDING != 0
    }

    pub(in crate::client::conn) const fn resume_first(&self) -> bool {
        self.0 & (CONTINUATION | RESUME_FIRST) == CONTINUATION | RESUME_FIRST
    }

    pub(in crate::client::conn) fn drained(&mut self, pending: bool, served: bool) {
        self.0 &= !(REQUEST_DRAIN | REQUEST_PENDING);
        if pending {
            self.0 |= REQUEST_PENDING;
        }
        if served {
            self.0 |= RESUME_FIRST;
        }
    }

    pub(in crate::client::conn) fn resumed(&mut self) {
        self.0 &= !RESUME_FIRST;
    }
}
