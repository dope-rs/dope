const CONTINUATION: u8 = 1 << 0;
const REQUEST_DRAIN: u8 = 1 << 1;

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

    pub(in crate::client::conn) fn take_drain(&mut self) -> bool {
        let taken = self.0 & REQUEST_DRAIN != 0;
        self.0 &= !REQUEST_DRAIN;
        taken
    }
}
