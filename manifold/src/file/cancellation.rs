use std::cell;

pub(in crate::file) struct Cancellation(cell::Cell<bool>);

impl Cancellation {
    pub(in crate::file) const fn new() -> Self {
        Self(cell::Cell::new(false))
    }

    pub(in crate::file) fn mark(&self) {
        self.0.set(true);
    }

    pub(in crate::file) fn is_pending(&self) -> bool {
        self.0.get()
    }

    pub(in crate::file) fn clear(&self) {
        self.0.set(false);
    }
}
