use std::cell;

pub(in crate::link::egress::queue) struct Counters {
    partial_sent: cell::Cell<usize>,
    submitted_plain: cell::Cell<usize>,
}

impl Counters {
    pub(in crate::link::egress::queue) fn new() -> Self {
        use std::cell::Cell;

        Self {
            partial_sent: Cell::new(0),
            submitted_plain: Cell::new(0),
        }
    }

    pub(in crate::link::egress::queue) fn partial(&self) -> usize {
        self.partial_sent.get()
    }

    pub(in crate::link::egress::queue) fn set_partial(&self, bytes: usize) {
        self.partial_sent.set(bytes);
    }

    pub(in crate::link::egress::queue) fn submitted(&self) -> usize {
        self.submitted_plain.get()
    }

    pub(in crate::link::egress::queue) fn set_submitted(&self, bytes: usize) {
        self.submitted_plain.set(bytes);
    }

    pub(in crate::link::egress::queue) fn take_submitted(&self) -> usize {
        self.submitted_plain.take()
    }
}
