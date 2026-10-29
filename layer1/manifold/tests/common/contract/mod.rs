pub mod batch;
pub mod codec;

use std::cell::Cell;

use dope_test::harness::observe::Trace;

/// Exact failures and terminal send counts observed from one application.
pub struct Report<E, const N: usize> {
    pub(crate) faults: Trace<E, N>,
    pub(crate) sent: Cell<Option<usize>>,
    pub(crate) cancelled: Cell<Option<usize>>,
}

impl<E, const N: usize> Default for Report<E, N> {
    fn default() -> Self {
        Self {
            faults: Trace::default(),
            sent: Cell::new(Some(0)),
            cancelled: Cell::new(Some(0)),
        }
    }
}

impl<E, const N: usize> Report<E, N> {
    pub fn record_sent(&self) {
        self.sent
            .set(self.sent.get().and_then(|count| count.checked_add(1)));
    }

    pub fn cancelled(&self) {
        self.cancelled
            .set(self.cancelled.get().and_then(|count| count.checked_add(1)));
    }
}
