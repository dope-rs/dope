use std::{cell::Cell, num::NonZero};

/// Observes bounded settlement order across a lifecycle stop boundary.
#[derive(Debug)]
pub struct Sequence {
    pub(crate) received: Cell<usize>,
    pub(crate) stopped: Cell<bool>,
    pub(crate) settled: Cell<usize>,
    pub(crate) seen: Box<[Cell<usize>]>,
    pub(crate) last: Cell<usize>,
    pub(crate) forced: Cell<bool>,
}

impl Sequence {
    pub fn new(capacity: NonZero<usize>) -> Self {
        Self {
            received: Cell::new(0),
            stopped: Cell::new(false),
            settled: Cell::new(0),
            seen: (0..capacity.get()).map(|_| Cell::new(0)).collect(),
            last: Cell::new(usize::MAX),
            forced: Cell::new(false),
        }
    }

    pub fn receive(&self) {
        self.received.set(self.received.get() + 1);
    }

    pub fn stop(&self) {
        self.stopped.set(true);
    }

    pub fn record(&self, index: usize, forced: bool) -> Result<(), usize> {
        let count = self.seen.get(index).ok_or(index)?;
        count.set(count.get() + 1);
        if self.stopped.get() {
            let previous = self.last.replace(index);
            if previous != usize::MAX && index < previous {
                self.forced.set(forced);
            }
        }
        self.settled.set(self.settled.get() + 1);
        Ok(())
    }
}
