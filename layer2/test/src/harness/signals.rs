use std::{cell::Cell, rc};

/// Monotonically increasing progress observed by a test driver.
pub trait Progress {
    fn hits(&self) -> u32;
}

#[derive(Clone, Default)]
pub struct Gate {
    state: rc::Rc<Cell<u32>>,
}

impl Gate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn hit(&self) {
        self.state.set(self.state.get().saturating_add(1));
    }
}

impl Progress for Gate {
    fn hits(&self) -> u32 {
        self.state.get()
    }
}

/// Stack-owned gate storage. Obtain a copyable handle with [`Self::handle`].
#[derive(Default)]
pub struct LocalGate {
    hits: Cell<u32>,
}

impl LocalGate {
    /// Creates empty stack-owned gate storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Borrows a copyable signal handle from gate storage.
    pub fn handle(&self) -> LocalGateRef<'_> {
        LocalGateRef(self)
    }
}

/// Copyable borrow of one stack-owned [`LocalGate`].
#[derive(Clone, Copy)]
pub struct LocalGateRef<'a>(&'a LocalGate);

impl LocalGateRef<'_> {
    /// Records one hit, saturating at `u32::MAX`.
    pub fn hit(self) {
        let hits = &self.0.hits;
        hits.set(hits.get().saturating_add(1));
    }
}

impl Progress for LocalGateRef<'_> {
    fn hits(&self) -> u32 {
        self.0.hits.get()
    }
}
