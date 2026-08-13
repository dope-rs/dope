use std::cell;

pub(super) struct Free {
    next: cell::Cell<u32>,
}

impl Free {
    pub(super) fn new(next: u32) -> Self {
        use std::cell::Cell;

        Self {
            next: Cell::new(next),
        }
    }

    pub(super) fn get(&self) -> u32 {
        self.next.get()
    }

    pub(super) fn set(&self, next: u32) {
        self.next.set(next);
    }

    pub(super) fn replace(&self, next: u32) -> u32 {
        self.next.replace(next)
    }
}
