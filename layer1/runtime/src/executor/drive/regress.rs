use std::time;

#[derive(Eq, PartialEq, Debug)]
pub struct Regression {
    requested: time::Instant,
    current: time::Instant,
}

impl Regression {
    pub(super) const fn new(ask: time::Instant, current: time::Instant) -> Self {
        Self {
            requested: ask,
            current,
        }
    }

    pub const fn requested(&self) -> time::Instant {
        self.requested
    }

    pub const fn current(&self) -> time::Instant {
        self.current
    }
}
