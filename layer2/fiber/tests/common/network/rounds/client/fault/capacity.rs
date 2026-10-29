use std::ops::ControlFlow;

use dope_test::harness::signals;

use crate::common::network::rounds::client::fault::read;

pub const BUFFER_CAP: usize = 1 << 13;
pub const TERMINAL_AFTER_BUFFER_CAP: usize = BUFFER_CAP + 1;

pub struct State {
    bytes: usize,
    seen: signals::Gate,
}

impl read::Observe for State {
    type Binding = signals::Gate;

    fn new(seen: &Self::Binding) -> Self {
        Self {
            bytes: 0,
            seen: seen.clone(),
        }
    }

    fn observe(&mut self, bytes: usize) -> ControlFlow<()> {
        self.bytes = self.bytes.saturating_add(bytes);
        if self.bytes >= TERMINAL_AFTER_BUFFER_CAP {
            self.seen.hit();
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }
}
