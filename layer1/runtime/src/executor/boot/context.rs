use std::mem;

use dope_core::driver::schedule::{app, clock, ready};

use crate::executor::{boot, root};

/// One startup-root poll with only its root wake, turn budget, read-only clock,
/// and exact poll cause.
pub struct Context<'poll, 'd: 'poll> {
    wake: ready::Target<'d>,
    work: app::Work<'poll, 'd>,
    clock: clock::Clock<'poll, 'd>,
    cause: boot::Cause,
}

impl<'poll, 'd: 'poll> Context<'poll, 'd> {
    pub(crate) fn new(
        wake: ready::Target<'d>,
        work: app::Work<'poll, 'd>,
        clock: clock::Clock<'poll, 'd>,
        cause: boot::Cause,
    ) -> Self {
        Self {
            wake,
            work,
            clock,
            cause,
        }
    }

    pub fn into_parts(
        self,
    ) -> (
        ready::Target<'d>,
        app::Work<'poll, 'd>,
        clock::Clock<'poll, 'd>,
        boot::Cause,
    ) {
        (self.wake, self.work, self.clock, self.cause)
    }

    pub fn into_poll(self) -> (root::Budget<'poll, 'd>, clock::Clock<'poll, 'd>) {
        (root::Budget::new(self.wake, self.work), self.clock)
    }
}

const fn assert_layout() {
    assert!(mem::size_of::<Context<'static, 'static>>() == 5 * mem::size_of::<usize>());
    assert!(mem::align_of::<Context<'static, 'static>>() == mem::align_of::<usize>());
}

const _: () = assert_layout();
