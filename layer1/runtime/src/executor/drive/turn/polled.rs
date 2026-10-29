use dope_core::driver::result::poll;
use turn::{modes, scope::Core};

use crate::executor::drive::turn;

#[must_use]
pub(crate) struct Polled<'turn, 'd, M> {
    core: Core<'turn, 'd, M>,
    events: poll::Drain,
    status: turn::Status,
}

impl<'turn, 'd, M> Polled<'turn, 'd, M>
where
    M: modes::Mode<'d>,
{
    pub(super) const fn new(
        core: Core<'turn, 'd, M>,
        events: poll::Drain,
        status: turn::Status,
    ) -> Self {
        Self {
            core,
            events,
            status,
        }
    }

    pub(crate) fn park(mut self) -> turn::Parked<'turn, 'd, M> {
        M::park(&mut self.core);
        turn::Parked::new(self.core, self.events, self.status)
    }
}
