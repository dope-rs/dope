use std::mem;

use dope_core::{driver, driver::result::poll};
use turn::{modes, scope::Core};

use crate::executor::drive::turn;

#[must_use]
pub(crate) enum Status {
    Active,
    Shutdown,
}

#[must_use]
pub(crate) struct Parked<'turn, 'd, M> {
    core: Core<'turn, 'd, M>,
    events: poll::Drain,
    status: Status,
}

impl<'turn, 'd, M> Parked<'turn, 'd, M>
where
    M: modes::Mode<'d>,
{
    pub(super) const fn new(core: Core<'turn, 'd, M>, events: poll::Drain, status: Status) -> Self {
        Self {
            core,
            events,
            status,
        }
    }

    pub(crate) const fn status(&self) -> &Status {
        &self.status
    }

    pub(crate) fn sync(mut self) -> Result<Option<turn::Idle<'turn, 'd, M>>, poll::Error> {
        if !self.core.pending.is_none() {
            return Ok(None);
        }
        if matches!(self.events, poll::Drain::Pending) {
            let _ = driver::Context::commit(&mut self.core.driver, self.core.turn.reactor())?;
            return Ok(None);
        }
        Ok(Some(turn::Idle::new(self.core)))
    }
}

const _: () = {
    assert!(mem::size_of::<Status>() == mem::size_of::<bool>());
    assert!(mem::align_of::<Status>() == mem::align_of::<bool>());
    assert!(
        mem::size_of::<self::Parked<'_, '_, modes::Boot>>()
            == mem::size_of::<turn::Polled<'_, '_, modes::Boot>>()
    );
    assert!(
        mem::align_of::<self::Parked<'_, '_, modes::Boot>>()
            == mem::align_of::<turn::Polled<'_, '_, modes::Boot>>()
    );
};
