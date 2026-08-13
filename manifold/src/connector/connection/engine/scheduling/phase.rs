use dope_core::driver::route::table;
use o3::collections;

use crate::connector::connection::engine::scheduling::book;

pub(in crate::connector) struct Schedule<'d, const ID: u8> {
    pub(in crate::connector) deadlines: book::DeadlineBook<'d, ID>,
    pub(in crate::connector) shutdown: Shutdown,
    phase: Phase,
}

pub(in crate::connector) enum Shutdown {
    Open,
    Closing(usize),
    Done,
}

#[derive(Clone, Copy)]
pub(in crate::connector) enum Phase {
    Dirty,
    Cancellations,
    Submission,
    Liveness,
}

impl Phase {
    pub(in crate::connector) const COUNT: usize = 4;

    const fn next(self) -> Self {
        match self {
            Self::Dirty => Self::Cancellations,
            Self::Cancellations => Self::Submission,
            Self::Submission => Self::Liveness,
            Self::Liveness => Self::Dirty,
        }
    }
}

impl<'d, const ID: u8> Schedule<'d, ID> {
    pub(in crate::connector) fn try_new(
        capacity: table::Capacity,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            deadlines: book::DeadlineBook::try_new(capacity.get())?,
            shutdown: Shutdown::Open,
            phase: Phase::Dirty,
        })
    }

    pub(in crate::connector) fn next_phase(&mut self) -> Phase {
        let phase = self.phase;
        self.phase = phase.next();
        phase
    }
}
