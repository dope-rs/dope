use dope_core::driver::{
    retained,
    schedule::{self, reservation},
};
use dope_net::{
    link::event,
    wire::{self, batch},
};

pub enum Admission<'a, 'c, 'owner, 'd: 'owner, W: wire::Wire> {
    Open {
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
        turn: schedule::Turn<'a, 'd>,
        capacity: batch::Capacity<W>,
    },
    Reserved {
        reservation: reservation::Retained<'a, 'a, 'c, 'owner, 'd>,
        turn: schedule::Turn<'a, 'd>,
        preceding: usize,
        capacity: batch::Capacity<W>,
    },
}

impl<'a, 'c, 'owner, 'd: 'owner, W: wire::Wire> Admission<'a, 'c, 'owner, 'd, W> {
    pub(super) fn open(
        turn: schedule::Turn<'a, 'd>,
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
    ) -> Self {
        Self::Open {
            driver,
            turn,
            capacity: batch::Capacity::full(),
        }
    }

    pub(super) fn reserve(
        turn: schedule::Turn<'a, 'd>,
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
        preceding: usize,
    ) -> Result<Self, &'a mut retained::Context<'c, 'owner, 'd>> {
        let work = turn.application();
        let Some(available) = work.remaining().checked_sub(preceding) else {
            return Err(driver);
        };
        let Some(capacity) = batch::Capacity::fit(available.saturating_add(1)) else {
            return Err(driver);
        };
        let upper = capacity.items().get() - 1;
        let Some(count) = upper.checked_add(preceding) else {
            return Err(driver);
        };
        if count == 0 {
            return Ok(Self::Open {
                driver,
                turn,
                capacity,
            });
        }
        let reservation = reservation::Retained::reserve(work, driver, count)?;
        Ok(Self::Reserved {
            reservation,
            turn,
            preceding,
            capacity,
        })
    }

    pub(super) fn capacity(&self) -> &batch::Capacity<W> {
        match self {
            Self::Open { capacity, .. } | Self::Reserved { capacity, .. } => capacity,
        }
    }

    pub(super) fn commit(
        self,
    ) -> (
        &'a mut retained::Context<'c, 'owner, 'd>,
        schedule::Turn<'a, 'd>,
    ) {
        match self {
            Self::Open { driver, turn, .. } => (driver, turn),
            Self::Reserved {
                reservation,
                turn,
                preceding,
                ..
            } => (reservation.commit(preceding), turn),
        }
    }

    pub(super) fn commit_batch<const ID: u8>(
        self,
        dispatch: &event::DispatchRecv<'_, ID, W::RecvBatch<'_>>,
    ) -> (
        &'a mut retained::Context<'c, 'owner, 'd>,
        schedule::Turn<'a, 'd>,
    ) {
        match self {
            Self::Open { driver, turn, .. } => (driver, turn),
            Self::Reserved {
                reservation,
                turn,
                preceding,
                ..
            } => {
                let additional = match dispatch {
                    event::DispatchRecv::Chunk(_, batch) => batch.len() - 1,
                    _ => 0,
                };
                (reservation.commit(preceding + additional), turn)
            }
        }
    }
}
