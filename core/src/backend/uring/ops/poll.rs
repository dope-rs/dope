use std::{io, time};

use crate::driver::{
    self,
    ops::{poll, reactors},
    schedule,
};

fn prepare<'d>(
    context: &mut driver::Context<'_, 'd>,
    work: schedule::Reactor<'_, 'd>,
) -> super::MaintenanceState {
    let work = reactors::Backend::prepare(context, work);
    let limit = work.remaining();
    let mut budget = work.budget::<super::Maintenance>(limit);
    super::maintain(context, &mut budget)
}

impl<'d> poll::Poll<'d> for driver::Context<'_, 'd> {
    fn commit(&mut self, work: schedule::Reactor<'_, 'd>) -> io::Result<poll::Commit> {
        let backend = prepare(self, work);
        let pending = backend != super::MaintenanceState::Drained
            || self.driver_ref().maintenance().has_deferred_maintenance();
        self.backend().ring.commit()?;
        Ok(if pending {
            poll::Commit::Pending
        } else {
            poll::Commit::Drained
        })
    }

    fn wait(
        &mut self,
        work: schedule::Reactor<'_, 'd>,
        timeout: Option<time::Duration>,
    ) -> io::Result<()> {
        let backend_pending = prepare(self, work) != super::MaintenanceState::Drained;
        let must_not_block = backend_pending
            || self.driver_ref().maintenance().has_deferred_maintenance()
            || self.driver_ref().ready().has_ready();
        let nonblocking = must_not_block || timeout.is_some_and(|timeout| timeout.is_zero());
        let backend = self.backend();
        if nonblocking {
            backend.ring.commit()
        } else {
            backend.ring.wait(timeout)
        }
    }
}
