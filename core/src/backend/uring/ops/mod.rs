mod bootstrap;
mod poll;
mod reservation;
mod source;

use crate::driver::{self, schedule};

pub(super) enum Completion {}
pub(super) enum Maintenance {}
pub(super) const REACTOR_LANES: usize = 2;

#[derive(Clone, Copy, PartialEq, Eq)]
enum MaintenanceState {
    Drained,
    Pending,
    Blocked,
}

fn maintain<'d>(
    context: &mut driver::Context<'_, 'd>,
    work: &mut schedule::Budget<'_, 'd, Maintenance>,
) -> MaintenanceState {
    while context.backend().has_maintenance() {
        if !work.take() {
            return MaintenanceState::Pending;
        }
        use crate::backend::uring::MaintenanceStep;
        if matches!(context.backend().maintain_one(), MaintenanceStep::Blocked) {
            return MaintenanceState::Blocked;
        }
    }
    MaintenanceState::Drained
}
