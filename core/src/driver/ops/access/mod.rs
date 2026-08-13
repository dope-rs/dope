use std::io;

use crate::driver::settings;

mod credits;
mod files;
mod maintenance;
mod outbound;
mod ready;
mod receive;
mod scheduler;

pub(in crate::driver) use credits::Credits;
pub use files::Files;
pub use maintenance::Maintenance;
pub use outbound::Outbound;
pub use ready::Ready;
pub use receive::Receive;
pub use scheduler::Scheduler;

pub(in crate::driver) struct Shared {
    scheduling: scheduler::State,
    receive: receive::State,
    maintenance: maintenance::State,
    files: files::State,
}

impl Shared {
    pub(in crate::driver) fn try_new(
        file_slots: settings::FileSlots,
        dynamic_slots: settings::ScheduleCapacity,
        receive: settings::Receive,
    ) -> io::Result<Self> {
        Ok(Self {
            scheduling: scheduler::State::try_new(file_slots, dynamic_slots)?,
            receive: receive::State::try_new(receive)?,
            maintenance: maintenance::State::try_new(file_slots)?,
            files: files::State::try_new(file_slots)?,
        })
    }
}
