use crate::{
    backend::{self, fixed},
    driver, io, platform,
};

pub(super) struct Reclaim<'a, 'd> {
    backend: &'a mut backend::Uring,
    driver: driver::Reference<'d>,
}

impl<'a, 'd> Reclaim<'a, 'd> {
    pub(super) fn new(backend: &'a mut backend::Uring, driver: driver::Reference<'d>) -> Self {
        Self { backend, driver }
    }

    pub(super) fn apply(self, completion: io::Completion) {
        let Self { backend, driver } = self;
        match completion.into_reclaim(driver) {
            io::Reclaim::Accepted(accepted) => fixed::Lifecycle::close(
                backend,
                driver::Close::untracked(accepted.into_slot()),
                driver,
                fixed::Phase::Final,
            ),
            io::Reclaim::Close(close) => {
                fixed::Lifecycle::close(backend, close, driver, fixed::Phase::Final)
            }
            io::Reclaim::Slots(retired) => {
                let slots = driver.outbound().take_retired_slots(retired);
                fixed::Lifecycle::release_slots(backend, slots);
            }
            io::Reclaim::Buffer(receive) => platform::Buffer::release(backend, receive),
            io::Reclaim::None => {}
        }
    }
}
