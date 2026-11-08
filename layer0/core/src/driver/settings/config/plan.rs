use std::{mem, num};

use crate::driver::{settings, settings::errors};

/// Proven capacity of the shared descriptor and dynamic-ready arena.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Ready(num::NonZero<u32>);

const _: () = {
    assert!(mem::size_of::<Ready>() == mem::size_of::<u32>());
    assert!(mem::align_of::<Ready>() == mem::align_of::<u32>());
};

impl Ready {
    const fn new(capacity: num::NonZero<u32>) -> Self {
        Self(capacity)
    }

    pub(crate) const fn raw(self) -> u32 {
        self.0.get()
    }
}

/// Cross-field validated driver construction plan.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Plan {
    config: settings::Config,
    ready: Ready,
}

impl Plan {
    pub(super) const fn new(config: settings::Config) -> Result<Self, errors::Config> {
        let files = config.file_slots();
        let slots = config.schedule().ready();
        let files = files.capacity().raw();
        let ready = slots.raw();
        let count = files as u64 + ready as u64;
        if count > u32::MAX as u64 {
            Err(errors::Config::ReadyCapacity {
                files,
                ready,
                maximum: u32::MAX,
            })
        } else {
            Ok(Self {
                config,
                ready: Ready::new(num::NonZero::<u32>::MIN.saturating_add(count as u32 - 1)),
            })
        }
    }

    pub(crate) const fn config(&self) -> &settings::Config {
        &self.config
    }

    pub(crate) const fn ready(self) -> Ready {
        self.ready
    }
}
