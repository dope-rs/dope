use std::{mem, num};

use crate::driver::{settings, settings::errors};

/// Proven capacity of the shared descriptor and dynamic-ready arena.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Ready(num::NonZero<u16>);

const _: () = {
    assert!(mem::size_of::<Ready>() == mem::size_of::<u16>());
    assert!(mem::align_of::<Ready>() == mem::align_of::<u16>());
};

impl Ready {
    const fn new(capacity: num::NonZero<u16>) -> Self {
        Self(capacity)
    }

    pub(crate) const fn into_nonzero(self) -> num::NonZero<u16> {
        self.0
    }

    pub(crate) const fn raw(self) -> u16 {
        self.0.get()
    }
}

/// Cross-field validated driver construction plan.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Plan {
    config: settings::Config,
    ready: Ready,
    ready_slots: u16,
}

impl Plan {
    pub(super) const fn new(config: settings::Config) -> Result<Self, errors::Config> {
        let files = config.file_slots();
        let slots = config.schedule().ready();
        let files = files.capacity().raw();
        let ready = slots.raw();
        let count = files as u64 + ready as u64;
        if count > u16::MAX as u64 {
            Err(errors::Config::ReadyCapacity {
                files,
                ready,
                maximum: u16::MAX as u32,
            })
        } else {
            Ok(Self {
                config,
                ready: Ready::new(num::NonZero::<u16>::MIN.saturating_add(count as u16 - 1)),
                ready_slots: ready as u16,
            })
        }
    }

    pub(crate) const fn config(&self) -> &settings::Config {
        &self.config
    }

    pub(crate) const fn ready(self) -> Ready {
        self.ready
    }

    pub(crate) const fn ready_slots(self) -> u16 {
        self.ready_slots
    }
}
