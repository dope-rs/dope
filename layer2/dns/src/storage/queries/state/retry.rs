use std::{num, time};

use o3::base::time::Positive;

use crate::config;

#[derive(Clone, Copy)]
#[repr(C)]
pub(in crate::storage::queries) struct Retry {
    pub(in crate::storage::queries::state) attempts: num::NonZero<u8>,
    timeout: Positive,
}

impl Retry {
    pub(in crate::storage::queries) const fn from_config(config: &config::Validated) -> Self {
        Self {
            attempts: config.query.attempts,
            timeout: config.query.attempt_timeout,
        }
    }

    pub(in crate::storage::queries::state) fn deadline(
        self,
        now: time::Instant,
        hard: time::Instant,
    ) -> time::Instant {
        now.checked_add(self.timeout.get())
            .map_or(hard, |end| end.min(hard))
    }
}
