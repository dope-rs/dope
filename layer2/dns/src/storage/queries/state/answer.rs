use std::time;

use crate::query::answer::addr;

#[derive(Debug)]
pub(crate) struct Answer<const N: usize> {
    pub(crate) addresses: addr::Addresses<N>,
    pub(crate) started_at: time::Instant,
    pub(crate) completed_at: time::Instant,
    pub(crate) valid_until: time::Instant,
}
