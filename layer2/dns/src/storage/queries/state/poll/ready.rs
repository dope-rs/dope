use std::time;

use o3::base::time::{Nanos, Unix};

use crate::{
    lookup::{self, error},
    storage::queries::state,
};

pub(crate) struct Ready<const N: usize> {
    pub(crate) reason: lookup::Reason,
    pub(crate) started_unix_ns: Unix<Nanos>,
    pub(crate) completed_at: time::Instant,
    pub(crate) result: Result<state::Answer<N>, error::Kind>,
}
