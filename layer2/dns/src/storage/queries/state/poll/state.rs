use std::time;

use crate::storage::queries::state::poll;

pub(crate) enum State<const N: usize> {
    Idle,
    Pending(Option<time::Instant>),
    Complete(poll::Ready<N>),
}
