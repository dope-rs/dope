use crate::wire::receive::discard;

/// Remaining raw-prefix discard owned by one connection.
pub struct State {
    pub(super) remaining: Option<discard::Prefix>,
}

impl State {
    pub(super) const EMPTY: Self = Self { remaining: None };
}
