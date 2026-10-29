use crate::shutdown;

mod run;

pub(crate) use run::Run;

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Outcome {
    Quiesced,
    Forced,
}

#[must_use]
pub struct Report<Q> {
    request: shutdown::Requested<Q>,
    outcome: Outcome,
}

impl<Q> Report<Q> {
    pub(crate) const fn new(request: shutdown::Requested<Q>, outcome: Outcome) -> Self {
        Self { request, outcome }
    }

    pub const fn outcome(&self) -> Outcome {
        self.outcome
    }

    pub fn into_parts(self) -> (shutdown::Requested<Q>, Outcome) {
        (self.request, self.outcome)
    }
}
