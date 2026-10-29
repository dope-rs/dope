use crate::probe::Check;

/// Couples a canary's caller-owned identity with its exact rejection evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Failure<P, O> {
    probe: P,
    check: Check<O>,
}

impl<P, O> Failure<P, O> {
    pub const fn new(probe: P, check: Check<O>) -> Self {
        Self { probe, check }
    }

    pub fn into_parts(self) -> (P, Check<O>) {
        (self.probe, self.check)
    }
}

impl<P, O: Copy> Failure<P, O> {
    pub(super) fn unread(&self) -> Option<O> {
        match &self.check {
            Check::InvalidFlags(observed) => Some(*observed),
            Check::Rejected(_) | Check::Unexpected(_) | Check::Nonterminal(_) => None,
        }
    }
}
