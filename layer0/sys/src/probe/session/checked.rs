use crate::{memory::buffer::canary, probe};

#[derive(Debug)]
pub enum Checked<P, O, E, I, L, C, F> {
    Wait(probe::Cause<P, O, E>),
    MissingBuffer(P),
    Buffer {
        probe: P,
        failure: canary::Failure<I, L, C, F>,
    },
    Terminated(P),
}

impl<P, O, E, I, L, C, F> From<probe::Cause<P, O, E>> for Checked<P, O, E, I, L, C, F> {
    fn from(cause: probe::Cause<P, O, E>) -> Self {
        Self::Wait(cause)
    }
}
