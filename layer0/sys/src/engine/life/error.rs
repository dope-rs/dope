/// Exact native failure while draining one driver generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error<E, F> {
    Completion(F),
    Reactor { cause: E, completion: Option<F> },
    File(E),
    Tuning,
}

impl<E, F> From<F> for Error<E, F> {
    fn from(fault: F) -> Self {
        Self::Completion(fault)
    }
}
