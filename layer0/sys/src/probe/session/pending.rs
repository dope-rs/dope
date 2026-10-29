use crate::probe::{
    Stopped,
    session::{Failure, Session, Stage},
};

type Cancelled<S> = Result<S, Stopped<Stage<S>, Failure<S>>>;

/// A live session permits cancellation without exposing mutable owner access.
pub trait Pending: Sized {
    type Session: Session<Pending = Self>;

    /// Reports the prior failure before acknowledging its exact unread completion.
    fn resume(
        stopped: Stopped<Self, Failure<Self::Session>>,
        report: impl FnMut(Failure<Self::Session>),
    ) -> Result<Self, Stopped<Self, Failure<Self::Session>>>;

    fn cancel(self) -> Cancelled<Self::Session>;
}
