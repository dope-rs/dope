use crate::probe::{
    self, Stopped,
    session::{Checked, Empty, Session, Stage},
};

type Failure<S, E> = Checked<
    <S as Session>::Probe,
    <<S as Session>::Outcome as probe::Outcome>::Observation,
    <S as Session>::Error,
    <S as Occupied>::Index,
    <S as Occupied>::Length,
    <S as Occupied>::Capacity,
    E,
>;

type Progress<S, E> = Result<<S as Session>::Pending, Stopped<Stage<S>, Failure<S, E>>>;

pub trait Occupied: Session {
    type Empty: Empty<Occupied = Self>;
    type Target;
    type Remove;
    type Index;
    type Length;
    type Capacity;

    fn slot(&self) -> Self::Target;
    fn try_remove(self) -> Result<Self::Empty, Stopped<Self, Self::Remove>>;
    fn next<E>(
        waiting: Self::Pending,
        check: impl FnOnce(&[u8]) -> Result<(), E>,
    ) -> Progress<Self, E>;
}
