use crate::probe::{
    Stopped,
    session::{Empty, Failure, Session, Stage},
};

pub trait Direct: Session {
    type Empty: Empty<Direct = Self>;

    fn option(self, probe: Self::Probe) -> Result<Self, Stopped<Stage<Self>, Failure<Self>>>;
    fn close(self, probe: Self::Probe) -> Result<Self::Empty, Stopped<Self, Failure<Self>>>;
}
