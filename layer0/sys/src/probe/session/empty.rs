use std::os::fd;

use crate::probe::{
    Stopped,
    session::{Direct, Failure, Occupied, Session},
};

pub trait Empty: Session {
    type Install;
    type Direct: Direct<Empty = Self>;
    type Occupied: Occupied<Empty = Self>;

    fn create(self, probe: Self::Probe) -> Result<Self::Direct, Stopped<Self, Failure<Self>>>;
    fn install(
        self,
        file: fd::BorrowedFd<'_>,
    ) -> Result<Self::Occupied, Stopped<Self, Self::Install>>;
}
