use crate::{
    backend::bound,
    driver::{self, flight},
};

pub(crate) trait Queue {
    fn submit<'owner, 'd: 'owner>(
        &mut self,
        submission: bound::Bound<'owner, 'd>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError>;
    fn cancel(&mut self, flight: &mut flight::Flight<'_>) -> Result<(), driver::SubmitError>;
}

pub(crate) trait Source {
    type Queue<'a>: Queue
    where
        Self: 'a;

    fn queue(&mut self) -> Self::Queue<'_>;
}
