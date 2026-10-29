use dope_sys::{memory, memory::buffer::provided};

use crate::reactor::buffers::Cleanup;

pub(crate) struct Build<L: memory::raw::Receive, P> {
    primary: P,
    cleanup: Cleanup<L>,
}

impl<L: memory::raw::Receive, P> Build<L, P> {
    pub(in crate::reactor) fn new(primary: P, cleanup: Cleanup<L>) -> Self {
        Self { primary, cleanup }
    }

    pub(in crate::reactor) fn open<F: provided::Failure<Cleanup = Cleanup<L>>>(
        fault: F,
        classify: impl FnOnce(memory::Error) -> P,
    ) -> Result<P, Self> {
        let (error, cleanup) = fault.into_parts();
        let primary = classify(error);
        match cleanup {
            Some(cleanup) => Err(Self { primary, cleanup }),
            None => Ok(primary),
        }
    }

    pub(in crate::reactor) fn primary(&self) -> P
    where
        P: Copy,
    {
        self.primary
    }

    pub(in crate::reactor) fn recover(self) -> (P, Cleanup<L>) {
        let Self { primary, cleanup } = self;
        (primary, cleanup)
    }
}

impl<L: memory::raw::Receive, P> memory::Failure for Build<L, P> {
    fn failure(&self) -> memory::Error {
        self.cleanup.failure()
    }
}
