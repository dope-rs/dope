use dope_core::driver::storage;

use crate::{
    executor::{self, session},
    shutdown,
    topology::{self, lending},
};

mod borrow;
mod build;

pub use borrow::Borrow;
pub use build::Build;

/// Executes an installer only inside the driver lifetime its owner supports.
pub trait Execute<S: storage::Plan>: Sized {
    type Error;

    fn execute<'report, 'scope, 'life, 'p, 'd, Q>(
        self,
        stage: &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>,
        report: &'report mut session::Completion,
    ) -> Result<
        shutdown::Requested<Q>,
        topology::Failure<'report, Self::Error, shutdown::Requested<Q>>,
    >
    where
        'd: 'p,
        'p: 'life,
        'life: 'scope,
        Self: 'd,
        S: 'd,
        Q: executor::Startup;
}
