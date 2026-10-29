use crate::{core::driver::schedule::ready::task, task::batch::RootError};

/// Failure to reserve or drive one fixed task batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Task(task::Error),
    Root(RootError),
}

const _: () = assert!(core::mem::size_of::<Error>() == 4 * core::mem::size_of::<usize>());
