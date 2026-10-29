use crate::core::driver::schedule::ready::{task, task::owner};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Full,
    Admission(task::Error),
    Topology(task::Error),
    Owner(owner::Error),
}
