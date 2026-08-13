use core::fmt;
use std::error;

use o3::collections;

use crate::task;

#[derive(Clone, Copy, Debug)]
pub enum GroupAdmissionError {
    Domain(task::Error),
    Allocation(collections::AllocationError),
}

impl fmt::Display for GroupAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Domain(error) => write!(formatter, "{error}"),
            Self::Allocation(error) => write!(formatter, "{error}"),
        }
    }
}

impl error::Error for GroupAdmissionError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Domain(error) => Some(error),
            Self::Allocation(error) => Some(error),
        }
    }
}

impl From<task::Error> for GroupAdmissionError {
    fn from(error: task::Error) -> Self {
        Self::Domain(error)
    }
}

impl From<collections::AllocationError> for GroupAdmissionError {
    fn from(error: collections::AllocationError) -> Self {
        Self::Allocation(error)
    }
}
