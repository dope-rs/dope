use std::{error, fmt};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Capacity { length: usize, limit: usize },
    Truncated,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capacity { length, limit } => {
                write!(formatter, "frame length {length} exceeds limit {limit}")
            }
            Self::Truncated => formatter.write_str("truncated length-prefixed frame"),
        }
    }
}

impl error::Error for Error {}
