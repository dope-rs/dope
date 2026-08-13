use std::{error, fmt, io};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    WakeHopCeiling,
    Capacity { requested: usize, available: usize },
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::WakeHopCeiling => {
                formatter.write_str("task domain parent exceeds the fixed wake-hop ceiling")
            }
            Self::Capacity {
                requested,
                available,
            } => write!(
                formatter,
                "dynamic ready capacity exhausted: requested {requested}, available {available}",
            ),
        }
    }
}

impl error::Error for Error {}

impl From<Error> for io::Error {
    fn from(error: Error) -> Self {
        let kind = match error {
            Error::WakeHopCeiling => io::ErrorKind::InvalidInput,
            Error::Capacity { .. } => io::ErrorKind::WouldBlock,
        };
        Self::new(kind, error)
    }
}
