use dope_sys::io::socket::addr;

/// Exact validation failure of a retained socket address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Storage {
        actual: usize,
        capacity: usize,
    },
    Header {
        actual: usize,
        required: usize,
    },
    Address {
        family: i32,
        actual: usize,
        required: usize,
    },
    Unsupported {
        family: i32,
        actual: usize,
    },
    PathUnsupported,
    PathEmpty,
    PathLength {
        actual: usize,
        maximum: usize,
    },
    PathNul {
        offset: usize,
    },
}

impl Error {
    pub(crate) fn from_path(error: addr::PathError) -> Self {
        match error {
            addr::PathError::Unsupported => Self::PathUnsupported,
            addr::PathError::Empty => Self::PathEmpty,
            addr::PathError::Length { actual, maximum } => Self::PathLength { actual, maximum },
            addr::PathError::Nul { offset } => Self::PathNul { offset },
        }
    }
}

impl From<addr::Error> for Error {
    fn from(error: addr::Error) -> Self {
        match error {
            addr::Error::Storage { actual, capacity } => Self::Storage { actual, capacity },
            addr::Error::Header { actual, required } => Self::Header { actual, required },
            addr::Error::Address {
                family,
                actual,
                required,
            } => Self::Address {
                family,
                actual,
                required,
            },
            addr::Error::Unsupported { family, actual } => Self::Unsupported { family, actual },
        }
    }
}
