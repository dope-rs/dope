use std::time;

const BEFORE_EPOCH: u64 = u64::MAX;
const OUT_OF_RANGE: u64 = u64::MAX - 1;

/// Unix-epoch nanoseconds with explicit unavailable states in one word.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Timestamp(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimestampError {
    BeforeEpoch,
    OutOfRange,
}

impl Timestamp {
    pub(crate) fn now() -> Self {
        Self::from(time::SystemTime::now())
    }

    pub fn get(self) -> Result<u64, TimestampError> {
        match self.0 {
            BEFORE_EPOCH => Err(TimestampError::BeforeEpoch),
            OUT_OF_RANGE => Err(TimestampError::OutOfRange),
            nanos => Ok(nanos),
        }
    }

    fn from(now: time::SystemTime) -> Self {
        let duration = match now.duration_since(time::UNIX_EPOCH) {
            Ok(duration) => duration,
            Err(_) => return Self(BEFORE_EPOCH),
        };
        match u64::try_from(duration.as_nanos()) {
            Ok(nanos) if nanos < OUT_OF_RANGE => Self(nanos),
            Ok(_) | Err(_) => Self(OUT_OF_RANGE),
        }
    }
}

const _: () = assert!(std::mem::size_of::<Timestamp>() == std::mem::size_of::<u64>());
