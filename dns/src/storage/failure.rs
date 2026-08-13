use crate::{discovery, storage::timestamp};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FailureTimes {
    pub(crate) started_unix_ns: timestamp::Timestamp,
    pub(crate) completed_unix_ns: timestamp::Timestamp,
}

impl FailureTimes {
    pub(crate) fn new(
        started_unix_ns: timestamp::Timestamp,
        completed_unix_ns: timestamp::Timestamp,
    ) -> Self {
        Self {
            started_unix_ns,
            completed_unix_ns,
        }
    }

    pub(crate) fn at(timestamp: timestamp::Timestamp) -> Self {
        Self::new(timestamp, timestamp)
    }
}

#[derive(Debug)]
pub(crate) struct Failure {
    pub(crate) kind: discovery::ErrorKind,
    pub(crate) times: FailureTimes,
}

impl Failure {
    pub(crate) fn new(kind: discovery::ErrorKind, times: FailureTimes) -> Self {
        Self { kind, times }
    }
}

const _: () =
    assert!(std::mem::size_of::<FailureTimes>() == std::mem::size_of::<timestamp::Timestamp>() * 2);
