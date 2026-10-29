//! Driver configuration failures.

use o3::buffer::storage::Zero;

use crate::{
    driver::{route, route::table, settings, settings::demands::slots},
    io::datagram::payload,
};

/// Failure to construct a representable driver plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Config {
    QueueSubmissionsOutOfRange {
        actual: u32,
        minimum: u32,
        maximum: u32,
    },
    ReceiveEntriesOutOfRange {
        actual: u32,
        minimum: u32,
        maximum: u32,
    },
    ReceiveEntriesNotPowerOfTwo {
        actual: u16,
    },
    ReceiveBuffer(Zero),
    ReceiveAllocationTooLarge {
        entries: u16,
        buffer_len: u32,
        actual: u64,
        maximum: u64,
    },
    DatagramPayload(payload::Error),
    DescriptorCapacityOutOfRange {
        accepted: u32,
        outbound: u32,
        actual: u64,
        minimum: u32,
        maximum: u32,
    },
    ReadyCapacity {
        files: u32,
        ready: u32,
        maximum: u32,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Claim {
    DuplicateRoute(route::Id),
    AcceptedDescriptorsOverflow {
        current: u32,
        added: u32,
    },
    OutboundDescriptorsOverflow {
        current: u32,
        added: u32,
    },
    DescriptorConnectionsOutOfRange {
        actual: usize,
        minimum: usize,
        maximum: usize,
    },
    ReceiveEntriesOverflow {
        current: u32,
        added: u32,
    },
    ReceiveEntriesPowerOfTwoOverflow {
        requested: u32,
    },
    ReceiveEntriesTooLarge {
        actual: usize,
        maximum: usize,
    },
    ResidentDomainsOverflow(settings::DomainSum),
    QueueSubmissionsPowerOfTwoOverflow {
        requested: u32,
    },
    SchedulerReadyOverflow(slots::Sum),
    SchedulerTimersOverflow(slots::Sum),
    Layout(Config),
}

impl From<Config> for Claim {
    fn from(source: Config) -> Self {
        Self::Layout(source)
    }
}

impl From<settings::DomainSum> for Claim {
    fn from(source: settings::DomainSum) -> Self {
        Self::ResidentDomainsOverflow(source)
    }
}

impl From<table::Invalid> for Claim {
    fn from(source: table::Invalid) -> Self {
        Self::DescriptorConnectionsOutOfRange {
            actual: source.actual,
            minimum: source.minimum,
            maximum: source.maximum,
        }
    }
}
