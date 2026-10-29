use o3::alloc;

use crate::link::egress::metadata;

/// Exact wait-table geometry rejected before retained egress allocation.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum WaitLayout {
    Product {
        lanes: metadata::LaneCount,
        stride: usize,
    },
    Limit {
        lanes: metadata::LaneCount,
        size: usize,
        maximum: usize,
    },
}

/// Failure to construct retained egress storage.

#[derive(Debug)]
pub enum Error {
    Wait(WaitLayout),
    Allocation(alloc::Error),
}
