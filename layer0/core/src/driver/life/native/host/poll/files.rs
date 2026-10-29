use std::os::fd;

use dope_sys::{
    Errno,
    io::reactor::{read, retry},
    table,
    table::change,
};

use crate::{
    driver::{
        life::native::host::poll::events,
        result::{poll, poll::Operation},
        retained::flight,
        route,
        route::{indices, table::Capacity},
        settings,
    },
    io::{
        event::batch::Flow,
        fd::handles::{Accepted, OwnedSlot, PhysicalSlot},
        transfer,
    },
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Slots;

// SAFETY: Capacity enumerates the exact bounded route prefix; PhysicalSlot keeps
// its proved SlotIndex. The projections retain both nominal domains unchanged.
unsafe impl change::raw::Slots for Slots {
    type Index = indices::SlotIndex;
    type Physical = PhysicalSlot;
    type Capacity = Capacity;
    type Data = events::Udata;

    const MAX: usize = route::SLOT_MASK as usize + 1;

    fn offset(index: Self::Index) -> usize {
        index.raw() as usize
    }

    fn count(capacity: Self::Capacity) -> usize {
        capacity.raw() as usize
    }

    unsafe fn nth(_: Self::Capacity, ordinal: usize) -> Self::Index {
        // SAFETY: the caller passes an ordinal below `count(capacity)`, which the
        // slot mask bounds, so the packed field keeps its proved position.
        indices::SlotIndex::from_field(ordinal as u64)
    }
}

// SAFETY: FileSlots partitions its bounded capacity into the accepted prefix
// and reserved outbound slots. Accepted validates that prefix and stays affine;
// OwnedSlot's projection retains the descriptor's original identity.
unsafe impl table::raw::Slots for Slots {
    type Layout = settings::FileSlots;
    type Owned = OwnedSlot;
    type Accepted = Accepted;
    type Missing = poll::Missing;

    fn excludes(capacity: Self::Capacity, index: Self::Index) -> Option<Self::Missing> {
        capacity.excludes(index).map(poll::Missing::Bounds)
    }

    fn backing(index: Self::Index) -> Self::Missing {
        poll::Missing::Backing { index }
    }

    fn vacant(index: Self::Index) -> Self::Missing {
        poll::Missing::Vacant { index }
    }

    fn replaced(index: Self::Index, expected: fd::RawFd, actual: fd::RawFd) -> Self::Missing {
        poll::Missing::Replaced {
            index,
            expected,
            actual,
        }
    }
}

impl read::Context for Slots {
    type Slots = Self;
    type Key = flight::raw::Echo;
    type Flow = Flow;
    type Operation = Operation;

    const ACCEPT: Operation = Operation::Accept;
    const RECV: Operation = Operation::Recv;
    const MESSAGE: Operation = Operation::RecvMsg;
}

impl retry::Context for Slots {
    type Slots = Self;
    type Key = flight::raw::Echo;
    type Operation = Operation;
    type Extent = transfer::Extent;
    type Outcome = Result<transfer::Len, Errno>;
    type Unit = Result<(), Errno>;

    const SOCKET_ERROR: Operation = Operation::SocketError;
    const SEND: Operation = Operation::Send;
    const CONNECT: Operation = Operation::Connect;

    fn sent(result: Result<transfer::Len, Errno>) -> Self::Outcome {
        result
    }

    fn connected(result: Result<(), Errno>) -> Self::Unit {
        result
    }
}
