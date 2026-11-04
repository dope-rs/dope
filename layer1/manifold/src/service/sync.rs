use std::{mem, num::NonZero};

use dope_core::driver::route::{indices, table};

/// Compile-time handling of epochs selected from an older service snapshot.
pub trait Policy {
    type State: Default + State;

    const AUTHORITATIVE: bool;
}

/// Closed retirement capability for snapshots which preserve existing epochs.
#[derive(Default)]
pub struct Frozen;

/// Active table scan for authoritative snapshot retirement.
#[derive(Default)]
pub struct Scan {
    next: indices::SlotIndex,
    remaining: Option<(table::Capacity, NonZero<u32>)>,
}

/// Retirement state selected by one snapshot synchronization policy.
pub trait State: sealed::State {
    /// Opens an authoritative scan when the policy permits retirement.
    fn scan(&mut self) -> Option<&mut Scan>;

    /// Advances one policy-owned retirement slot.
    fn next(&mut self) -> Option<indices::SlotIndex>;

    /// Reports whether an opened scan still owns table slots.
    fn is_pending(&self) -> bool;
}

mod sealed {
    pub trait State {}

    impl State for super::Frozen {}
    impl State for super::Scan {}
}

impl Scan {
    pub(crate) fn arm(&mut self, cap: table::Capacity) {
        // The owning table keeps its capacity; rearming owes a lap from the current slot.
        self.remaining = NonZero::new(cap.count().raw()).map(|left| (cap, left));
    }
}

impl State for Frozen {
    fn scan(&mut self) -> Option<&mut Scan> {
        None
    }

    fn next(&mut self) -> Option<indices::SlotIndex> {
        None
    }

    fn is_pending(&self) -> bool {
        false
    }
}

impl State for Scan {
    fn scan(&mut self) -> Option<&mut Scan> {
        Some(self)
    }

    fn next(&mut self) -> Option<indices::SlotIndex> {
        let (cap, left) = self.remaining?;
        let slot = self.next;
        self.next = cap.next(slot).unwrap_or(indices::SlotIndex::ZERO);
        self.remaining = NonZero::new(left.get() - 1).map(|left| (cap, left));
        Some(slot)
    }

    fn is_pending(&self) -> bool {
        self.remaining.is_some()
    }
}

/// Keeps existing epochs frozen to their selected endpoint until their normal
/// lifecycle ends. Only later epochs select from the new snapshot.
#[derive(Clone, Copy)]
pub struct Preserve;

impl Policy for Preserve {
    type State = Frozen;

    const AUTHORITATIVE: bool = false;
}

/// Treats each installed snapshot as authoritative. Deferred attempts rebind
/// before submission; busy and connected epochs whose endpoint disappeared
/// close with `EndpointRetired` and redial from the new snapshot.
#[derive(Clone, Copy)]
pub struct Replace;

impl Policy for Replace {
    type State = Scan;

    const AUTHORITATIVE: bool = true;
}

const _: () = assert!(mem::size_of::<Frozen>() == 0);
const _: () = assert!(mem::size_of::<Scan>() == 3 * mem::size_of::<u32>());
