use std::mem;

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
    slots: Option<table::Iter>,
}

/// Retirement state selected by one snapshot synchronization policy.
pub trait State {
    /// Opens an authoritative scan when the policy permits retirement.
    fn scan(&mut self) -> Option<&mut Scan>;

    /// Advances one policy-owned retirement slot.
    fn next(&mut self) -> Option<indices::SlotIndex>;

    /// Reports whether an opened scan still owns table slots.
    fn is_pending(&self) -> bool;
}

impl Scan {
    pub(crate) fn arm(&mut self, cap: table::Capacity) {
        self.slots = Some(cap.into_iter());
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
        let slots = self.slots.as_mut()?;
        let slot = slots.next();
        if slots.len() == 0 {
            self.slots = None;
        }
        slot
    }

    fn is_pending(&self) -> bool {
        self.slots.is_some()
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
const _: () = assert!(mem::size_of::<Scan>() <= 2 * mem::size_of::<usize>());
