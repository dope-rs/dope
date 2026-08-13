use dope_core::driver::route::{self, table};

mod supported;
pub(crate) use supported::Supported;

/// Compile-time handling of epochs selected from an older service snapshot.
pub trait Policy: Supported {}

/// Keeps existing epochs frozen to their selected endpoint until their normal
/// lifecycle ends. Only later epochs select from the new snapshot.
#[derive(Clone, Copy, Debug)]
pub struct Preserve;

impl Supported for Preserve {
    type Retirements = ();

    const AUTHORITATIVE: bool = false;

    fn arm(_retirements: &mut Self::Retirements, _capacity: table::Capacity) {}

    fn next(
        _retirements: &mut Self::Retirements,
        _capacity: table::Capacity,
    ) -> Option<route::SlotIndex> {
        None
    }

    fn pending(_retirements: &Self::Retirements) -> bool {
        false
    }
}

impl Policy for Preserve {}

/// Treats each installed snapshot as authoritative. Deferred attempts rebind
/// before submission; busy and connected epochs whose endpoint disappeared
/// close with `EndpointRetired` and redial from the new snapshot.
#[derive(Clone, Copy, Debug)]
pub struct Replace;

impl Supported for Replace {
    type Retirements = (usize, usize);

    const AUTHORITATIVE: bool = true;

    fn arm(retirements: &mut Self::Retirements, capacity: table::Capacity) {
        retirements.1 = capacity.get();
    }

    fn next(
        retirements: &mut Self::Retirements,
        capacity: table::Capacity,
    ) -> Option<route::SlotIndex> {
        if retirements.1 == 0 {
            return None;
        }
        let Some(slot) = capacity.slot(retirements.0) else {
            retirements.1 = 0;
            return None;
        };
        retirements.0 += 1;
        if retirements.0 == capacity.get() {
            retirements.0 = 0;
        }
        retirements.1 -= 1;
        Some(slot)
    }

    fn pending(retirements: &Self::Retirements) -> bool {
        retirements.1 != 0
    }
}

impl Policy for Replace {}
