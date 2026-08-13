use dope_core::driver::route::{self, table};

pub trait Supported {
    type Retirements: Default;

    const AUTHORITATIVE: bool;

    fn arm(retirements: &mut Self::Retirements, capacity: table::Capacity);

    fn next(
        retirements: &mut Self::Retirements,
        capacity: table::Capacity,
    ) -> Option<route::SlotIndex>;

    fn pending(retirements: &Self::Retirements) -> bool;
}
