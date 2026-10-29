use dope_sys::io::reactor::complete::Record;

use crate::reactor::cq::{Flags, Outcome};

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(C)]
pub struct Observed<T> {
    token: T,
    outcome: Outcome,
    flags: Flags,
}

impl<T> Record<T> for Observed<T> {
    type Outcome = Outcome;
    type Flags = Flags;

    fn new(token: T, outcome: Self::Outcome, flags: Self::Flags) -> Self {
        Self {
            token,
            outcome,
            flags,
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Observed<u64>>() == 2 * std::mem::size_of::<u64>());
    assert!(std::mem::align_of::<Observed<u64>>() == std::mem::align_of::<u64>());
};
