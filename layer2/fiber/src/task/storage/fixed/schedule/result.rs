use std::convert::Infallible;

use crate::task::storage::fixed::schedule::{Mode, queue, selected};

pub enum Outcome<T, O, I = Infallible> {
    Pending { target: T },
    PendingReady { target: T },
    Complete { target: T, output: O },
    Yielded { target: T, output: I },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Active,
    Absent,
}

#[must_use = "a selection must be handled before the application turn ends"]
pub enum Selection<
    'plane,
    'turn,
    'd,
    F,
    T: Copy,
    M: Mode<'d, F>,
    S: queue::Backend<'d> + 'plane = queue::Flat<'d>,
> {
    Idle,
    Yielded,
    Ready(selected::Item<'plane, 'turn, 'd, F, T, M, S>),
}
