use schedule::selected;

use crate::task::storage::fixed::{
    quota::{self, State},
    schedule::{self, queue},
};

pub enum Outcome<T, O> {
    Pending { target: T },
    PendingReady { target: T },
    Complete { target: T, output: O },
}

#[must_use = "a selection must be handled before the application turn ends"]
pub enum Selection<'plane, 'turn, 'd, R, Q = quota::Physical, S = queue::Flat<'d>>
where
    R: schedule::PlaneTree<'d>,
    Q: State,
    S: queue::Backend<'d> + 'plane,
{
    Idle,
    Yielded,
    Ready(selected::Item<'plane, 'turn, 'd, R, Q, S>),
}
