use core::mem;

use store::Store;

mod dequeued;
mod drain;
mod entry;
mod prepared;
mod queue;
mod state;
mod store;

pub use dequeued::Dequeued;
pub(in crate::task::storage::fixed::schedule) use drain::Drain;
pub use entry::Entry;
pub use prepared::Prepared;
pub(in crate::task::storage::fixed::schedule) use queue::Queue;
pub(in crate::task::storage::fixed::schedule) use state::State;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum Side {
    First,
    Second,
}

impl Side {
    const fn index(self) -> usize {
        match self {
            Self::First => 0,
            Self::Second => 1,
        }
    }

    const fn other(self) -> Self {
        match self {
            Self::First => Self::Second,
            Self::Second => Self::First,
        }
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Option<Side>>() == 1);
    assert!(mem::size_of::<Dequeued<'static, 'static>>() == 2 * word);
    assert!(mem::size_of::<Option<Dequeued<'static, 'static>>>() == 2 * word);
    assert!(mem::size_of::<Drain<'static, 'static>>() == 2 * word);
    assert!(mem::size_of::<Option<Drain<'static, 'static>>>() == 2 * word);
    assert!(mem::size_of::<Entry>() == 3 * word);
    assert!(mem::size_of::<State>() == 5 * word);
};
