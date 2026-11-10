use core::{mem, pin};

use crate::task::storage::fixed::schedule::ready::{Global, linked::Queue};

pub struct Prepared<'a, 'd> {
    pub(in crate::task::storage::fixed::schedule) queue: pin::Pin<&'a Queue<'d>>,
    pub(in crate::task::storage::fixed::schedule) index: Global,
}

impl<'a, 'd> Prepared<'a, 'd> {
    pub(in crate::task::storage::fixed::schedule::ready::linked) const fn new(
        queue: pin::Pin<&'a Queue<'d>>,
        index: Global,
    ) -> Self {
        Self { queue, index }
    }
}

const _: () = {
    assert!(mem::size_of::<Prepared<'static, 'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Prepared<'static, 'static>>>() == 2 * mem::size_of::<usize>());
};
