use core::{mem, pin};

use crate::task::storage::fixed::schedule::ready::linked::Queue;

pub struct Prepared<'a, 'd> {
    pub(in crate::task::storage::fixed::schedule) queue: pin::Pin<&'a Queue<'d>>,
}

impl<'a, 'd> Prepared<'a, 'd> {
    pub(in crate::task::storage::fixed::schedule::ready::linked) const fn new(
        queue: pin::Pin<&'a Queue<'d>>,
    ) -> Self {
        Self { queue }
    }
}

const _: () = {
    assert!(mem::size_of::<Prepared<'static, 'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Prepared<'static, 'static>>>() == mem::size_of::<usize>());
};
