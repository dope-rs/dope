use core::mem;

use o3::depot::queue::indexed::keyed;

use crate::{
    abi,
    task::storage::{Handle, Id, set::slot},
};

pub(in crate::task::storage::set) type Membership<'d, D> = (u64, Handle<'d, D>);

pub(in crate::task::storage::set) struct Order<'d, D> {
    /// The first member's complete identity remains unique after its slot is reused.
    pub(in crate::task::storage::set) identity: Handle<'d, D>,
    pub(in crate::task::storage::set) fifo: keyed::Fifo<Handle<'d, D>, Id<'d, D>>,
}

impl<'d, D> Order<'d, D> {
    pub(super) fn new<F, M>(
        done: slot::Done<'_, '_, 'd, F, M, D, Membership<'d, D>>,
        group: Membership<'d, D>,
    ) -> Self
    where
        F: abi::Fiber<'d>,
    {
        let mut order = Self {
            identity: done.key(),
            fifo: keyed::Fifo::new(),
        };
        done.queue(&mut order.fifo, group);
        order
    }
}

const _: () = {
    assert!(mem::size_of::<Order<'static, ()>>() == mem::size_of::<[u64; 2]>());
};
