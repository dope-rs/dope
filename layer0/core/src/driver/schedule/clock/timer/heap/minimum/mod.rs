use std::{marker, mem};

use fired::Fired;
use o3::base::marker::ThreadBound;

use crate::driver::schedule::clock::timer::heap::{core::Core, order};

pub(in crate::driver::schedule::clock::timer::heap) mod fired;

/// Exact capability proving that an armed minimum exists.
pub struct Minimum<'a, 'id, P: Ord, R, Tag = ()> {
    core: &'a mut Core<P, R>,
    brand: marker::PhantomData<fn(&'id Tag) -> &'id Tag>,
    thread: marker::PhantomData<ThreadBound>,
    root: order::Root,
}

impl<'a, 'id, P: Ord, R, Tag> Minimum<'a, 'id, P, R, Tag> {
    pub(super) fn new(
        core: &'a mut Core<P, R>,
        brand: marker::PhantomData<fn(&'id Tag) -> &'id Tag>,
        root: order::Root,
    ) -> Self {
        Self {
            core,
            brand,
            thread: marker::PhantomData,
            root,
        }
    }

    pub fn priority(&self) -> &P {
        order::Order::new(self.core).min_priority(self.root)
    }

    /// Removes the proven minimum and marks its resident as fired.
    pub fn fire(self) -> Fired<'a, 'id, P, R, Tag> {
        let core = self.core;
        let live = core.fire(self.root);
        Fired {
            core,
            brand: self.brand,
            thread: self.thread,
            live,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Minimum<'_, '_, u8, u8>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Minimum<'_, '_, u8, u8>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Option<Minimum<'_, '_, u8, u8>>>() == mem::size_of::<usize>());
};
