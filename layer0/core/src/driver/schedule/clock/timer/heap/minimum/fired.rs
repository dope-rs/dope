use std::{marker, mem};

use o3::base::marker::ThreadBound;

use crate::driver::schedule::clock::timer::heap::{core::Core, tokens::Live};

/// Exact capability for a just-fired resident.
pub struct Fired<'a, 'id, P: Ord, R, Tag = ()> {
    pub(super) core: &'a mut Core<P, R>,
    pub(super) brand: marker::PhantomData<fn(&'id Tag) -> &'id Tag>,
    pub(super) thread: marker::PhantomData<ThreadBound>,
    pub(super) live: Live,
}

impl<'id, P: Ord, R, Tag> Fired<'_, 'id, P, R, Tag> {
    pub fn resident(&self) -> &R {
        self.core.resident(self.live)
    }
}

const _: () = {
    assert!(mem::size_of::<Fired<'_, '_, u8, u8>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Fired<'_, '_, usize, u8>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Fired<'_, '_, usize, u8>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Option<Fired<'_, '_, usize, u8>>>() == 2 * mem::size_of::<usize>());
};
