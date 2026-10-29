use std::{
    cell::Cell,
    marker::PhantomData,
    mem::{align_of, size_of},
};

use o3::mem::{self, Return};

use crate::driver::schedule::ready::wake::fanout::storage::{Key, Registry};

/// A linear persistent subscription in a [`Registry`].
#[repr(transparent)]
pub struct Lease<'target, W> {
    active: Option<mem::Lease<Bound<'target, W>>>,
    target: PhantomData<Cell<&'target Registry<W>>>,
}

struct Bound<'target, W>(&'target Registry<W>);

impl<W> Return for Bound<'_, W> {
    type Item = Key;

    fn return_item(self, key: Self::Item) {
        self.0.retire(key);
    }
}

impl<W> Default for Lease<'_, W> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'target, W> Lease<'target, W> {
    pub const fn new() -> Self {
        Self {
            active: None,
            target: PhantomData,
        }
    }

    pub(super) fn active(&self) -> Option<(&'target Registry<W>, Key)> {
        self.active
            .as_ref()
            .map(|lease| (lease.sink().0, *lease.item()))
    }

    pub(super) fn set(&mut self, registry: &'target Registry<W>, key: Key) {
        self.active = Some(mem::Lease::new(Bound(registry), key));
    }
}

const _: () = {
    assert!(size_of::<Lease<'static, usize>>() == 2 * size_of::<usize>());
    assert!(align_of::<Lease<'static, usize>>() == align_of::<usize>());
};
