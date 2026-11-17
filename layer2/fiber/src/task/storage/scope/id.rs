use core::marker;

use o3::base::{cell::region, marker::ThreadBound};

use crate::task::storage::{fixed::schedule::ready::Global, scope};

/// A task identity retains its original driver and storage domain and stays on its issuing thread.
#[must_use = "dropping a task identity does not cancel or remove its fiber"]
pub struct Id<'d, D, K> {
    key: K,
    domain: marker::PhantomData<(&'d mut &'d (), fn(D) -> D, ThreadBound)>,
}

impl<'d, D, K> Id<'d, D, K> {
    pub(in crate::task::storage) const fn new(key: K) -> Self {
        Self {
            key,
            domain: marker::PhantomData,
        }
    }

    pub(in crate::task::storage) const fn key(&self) -> &K {
        &self.key
    }

    pub(in crate::task::storage) fn bind<'s>(
        self,
        _: region::Identity<'s>,
    ) -> scope::Scoped<'s, 'd, D, K> {
        Id::new(self.key)
    }
}

const _: () = {
    assert!(size_of::<Id<'static, (), Global>>() == size_of::<Global>());
    assert!(size_of::<Option<Id<'static, (), Global>>>() == size_of::<Global>());
};
