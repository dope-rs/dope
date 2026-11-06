use core::{marker, ptr};

use o3::{base::cell::region, depot::arena::slab::key};

use crate::task::storage::{Handle, scope, set::group};

/// A task identity retains its original driver and storage domain and stays on its issuing thread.
#[must_use = "dropping a task identity does not cancel or remove its fiber"]
pub struct Id<'d, D, K = key::Parts> {
    key: K,
    owner: Owner<(&'d mut &'d (), D)>,
}

#[derive(Clone, Copy)]
pub(crate) struct Owner<D> {
    address: ptr::NonNull<()>,
    domain: marker::PhantomData<fn(D) -> D>,
}

impl<D> Owner<D> {
    pub(crate) fn of(value: &impl Sized) -> Self {
        Self {
            address: ptr::NonNull::from(value).cast(),
            domain: marker::PhantomData,
        }
    }
}

impl<'d, D, K> Id<'d, D, K> {
    pub(crate) const fn new(key: K, owner: Owner<D>) -> Self {
        Self {
            key,
            owner: Owner {
                address: owner.address,
                domain: marker::PhantomData,
            },
        }
    }

    pub(in crate::task::storage) fn bind<'s>(
        self,
        _: region::Identity<'s>,
    ) -> scope::Scoped<'s, 'd, D, K> {
        Id::new(
            self.key,
            Owner {
                address: self.owner.address,
                domain: marker::PhantomData,
            },
        )
    }

    pub(crate) fn raw_for(&self, owner: Owner<D>) -> Option<K>
    where
        K: Copy,
    {
        (self.owner.address == owner.address).then_some(self.key)
    }
}

impl<'d, D> Id<'d, D> {
    pub(crate) const fn from_key(key: Handle<'d, D>, owner: Owner<D>) -> Self {
        Self::new(key.parts(), owner)
    }

    pub(crate) const fn from_raw(raw: key::Handle<D>, owner: Owner<D>) -> Self {
        Self::new(raw.parts(), owner)
    }

    pub(crate) fn key_for(&self, owner: Owner<D>) -> Option<Handle<'d, D>> {
        self.raw_for(owner).map(Handle::from_parts)
    }
}

impl<'s, 'd, D, K: Copy> scope::Scoped<'s, 'd, D, K> {
    pub(in crate::task::storage) fn original(&self) -> Id<'d, D, K> {
        Id::new(
            self.key,
            Owner {
                address: self.owner.address,
                domain: marker::PhantomData,
            },
        )
    }
}

impl<'s, 'd, D> Id<'d, group::Scope<'s, D>> {
    pub(in crate::task::storage) fn into_group(self) -> group::Id<'s, 'd, D> {
        group::Id {
            key: Handle::from_parts(self.key),
        }
    }
}
