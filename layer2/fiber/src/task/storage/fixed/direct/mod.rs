use std::{marker, pin};

use o3::{
    base::cell::region,
    depot::arena::slab::{key, storage},
};

use crate::task::storage::{
    fixed::{Id, Slab, local, store},
    scope::Scope,
};

#[must_use = "an admission must be consumed by insertion"]
pub struct Admit<'a, 's, 'd, F, D, S: storage::Storage> {
    slot: store::Anchor<'a, F, D, S>,
    proof: region::Identity<'s>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'s, 'd, F, D, S: storage::Storage> Admit<'_, 's, 'd, F, D, S> {
    pub fn insert(self, fiber: F) -> Id<'s, 'd, D> {
        Id::new(self.slot.insert(fiber).parts(), self.proof)
    }
}

impl<'s, 'd, F, const N: usize, D, S: storage::Storage>
    Scope<'s, pin::Pin<&mut Slab<'d, F, N, D, S>>>
{
    pub fn admit(&mut self) -> Option<Admit<'_, 's, 'd, F, D, S>> {
        Some(Admit {
            slot: self.source.as_mut().project().inner.vacant()?,
            proof: self.proof,
            driver: marker::PhantomData,
        })
    }

    pub fn insert(&mut self, fiber: F) -> Option<Id<'s, 'd, D>> {
        Some(self.admit()?.insert(fiber))
    }

    pub fn entry<K: Copy + Into<key::Parts>>(
        &mut self,
        id: Id<'s, 'd, D, K>,
    ) -> Result<local::Entry<impl local::Member<Fiber = F>, Id<'s, 'd, D, K>>, Id<'s, 'd, D, K>>
    {
        let Some(member) = self.source.as_mut().project().inner.anchor(id.key.into()) else {
            return Err(id);
        };
        Ok(local::Entry { member, id })
    }

    pub fn remove<K: Copy + Into<key::Parts>>(&mut self, id: Id<'s, 'd, D, K>) -> local::Removal {
        match self.try_remove(id) {
            Ok(()) => local::Removal::Removed,
            Err(_) => local::Removal::Absent,
        }
    }

    pub fn try_remove<K: Copy + Into<key::Parts>>(
        &mut self,
        id: Id<'s, 'd, D, K>,
    ) -> Result<(), Id<'s, 'd, D, K>> {
        self.entry(id)?.remove();
        Ok(())
    }
}
