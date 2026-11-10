use std::marker;

use o3::{
    base::{cell::region, marker::ThreadBound},
    depot::arena::slab::key,
};

use crate::task::storage::fixed::local;

#[must_use = "dropping a task identity does not remove its fiber"]
pub struct Id<'s, 'd, D, K = key::Parts> {
    pub(super) key: K,
    proof: region::Identity<'s>,
    domain: marker::PhantomData<(&'d mut &'d (), fn(D) -> D, ThreadBound)>,
}

impl<'s, 'd, D, K> Id<'s, 'd, D, K> {
    pub(super) const fn new(key: K, proof: region::Identity<'s>) -> Self {
        Self {
            key,
            proof,
            domain: marker::PhantomData,
        }
    }
}

impl<'s, 'd, D> Id<'s, 'd, D> {
    pub(super) fn ended(self) -> Id<'s, 'd, D, local::Ended> {
        Id::new(local::Ended(self.key), self.proof)
    }
}

const _: () = {
    assert!(std::mem::size_of::<Id<'static, 'static, ()>>() == std::mem::size_of::<key::Parts>());
    assert!(
        std::mem::size_of::<Option<Id<'static, 'static, ()>>>()
            == std::mem::size_of::<key::Parts>()
    );
};
