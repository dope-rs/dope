use std::mem;

use o3::depot::arena::slab::key;

use crate::conn::output::control::{owner, revision};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::conn) struct Key {
    owner: key::Parts<{ owner::MAX }>,
    revision: key::Generation<{ revision::MAX }>,
}

impl Key {
    pub(in crate::conn::output::control) const fn new(
        owner: key::Parts<{ owner::MAX }>,
        revision: key::Generation<{ revision::MAX }>,
    ) -> Self {
        Self { owner, revision }
    }

    pub(in crate::conn::output::control) const fn parts(
        self,
    ) -> (
        key::Parts<{ owner::MAX }>,
        key::Generation<{ revision::MAX }>,
    ) {
        (self.owner, self.revision)
    }

    pub(in crate::conn) const fn index(self) -> usize {
        self.owner.index() as usize
    }
}

const _: () = assert!(mem::size_of::<Option<Key>>() == 2 * mem::size_of::<u64>());
