use std::{hash, marker, mem};

use dope_core::driver::route::{self, Identity, KeyTag, indices, kind, lease, table};
use o3::base::num::bounded;

/// Exact-generation connection authority bound to its driver lifetime and route.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Key<'d, ID: Identity, C: kind::Class = kind::Any> {
    target: route::Target<'d, ID>,
    class: marker::PhantomData<C>,
}

#[derive(Clone, Copy)]
pub(in crate::link) struct Keyspace<'d, ID: Identity> {
    space: route::Space<'d, ID>,
}

impl<ID: Identity, C: kind::Class> PartialEq for Key<'_, ID, C> {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target
    }
}

impl<ID: Identity, C: kind::Class> Eq for Key<'_, ID, C> {}

impl<ID: Identity, C: kind::Class> hash::Hash for Key<'_, ID, C> {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.target.hash(state);
    }
}

impl<'d, ID: Identity> Key<'d, ID> {
    pub(in crate::link::pool) const fn from_target(target: route::Target<'d, ID>) -> Self {
        Self {
            target,
            class: marker::PhantomData,
        }
    }
}

impl<'d, ID: Identity, C: kind::Class> Key<'d, ID, C> {
    pub(in crate::link::pool) const fn connection(&self) -> Key<'d, ID> {
        Key {
            target: self.target,
            class: marker::PhantomData,
        }
    }

    pub const fn target(self) -> route::Target<'d, ID> {
        self.target
    }

    pub const fn parts(self) -> table::Parts<ID> {
        self.target.parts()
    }

    pub const fn lane(self) -> indices::SlotIndex {
        self.target.slot()
    }

    pub const fn index(self) -> bounded::U32<0, { route::SLOT_MASK as u32 }> {
        self.lane().index()
    }

    pub const fn epoch(self) -> route::Epoch {
        self.target.epoch()
    }
}

impl<'d, ID: Identity> Keyspace<'d, ID> {
    pub(in crate::link) fn from_claim(route: &lease::Claim<'d, ID>) -> Self {
        Self {
            space: route.driver().targets(),
        }
    }

    pub(in crate::link) fn parse<C: kind::Class>(
        self,
        target: route::Erased<'d, C>,
    ) -> Result<Key<'d, ID, C>, route::Mismatch<'d, ID, C>> {
        let op = self.space.parse(target)?;
        Ok(Key {
            target: route::Target::from(&op),
            class: marker::PhantomData,
        })
    }

    pub(in crate::link) const fn bind_table(self, key: table::Key<ID>) -> Key<'d, ID> {
        Key::from_target(self.space.bind(key.slot(), key.epoch()))
    }

    pub(in crate::link) const fn bind_parts(
        self,
        slot: indices::SlotIndex,
        epoch: route::Epoch,
    ) -> Key<'d, ID> {
        Key::from_target(self.space.bind(slot, epoch))
    }
}

const _: () = assert!(mem::size_of::<Key<'static, KeyTag<0>>>() == mem::size_of::<route::Token>());
const _: () =
    assert!(mem::align_of::<Key<'static, KeyTag<0>>>() == mem::align_of::<route::Token>());
const _: () = assert!(mem::size_of::<Keyspace<'static, KeyTag<0>>>() == 0);
const _: () = assert!(
    mem::size_of::<lease::Owner<'static, KeyTag<0>, Keyspace<'static, KeyTag<0>>>>()
        == mem::size_of::<lease::Owner<'static, KeyTag<0>>>()
);

const _: () = {
    assert!(
        mem::size_of::<Key<'static, KeyTag<0>, kind::Send>>()
            == mem::size_of::<Key<'static, KeyTag<0>>>()
    );
    assert!(
        mem::size_of::<Key<'static, KeyTag<0>, kind::Recv>>()
            == mem::size_of::<Key<'static, KeyTag<0>>>()
    );
    assert!(
        mem::size_of::<Option<Key<'static, KeyTag<0>, kind::Send>>>()
            == mem::size_of::<Option<Key<'static, KeyTag<0>>>>()
    );
    assert!(
        mem::size_of::<Option<Key<'static, KeyTag<0>, kind::Recv>>>()
            == mem::size_of::<Option<Key<'static, KeyTag<0>>>>()
    );
};
