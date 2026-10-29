use std::{cell, marker, mem, ops};

use dope::core::driver::route::KeyTag;
use o3::{depot, depot::arena::linked::fifo::ends};

use crate::{
    core::driver::route::indices,
    manifold::{app::dispatch::identity, client::conn},
    net::port::{entry, table},
};

pub(in crate::net::port) struct Cleanup;
pub(in crate::net::port) struct Deferred;

type Resident<I, K> = Option<Live<I, K>>;

#[repr(transparent)]
struct Live<I, K> {
    token: I,
    brand: marker::PhantomData<fn(K) -> K>,
}

#[repr(transparent)]
pub(super) struct Fifo<I: identity::Identity, K>(ends::Cell<Resident<I, K>>);

#[repr(transparent)]
pub(in crate::net::port) struct Link<I, K>(cell::Cell<Option<I>>, marker::PhantomData<fn(K) -> K>);

pub(super) struct Bound<'a, 'd, E, I: identity::Identity> {
    token: I,
    index: indices::SlotIndex,
    entry: &'a entry::Entry<'d, E, I>,
}

pub(super) struct Front<'a, 'd, E, I: identity::Identity, K> {
    topology: ends::Front<&'a ends::Cell<Resident<I, K>>, Resident<I, K>>,
    bound: Bound<'a, 'd, E, I>,
    link: &'a Link<I, K>,
}

impl<I: Copy, K> Clone for Live<I, K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<I: Copy, K> Copy for Live<I, K> {}

impl<I: PartialEq, K> PartialEq for Live<I, K> {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token
    }
}

impl<I: Eq, K> Eq for Live<I, K> {}

impl<I: identity::Identity, K> Live<I, K> {
    fn new(token: I) -> Self {
        Self {
            token,
            brand: marker::PhantomData,
        }
    }

    fn index(self) -> indices::SlotIndex {
        self.token.slot()
    }

    /// # Safety
    /// This bound token's slot must name one live entry of the same table.
    unsafe fn bind<'a, 'd, E>(
        self,
        entries: &'a table::raw::Entries<'d, E, I>,
    ) -> &'a entry::Entry<'d, E, I> {
        unsafe { entries.bind(self.index()) }
    }
}

impl<'a, 'd, E, I: identity::Identity> Bound<'a, 'd, E, I> {
    pub(in crate::net::port::table) fn get(
        entries: &'a table::raw::Entries<'d, E, I>,
        token: I,
    ) -> Option<Self> {
        let index = token.slot();
        let entry = entries.get(index)?;
        (Some(token) == entry.token.get()).then_some(Self {
            token,
            index,
            entry,
        })
    }

    fn from_live(token: I, entry: &'a entry::Entry<'d, E, I>) -> Self {
        Self {
            token,
            index: token.slot(),
            entry,
        }
    }

    pub(in crate::net::port::table) fn into_parts(self) -> (I, &'a entry::Entry<'d, E, I>) {
        (self.token, self.entry)
    }

    pub(in crate::net::port::table) const fn index(&self) -> indices::SlotIndex {
        self.index
    }
}

impl<'d, E, I: identity::Identity> ops::Deref for Bound<'_, 'd, E, I> {
    type Target = entry::Entry<'d, E, I>;

    fn deref(&self) -> &Self::Target {
        self.entry
    }
}

impl<I: identity::Identity, K> Fifo<I, K> {
    pub(in crate::net::port::table) const fn new() -> Self {
        Self(ends::Cell::empty())
    }

    pub(in crate::net::port::table) fn push<'a, 'd, E>(
        &self,
        entries: &'a table::raw::Entries<'d, E, I>,
        bound: &Bound<'a, 'd, E, I>,
        select: impl Fn(&'a entry::Entry<'d, E, I>) -> &'a Link<I, K>,
    ) where
        K: 'a,
    {
        let entry = bound.entry;
        let link = select(entry);
        if !link.is_vacant() {
            return;
        }
        let index = Live::new(bound.token);
        // SAFETY: Bound proves the new resident's exact live entry. Existing
        // residents came through this queue brand, and linked entries cannot recycle.
        unsafe {
            ends::Cell::push_back(depot::raw::Proof::new((&self.0, index, |tail| {
                link.set(index);
                if let Some(tail) = tail {
                    let tail = tail.bind(entries);
                    select(tail).set(index);
                }
            })));
        }
    }

    pub(in crate::net::port::table) fn front<'a, 'd, E>(
        &'a self,
        entries: &'a table::raw::Entries<'d, E, I>,
        select: impl Fn(&'a entry::Entry<'d, E, I>) -> &'a Link<I, K>,
    ) -> Option<Front<'a, 'd, E, I, K>>
    where
        K: 'a,
    {
        let topology = self.0.front()?;
        // SAFETY: every resident was bound to this stable entry slice by push,
        // and its queue-branded link prevents generation reuse before pop.
        let live = topology.get();
        let entry = unsafe { live.bind(entries) };
        let bound = Bound::from_live(live.token, entry);
        let link = select(entry);
        Some(Front {
            topology,
            bound,
            link,
        })
    }

    pub(in crate::net::port::table) fn is_ready(&self) -> bool {
        !self.0.is_empty()
    }
}

impl<I: identity::Identity, K> Link<I, K> {
    pub(in crate::net::port) const fn new() -> Self {
        Self(cell::Cell::new(None), marker::PhantomData)
    }

    pub(in crate::net::port) fn is_vacant(&self) -> bool {
        self.0.get().is_none()
    }

    fn set(&self, current: Live<I, K>) {
        self.0.set(Some(current.token));
    }

    fn take(&self, current: Live<I, K>) -> Resident<I, K> {
        self.0
            .replace(None)
            .filter(|next| *next != current.token)
            .map(Live::new)
    }
}

impl<'a, 'd, E, I: identity::Identity, K> Front<'a, 'd, E, I, K> {
    pub(in crate::net::port::table) const fn index(&self) -> indices::SlotIndex {
        self.bound.index
    }

    pub(in crate::net::port::table) fn pop(self) -> Bound<'a, 'd, E, I> {
        // SAFETY: topology owns this exact front and its branded link.
        unsafe {
            ends::Front::pop(depot::raw::Proof::new((
                self.topology,
                self.bound,
                |resident, bound| (self.link.take(resident), bound),
            )))
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<Resident<conn::Id<'static, KeyTag<0>>, Cleanup>>()
            == mem::size_of::<Option<conn::Id<'static, KeyTag<0>>>>()
    );
    assert!(
        mem::align_of::<Resident<conn::Id<'static, KeyTag<0>>, Cleanup>>()
            == mem::align_of::<Option<conn::Id<'static, KeyTag<0>>>>()
    );
};
