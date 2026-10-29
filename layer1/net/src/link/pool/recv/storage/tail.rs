use std::{mem, ops::Deref};

use dope_core::driver::{
    receive::credit,
    route::{
        self, Identity, KeyTag,
        indices::SlotIndex,
        table::{self, Slab, borrows, conn, entries::occupied},
    },
};
use o3::{
    depot::{
        arena::{linked::slots, slab::external::errors},
        queue::pool,
    },
    mem::{Lease, lease},
};

use crate::{
    link::{
        pool::{
            Key,
            recv::{
                self, input,
                storage::{self, Seal},
            },
        },
        slot::types,
    },
    wire,
    wire::receive::{Chunk, mode::Disabled},
};

/// Proof that one resident connection's selected tail state has been cleared.
#[repr(transparent)]
pub struct Cleared<'a, T, Tag: route::Tag> {
    entry: occupied::Entry<'a, T, Tag>,
}

impl<T, Tag: route::Tag> Cleared<'_, T, Tag> {
    pub(in crate::link::pool) fn remove(self) -> T {
        self.entry.remove()
    }

    pub(in crate::link::pool) fn try_remove<R, E>(
        self,
        take: impl FnOnce(T) -> Result<R, (T, E)>,
    ) -> Result<R, (Self, E)> {
        self.entry
            .try_remove(take)
            .map_err(|(entry, error)| (Self { entry }, error))
    }
}

impl<'a, T, Tag: route::Tag> Deref for Cleared<'a, T, Tag> {
    type Target = occupied::Entry<'a, T, Tag>;

    fn deref(&self) -> &Self::Target {
        &self.entry
    }
}

impl<'a, T, Tag: route::Tag> Cleared<'a, T, Tag> {
    pub(in crate::link::pool) fn get_mut(&mut self) -> &mut T {
        self.entry.get_mut()
    }
}

/// Connection slab and its statically selected tail storage.
pub struct Table<T, Tag: route::Tag, S, R = ()> {
    slab: Slab<T, Tag>,
    tail: S,
    storage: R,
}

impl<T, Tag: route::Tag, S, R> Table<T, Tag, S, R> {
    pub(in crate::link::pool) fn access(&mut self) -> borrows::Access<'_, T, Tag> {
        self.slab.access()
    }

    pub(in crate::link::pool) fn vacancy(&mut self) -> borrows::Vacancy<'_, T, Tag> {
        self.slab.vacancy()
    }
}

impl<T, Tag: route::Tag, S, R> Deref for Table<T, Tag, S, R> {
    type Target = Slab<T, Tag>;

    fn deref(&self) -> &Self::Target {
        &self.slab
    }
}

impl<T, ID: Identity, S> Table<T, ID, S> {
    pub(in crate::link::pool) fn try_storage<'d, P: storage::raw::Mode>(
        self,
        storage: recv::State<'d, ID, P>,
    ) -> crate::Result<Table<T, ID, S, recv::State<'d, ID, P>>> {
        let entries = self.slab.view().capacity().count().get();
        if let storage::Extent::Lanes(lanes) = P::extent(&storage.storage)
            && lanes != entries
        {
            return Err(crate::Resource::Config(
                crate::ConfigError::ReceiveCapacity { entries, lanes },
            ));
        }
        let lanes = storage.rearm.capacity();
        if lanes != entries {
            return Err(crate::Resource::Config(crate::ConfigError::RearmCapacity {
                entries,
                lanes,
            }));
        }
        Ok(Table {
            slab: self.slab,
            tail: self.tail,
            storage,
        })
    }
}

impl<'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec + 'w + 'd, U, S, P: storage::raw::Mode>
    Table<types::Connection<'w, 'd, ID, W, U>, ID, S, recv::State<'d, ID, P>>
{
    pub(in crate::link::pool) fn get<'a>(
        &'a mut self,
        parts: table::Parts<ID>,
    ) -> Option<Borrowed<'a, 'd, ID, P, &'a mut types::Connection<'w, 'd, ID, W, U>>> {
        let entry = self.slab.access().get(parts)?;
        // SAFETY: construction pairs all fixed extents; this resident supplies the exact key.
        use o3::depot::raw::Proof;
        let bound = unsafe { Proof::new((&mut self.storage, entry.key)) }.into();
        Some((entry, bound))
    }

    pub(in crate::link::pool) fn try_entry<'a>(
        &'a mut self,
        parts: table::Parts<ID>,
    ) -> Result<Occupied<'a, 'w, 'd, ID, W, U, P>, errors::Lookup<route::Epoch, ID>> {
        let entry = self.slab.access().try_occupied(parts)?;
        // SAFETY: the occupied entry belongs to this table's validated fixed domain.
        use o3::depot::raw::Proof;
        let bound = unsafe { Proof::new((&mut self.storage, entry.get().key)) }.into();
        Ok((entry, bound))
    }

    pub(in crate::link::pool) fn pending(&self) -> usize {
        self.storage.rearm.len()
    }

    pub(in crate::link::pool) fn pop(&mut self) -> Option<Key<'d, ID>> {
        self.storage.rearm.pop_front()
    }
}

impl<'d, T, ID: Identity, S, P: storage::raw::Mode> Table<T, ID, S, recv::State<'d, ID, P>> {
    pub(in crate::link::pool) fn read(&self, parts: table::Parts<ID>) -> Option<(&T, bool)> {
        use o3::depot::raw::Proof;

        let entry = self.slab.view().get(parts)?;
        // SAFETY: try_storage paired the fixed extents; this lookup admits the exact index.
        let slot =
            unsafe { Proof::new((&self.storage.storage, parts.slot().index().into_usize())) };
        let lane = P::has_queued(slot);
        Some((entry, lane))
    }
}

impl<'d, T, ID: Identity, S, P> Table<T, ID, S, recv::State<'d, ID, P>>
where
    P: storage::raw::Mode<Storage<'d> = storage::Retained<'d>>,
{
    pub(in crate::link::pool) fn credit(&self) -> &credit::Storage<'d> {
        &self.storage.storage.credit
    }
}

/// Statically selected direct or deferred tail storage.
pub trait Store: Seal + Sized {
    fn try_new<T, Tag: route::Tag>(cap: conn::Capacity) -> crate::Result<Table<T, Tag, Self>>;

    fn clear<'a, 'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec + 'w + 'd, T>(
        slot: Slot<'a, types::Connection<'w, 'd, ID, W, T>, ID, Self>,
    ) -> Cleared<'a, types::Connection<'w, 'd, ID, W, T>, ID>;
}

impl Store for () {
    fn try_new<T, Tag: route::Tag>(cap: conn::Capacity) -> crate::Result<Table<T, Tag, Self>> {
        Ok(Table {
            slab: Slab::try_new(cap.into()).map_err(crate::Resource::Allocation)?,
            tail: (),
            storage: (),
        })
    }

    fn clear<'a, 'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec + 'w + 'd, T>(
        slot: Slot<'a, types::Connection<'w, 'd, ID, W, T>, ID, Self>,
    ) -> Cleared<'a, types::Connection<'w, 'd, ID, W, T>, ID> {
        Cleared { entry: slot.entry }
    }
}

#[repr(transparent)]
pub struct Taken<'a, 'w, 'd: 'w, W: wire::Spec + 'w> {
    removed: Detached<'a, 'w, 'd, W>,
}

/// One deferred tail lane and one backing entry per connection.
pub struct Queued<'w, 'd: 'w, W: wire::Spec + 'w> {
    chunks: pool::Lanes<Chunk<'w, 'd, W, Disabled>>,
}

impl<'w, 'd: 'w, W: wire::Spec + 'w> Queued<'w, 'd, W> {
    fn try_new(conns: conn::Capacity) -> crate::Result<Self> {
        let count = conns.entries().get();
        let plan = pool::Plan::new(count, count)
            .map_err(crate::ConfigError::ReceiveLayout)
            .map_err(crate::Resource::Config)?;
        Ok(Self {
            chunks: pool::Lanes::try_new(plan).map_err(crate::Resource::Allocation)?,
        })
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w> Seal for Queued<'w, 'd, W> {}

impl<'w, 'd: 'w, W: wire::Spec + 'w> Store for Queued<'w, 'd, W> {
    fn try_new<T, Tag: route::Tag>(cap: conn::Capacity) -> crate::Result<Table<T, Tag, Self>> {
        Ok(Table {
            slab: Slab::try_new(cap.into()).map_err(crate::Resource::Allocation)?,
            tail: Self::try_new(cap)?,
            storage: (),
        })
    }

    fn clear<'a, 'v, 'owner: 'v, ID: Identity + 'v, V: wire::Spec + 'v + 'owner, T>(
        slot: Slot<'a, types::Connection<'v, 'owner, ID, V, T>, ID, Self>,
    ) -> Cleared<'a, types::Connection<'v, 'owner, ID, V, T>, ID> {
        let (entry, mut chunks) = slot.split();
        chunks.clear();
        Cleared { entry }
    }
}

impl<'a, 'w, 'd: 'w, W: wire::Spec + 'w> Taken<'a, 'w, 'd, W> {
    pub(in crate::link::pool::recv) fn new(removed: Detached<'a, 'w, 'd, W>) -> Self {
        Self { removed }
    }

    pub fn commit(self) -> Chunk<'w, 'd, W, Disabled> {
        self.removed.into_parts().1
    }
}

const _: () = {
    assert!(
        mem::size_of::<Cleared<'static, (), KeyTag<0>>>()
            == mem::size_of::<occupied::Entry<'static, (), KeyTag<0>>>()
    );
    assert!(
        mem::align_of::<Cleared<'static, (), KeyTag<0>>>()
            == mem::align_of::<occupied::Entry<'static, (), KeyTag<0>>>()
    );
    assert!(mem::size_of::<Table<(), KeyTag<0>, ()>>() == mem::size_of::<Slab<(), KeyTag<0>>>());
    assert!(mem::align_of::<Table<(), KeyTag<0>, ()>>() == mem::align_of::<Slab<(), KeyTag<0>>>());
};

pub struct Slot<'a, T, Tag: route::Tag, S> {
    entry: occupied::Entry<'a, T, Tag>,
    tail: &'a mut S,
}

impl<'a, T, Tag: route::Tag, S> Deref for Slot<'a, T, Tag, S> {
    type Target = occupied::Entry<'a, T, Tag>;

    fn deref(&self) -> &Self::Target {
        &self.entry
    }
}

impl<T, Tag: route::Tag, S> Slot<'_, T, Tag, S> {
    pub(in crate::link::pool) fn get_mut(&mut self) -> &mut T {
        self.entry.get_mut()
    }
}

impl<'a, T, Tag: route::Tag, S> Slot<'a, T, Tag, S> {
    pub(in crate::link::pool) fn new<R>(
        table: &'a mut Table<T, Tag, S, R>,
        parts: table::Parts<Tag>,
    ) -> Option<Self> {
        let entry = table.slab.access().occupied(parts)?;
        Some(Self {
            entry,
            tail: &mut table.tail,
        })
    }

    pub(in crate::link::pool::recv) fn queue<'w, 'd: 'w, W: wire::Spec + 'w, M>(
        self,
    ) -> Slot<'a, T, Tag, Queued<'w, 'd, W>>
    where
        M: input::Replay<W> + input::Kind<W, Tail<'w, 'd> = S>,
    {
        Slot {
            entry: self.entry,
            tail: M::queue(self.tail),
        }
    }
}

impl<'a, 'w, 'd: 'w, T, Tag: route::Tag, W: wire::Spec + 'w> Slot<'a, T, Tag, Queued<'w, 'd, W>> {
    pub(in crate::link::pool::recv) fn split(
        self,
    ) -> (
        occupied::Entry<'a, T, Tag>,
        pool::Lane<'a, Chunk<'w, 'd, W, Disabled>>,
    ) {
        use o3::depot::raw::Proof;

        let Self { entry, tail } = self;
        // SAFETY: Table retains both fixed allocations and never lends either
        // owner mutably. The occupied slot belongs to this original table.
        let chunks =
            unsafe { Proof::new((&mut tail.chunks, entry.key().slot().index().get() as usize)) }
                .into();
        (entry, chunks)
    }
}

impl<'a, 'w: 'a, 'd: 'w, ID: Identity + 'w, W: wire::Spec + 'w + 'd, U, S>
    Slot<'a, types::Connection<'w, 'd, ID, W, U>, ID, S>
{
    pub(in crate::link::pool) fn receive<P: storage::raw::Mode>(
        table: &'a mut Table<types::Connection<'w, 'd, ID, W, U>, ID, S, recv::State<'d, ID, P>>,
        parts: table::Parts<ID>,
    ) -> Option<Borrowed<'a, 'd, ID, P, Self>> {
        let entry = table.slab.access().occupied(parts)?;
        // SAFETY: Table owns matching fixed extents and this exact occupied resident.
        use o3::depot::raw::Proof;
        let bound = unsafe { Proof::new((&mut table.storage, entry.get().key)) }.into();
        Some((
            Self {
                entry,
                tail: &mut table.tail,
            },
            bound,
        ))
    }

    pub(in crate::link::pool) fn at<P: storage::raw::Mode>(
        table: &'a mut Table<types::Connection<'w, 'd, ID, W, U>, ID, S, recv::State<'d, ID, P>>,
        index: SlotIndex,
    ) -> Option<Borrowed<'a, 'd, ID, P, Self>> {
        let entry = table.slab.access().at(index)?;
        // SAFETY: the occupied resident comes from this same paired table.
        use o3::depot::raw::Proof;
        let bound = unsafe { Proof::new((&mut table.storage, entry.get().key)) }.into();
        Some((
            Self {
                entry,
                tail: &mut table.tail,
            },
            bound,
        ))
    }
}

type Detached<'a, 'w, 'd, W> = Lease<
    slots::Vacant<Chunk<'w, 'd, W, Disabled>, slots::Bound<'a, Chunk<'w, 'd, W, Disabled>>>,
    lease::Open,
>;
type Occupied<'a, 'w, 'd, ID, W, U, P> = (
    occupied::Entry<'a, types::Connection<'w, 'd, ID, W, U>, ID>,
    recv::Borrow<'a, 'd, ID, P>,
);

type Borrowed<'a, 'd, ID, P, T> = (T, recv::Borrow<'a, 'd, ID, P>);
