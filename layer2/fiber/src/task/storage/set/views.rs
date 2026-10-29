use core::{mem, pin};

use o3::{
    depot,
    depot::{
        index::Initialized,
        queue::{indexed::keyed, links},
    },
};

use crate::{
    abi,
    task::{
        storage,
        storage::{
            Handle, phase, set,
            set::{
                Allocation, Borrowed,
                group::order,
                pools::Header,
                ready, slot,
                slot::{Slot, state},
            },
        },
    },
};

type Selected<'s, 'a, 'd, F, M, D, G> = depot::raw::Proof<
    (&'s View<'a, 'd, F, M, D, G>, links::Index),
    Initialized<storage::Id<'d, D>>,
>;

/// A stable, lifetime-bound view of a task set's intrusive topology.
#[repr(transparent)]
pub(crate) struct View<'a, 'd, F: abi::Fiber<'d>, M, D, G = ()> {
    view: Borrowed<'a, 'd, F, M, D, G>,
}

#[repr(transparent)]
struct Projected<'s, 'a, 'd, F: abi::Fiber<'d>, M, D, G>(Selected<'s, 'a, 'd, F, M, D, G>);

// SAFETY: the live view retains this exact pinned slot, key generation, and link plane.
unsafe impl<'s, 'a, 'd, F: abi::Fiber<'d>, M, D, G> depot::raw::KeyBorrow<'s, storage::Id<'d, D>>
    for Projected<'s, 'a, 'd, F, M, D, G>
{
    type Key = Handle<'d, D>;

    fn parts(self) -> (Self::Key, &'s keyed::Link<Self::Key, storage::Id<'d, D>>) {
        let index = self.0.key().raw();
        let slot = View::project(self.0);
        (slot.current(index), &slot.get_ref().link)
    }
}

pub(in crate::task::storage) struct Resident<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G = ()> {
    view: &'b View<'a, 'd, F, M, D, G>,
    order: &'b order::Order<'d, D>,
}

pub(in crate::task::storage::set) struct Vacant<'a, 'd, F: abi::Fiber<'d>, M, D, G = ()> {
    header: &'a Header<'d, F, M, D, G>,
    slot: pin::Pin<&'a Slot<'d, F, M, D, G>>,
    index: ready::raw::Index<D>,
}

impl<'a, 'd, F: abi::Fiber<'d>, M, D, G> Vacant<'a, 'd, F, M, D, G> {
    pub(in crate::task::storage::set) fn index(&self) -> ready::raw::Index<D> {
        self.index
    }

    pub(in crate::task::storage::set) fn acquire(
        self,
    ) -> (slot::Admitting<'a, 'd, F, M, D, G>, Handle<'d, D>) {
        let vacant = mem::ManuallyDrop::new(self);
        (vacant.slot.begin(), vacant.slot.current(vacant.index.raw()))
    }
}

impl<'d, F: abi::Fiber<'d>, M, D, G> Drop for Vacant<'_, 'd, F, M, D, G> {
    fn drop(&mut self) {
        self.slot
            .free
            .set(self.header.free.get().unwrap_or(self.index));
        self.header.free.set(Some(self.index));
    }
}

impl<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> Resident<'b, 'a, 'd, F, M, D, G> {
    pub(in crate::task::storage::set) fn front(
        view: &'b View<'a, 'd, F, M, D, G>,
        order: &'b order::Order<'d, D>,
    ) -> Selected<'b, 'a, 'd, F, M, D, G> {
        // SAFETY: Order is published only after its first completed slot is linked.
        // Its exclusive entry is retired within the last detach, before it can be borrowed again.
        let source = unsafe { depot::raw::Proof::new(Self { view, order }) };
        let live = keyed::Fifo::front(source);
        // SAFETY: the order retains its initialized endpoint in this exact view.
        unsafe { live.map(|(source, index)| (source.view, index)) }
    }
}

// SAFETY: each retained order owns its listed slots in this exact storage view.
unsafe impl<'d, F: abi::Fiber<'d>, M, D, G> depot::raw::KeySource<storage::Id<'d, D>>
    for Resident<'_, '_, 'd, F, M, D, G>
{
    type Key = Handle<'d, D>;

    fn fifo(&self) -> &keyed::Fifo<Self::Key, storage::Id<'d, D>> {
        &self.order.fifo
    }
}

impl<'a, 'd, F: abi::Fiber<'d>, M, D, G> View<'a, 'd, F, M, D, G> {
    pub(in crate::task::storage::set) fn new(pool: &'a Allocation<'d, F, M, D, G>) -> Self {
        Self {
            view: Borrowed::new(pool),
        }
    }

    pub(in crate::task::storage::set) fn reborrow(&self) -> Self {
        Self { view: self.view }
    }

    pub(in crate::task::storage::set) fn scheduled(
        &self,
        index: ready::raw::Index<D>,
    ) -> pin::Pin<&'a Slot<'d, F, M, D, G>> {
        // SAFETY: scheduling admits this exact in-bounds index of the fixed pool.
        unsafe { self.at(index.raw() as usize) }
    }

    /// Projects a structurally pinned slot in this view's caller-owned tail block.
    /// # Safety
    /// `index` is inside this view's initialized extent.
    unsafe fn at(&self, index: usize) -> pin::Pin<&'a Slot<'d, F, M, D, G>> {
        // SAFETY: the slot belongs to the pinned view, so pinning is structural.
        unsafe { pin::Pin::new_unchecked(self.view.as_slice().get_unchecked(index)) }
    }

    /// Projects the exact pinned slot at `index`, or `None` outside this pool.
    fn get(&self, index: usize) -> Option<pin::Pin<&'a Slot<'d, F, M, D, G>>> {
        // SAFETY: the bound check proves `index` is inside this fixed pool.
        (index < self.view.len()).then(|| unsafe { self.at(index) })
    }

    pub(in crate::task::storage::set) fn header(&self) -> &'a Header<'d, F, M, D, G> {
        self.view.header()
    }

    pub(in crate::task::storage::set) fn vacant(&self) -> Option<Vacant<'a, 'd, F, M, D, G>> {
        let header = self.header();
        let index = header.free.get()?;
        // SAFETY: the free list stores only indices of this fixed pool.
        let slot = unsafe { self.at(index.raw() as usize) };
        let next = slot.free.replace(index);
        header.free.set((next != index).then_some(next));
        Some(Vacant {
            header,
            slot,
            index,
        })
    }

    pub(in crate::task::storage::set) fn read<R>(
        &self,
        key: Handle<'d, D>,
        inspect: impl for<'m> FnOnce(&'m M) -> R,
    ) -> Option<R> {
        let slot = self.resolve(key)?;
        Some(inspect(slot.data()))
    }

    pub(in crate::task::storage::set) fn inspect<R>(
        &self,
        key: Handle<'d, D>,
        inspect: impl for<'m> FnOnce(&'m M, phase::State) -> R,
    ) -> Option<R> {
        let slot = self.resolve(key)?;
        let phase = match slot.state() {
            state::State::Admitting => phase::State::Admitting,
            state::State::Running => phase::State::Running,
            state::State::Queued => phase::State::Queued,
            state::State::Completed => phase::State::Completed,
            state::State::External => phase::State::External,
            state::State::Free | state::State::Retired | state::State::Retiring => return None,
        };
        Some(inspect(slot.data(), phase))
    }

    pub(in crate::task::storage::set) fn project(
        live: Selected<'_, 'a, 'd, F, M, D, G>,
    ) -> pin::Pin<&'a Slot<'d, F, M, D, G>> {
        // SAFETY: the live key was admitted by this exact fixed pool.
        unsafe { live.source().at(live.key().raw() as usize) }
    }

    pub(in crate::task::storage::set) fn entry<'s>(
        live: Selected<'s, 'a, 'd, F, M, D, G>,
    ) -> keyed::Entry<'s, Handle<'d, D>, storage::Id<'d, D>> {
        keyed::Entry::from_source(Projected(live))
    }

    pub(in crate::task::storage::set) fn try_wake(
        &self,
        key: Handle<'d, D>,
    ) -> Result<(), set::WakeError> {
        self.get(key.index() as usize)
            .ok_or(set::WakeError::Missing)?
            .try_wake(key)
    }

    pub(in crate::task::storage::set) fn resolve(
        &self,
        key: Handle<'d, D>,
    ) -> Option<pin::Pin<&'a Slot<'d, F, M, D, G>>> {
        let slot = self.get(key.index() as usize)?;
        if !slot.matches(key.parts()) {
            return None;
        }
        Some(slot)
    }

    pub(in crate::task::storage::set) fn drop_slot(
        &self,
        entry: slot::Discardable<'_, 'd, F, M, D, G>,
        token: &mut slot::Token<'_, F, M>,
    ) {
        let entry = entry.discard(token);
        self.release(entry);
    }

    pub(in crate::task::storage::set) fn clear(&self, token: &mut slot::Token<'_, F, M>) {
        for index in 0..self.view.len() {
            // SAFETY: the fixed pool view contains this exact offset, and its
            // key representation bounds the allocation to the u32 index domain.
            let index = unsafe { ready::raw::Index::from_offset(index as u32) };
            let entry = self.scheduled(index);
            if let Some(drop) = entry.discardable() {
                drop.discard(token);
            }
        }
    }

    pub(in crate::task::storage::set) fn release(&self, entry: slot::Vacated<'_, 'd, F, M, D, G>) {
        let entry = self.header().retire(entry);
        entry.recycle(&self.header().free);
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<View<'static, 'static, abi::Ready<()>, (), ()>>() == word);
    assert!(
        mem::size_of::<Resident<'static, 'static, 'static, abi::Ready<()>, (), ()>>() == 2 * word
    );
    assert!(mem::size_of::<Vacant<'static, 'static, abi::Ready<()>, (), ()>>() == 3 * word);
};
