use std::{array, marker, mem, num};

use o3::{alloc, alloc::tail, mem::Lease};
use state::State;

use crate::driver::{
    route,
    route::{indices, table},
    schedule::{
        ready,
        ready::{
            arena::{
                layout::{Layout, TILE_SLOTS},
                payload::{Bound, Payload, Task},
                pool, set,
            },
            key::{
                DispatchKey, FreeIndex, Index, Local, ReservedKey, Staged, TaskIndex, TaskKey,
                TopologyKey,
            },
            kind::Kind,
            state,
        },
    },
    settings,
};

pub(in crate::driver::schedule::ready) enum Renewal<'d> {
    Reusable(ReservedKey<'d>),
    Retired(pool::Retirement<'d>),
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(in crate::driver::schedule::ready) struct Header {
    pub(in crate::driver::schedule::ready) descriptor_end: table::Capacity,
    pub(in crate::driver::schedule::ready) capacity: settings::Ready,
}

#[repr(C, align(64))]
pub(in crate::driver::schedule::ready) struct Tile {
    pub(in crate::driver::schedule::ready) cells: [Cell; TILE_SLOTS],
}

#[repr(C)]
pub(in crate::driver::schedule::ready) struct Cell {
    pub(in crate::driver::schedule::ready) payload: Payload,
    pub(in crate::driver::schedule::ready) state: State,
}

impl Tile {
    pub(in crate::driver::schedule::ready) fn new(layout: Layout, tile: usize) -> Self {
        Self {
            cells: array::from_fn(|lane| {
                let offset = (tile * TILE_SLOTS + lane) as u32;
                let index = layout.descriptors.raw() + offset;
                let (payload, kind) = if index >= u32::from(layout.capacity.raw()) {
                    (Payload::retired(), Kind::Retired)
                } else {
                    (Payload::free(layout.next_free(index)), Kind::Free)
                };
                Cell {
                    payload,
                    state: State::new(ready::Generation::INITIAL, kind),
                }
            }),
        }
    }
}

pub(in crate::driver::schedule::ready) struct Resident<'a, S = state::Any> {
    pub(in crate::driver::schedule::ready) cell: &'a Cell,
    pub(in crate::driver::schedule::ready) index: num::NonZero<u16>,
    pub(in crate::driver::schedule::ready) proof: marker::PhantomData<fn(S) -> S>,
}

impl<S> Clone for Resident<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for Resident<'_, S> {}

impl<'a, S> Resident<'a, S> {
    pub(in crate::driver::schedule::ready) fn payload(self) -> &'a Payload {
        &self.cell.payload
    }

    pub(in crate::driver::schedule::ready) fn state(self) -> &'a State {
        &self.cell.state
    }

    pub(in crate::driver::schedule::ready) fn generation(self) -> ready::Generation {
        self.state().generation()
    }

    pub(in crate::driver::schedule::ready) fn kind(self) -> Kind {
        self.state().kind()
    }

    pub(in crate::driver::schedule::ready) fn set_kind(self, kind: Kind) {
        self.state().set_kind(kind);
    }

    pub(in crate::driver::schedule::ready) fn set_dispatch(self, target: route::Token) {
        self.payload().set_dispatch(target);
    }

    pub(in crate::driver::schedule::ready) fn overwrite(self, payload: Payload) {
        self.payload().overwrite(payload);
    }

    pub(in crate::driver::schedule::ready) fn set_free(self, next: FreeIndex) {
        self.payload().set_free(next);
    }
}

impl<'a> Resident<'a, state::Task> {
    pub(in crate::driver::schedule::ready) fn bound(self) -> Bound<'a> {
        Bound::new(self.payload())
    }
}

impl<'a> Resident<'a, state::Topology> {
    /// Releases this root using its exact owning key.
    pub(in crate::driver::schedule::ready::arena) fn unroot(
        self,
        key: TopologyKey<'a>,
    ) -> ReservedKey<'a> {
        self.set_kind(Kind::Reserved);
        key.reserve()
    }
}

pub(in crate::driver::schedule::ready) enum Entry<'a> {
    Dispatch(ready::DynamicIndex),
    Task(Bound<'a>, TaskIndex),
}

pub(in crate::driver::schedule::ready) enum Drain {
    Descriptor(indices::SlotIndex),
    Dispatch(route::Token),
}

#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct Slots {
    pub(in crate::driver::schedule::ready) block: tail::Block<Header, Tile>,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct Mutation<'a>(
    pub(in crate::driver::schedule::ready) &'a Slots,
);

pub(in crate::driver::schedule::ready) struct Table {
    pub(in crate::driver::schedule::ready) slots: Slots,
    pub(in crate::driver::schedule::ready) pool: ready::Pool,
}

impl Table {
    pub(in crate::driver::schedule::ready) fn try_new(
        layout: Layout,
    ) -> Result<Self, alloc::Error> {
        let header = Header {
            descriptor_end: layout.descriptors,
            capacity: layout.capacity,
        };
        let memory = tail::Plan::try_new(layout.tile_count())?.try_allocate()?;
        let block = memory.fill(header, |_, index| Tile::new(layout, index));
        let slots = Slots { block };
        let pool = ready::Pool::new(layout);
        Ok(Self { slots, pool })
    }
}

impl Slots {
    pub(in crate::driver::schedule::ready) fn header(&self) -> Header {
        *tail::View::new(&self.block).header()
    }

    pub(in crate::driver::schedule::ready) fn descriptor(
        &self,
        index: ready::DynamicIndex,
    ) -> Option<indices::SlotIndex> {
        (index.get() < self.header().descriptor_end.count().get())
            .then_some(indices::SlotIndex::from(index.0.get() - 1))
    }

    pub(in crate::driver::schedule::ready) fn resolve(
        &self,
        key: ready::Stored,
    ) -> Option<Entry<'_>> {
        let index = key.index();
        let header = self.header();
        if index < header.descriptor_end.raw() || index >= u32::from(header.capacity.raw()) {
            return None;
        }
        let code = key.index_code();
        let resident = self.resident(Local(code));
        if resident.generation() != key.epoch() {
            return None;
        }
        match resident.kind() {
            Kind::Dispatch => Some(Entry::Dispatch(ready::DynamicIndex(code))),
            Kind::Task => {
                let task = Resident::<state::Task> {
                    cell: resident.cell,
                    index: code,
                    proof: marker::PhantomData,
                };
                Some(Entry::Task(task.bound(), TaskIndex::from_code(code)))
            }
            Kind::Free | Kind::Reserved | Kind::Topology | Kind::Retired => None,
        }
    }

    pub(in crate::driver::schedule::ready) fn drain(&self, index: set::Queued) -> Drain {
        let index = index.0;
        if index.get() < self.header().descriptor_end.count().get() {
            return Drain::Descriptor(indices::SlotIndex::from(index.0.get() - 1));
        }
        Drain::Dispatch(self.resident(Local(index.0)).payload().dispatch())
    }

    pub(in crate::driver::schedule::ready) fn resident<I: Index>(
        &self,
        index: I,
    ) -> Resident<'_, I::State> {
        let index = index.code();
        let raw = u32::from(index.get() - 1);
        let dynamic = (raw - self.header().descriptor_end.raw()) as usize;
        let tile = dynamic / TILE_SLOTS;
        let lane = dynamic % TILE_SLOTS;
        // SAFETY: exact index capabilities name live ready slots. `Local` is
        // created only after the arena bounds and descriptor boundary are proved.
        let tile = unsafe { tail::View::new(&self.block).as_slice().get_unchecked(tile) };
        let cell = unsafe { tile.cells.get_unchecked(lane) };
        Resident {
            cell,
            index,
            proof: marker::PhantomData,
        }
    }

    pub(in crate::driver::schedule::ready) fn mutation(&self) -> Mutation<'_> {
        Mutation(self)
    }
}

impl<'a> Mutation<'a> {
    pub(in crate::driver::schedule::ready) fn next(self, index: FreeIndex) -> FreeIndex {
        self.0.resident(index).payload().reserve()
    }

    pub(in crate::driver::schedule::ready) fn link(self, index: FreeIndex, next: FreeIndex) {
        self.0.resident(index).set_free(next);
    }

    pub(in crate::driver::schedule::ready) fn reserve(self, index: FreeIndex) -> ReservedKey<'a> {
        let resident = self.0.resident(index);
        let epoch = resident.generation();
        resident.set_kind(Kind::Reserved);
        ReservedKey::new(index, epoch)
    }

    pub(in crate::driver::schedule::ready) fn dispatch(
        self,
        key: ReservedKey<'a>,
        target: route::Token,
    ) -> DispatchKey<'a> {
        let resident = self.0.resident(&key);
        resident.set_dispatch(target);
        resident.set_kind(Kind::Dispatch);
        key.dispatch()
    }

    pub(in crate::driver::schedule::ready) fn task(
        self,
        key: ReservedKey<'a>,
        task: Task<'a>,
    ) -> Staged<'a> {
        let resident = self.0.resident(&key);
        resident.overwrite(task.into_payload());
        resident.set_kind(Kind::Task);
        key.stage()
    }

    pub(in crate::driver::schedule::ready) fn topology(
        self,
        key: ReservedKey<'a>,
    ) -> TopologyKey<'a> {
        let resident = self.0.resident(&key);
        resident.overwrite(Payload::retired());
        resident.set_kind(Kind::Topology);
        key.topology()
    }

    pub(in crate::driver::schedule::ready) fn unroute(
        self,
        key: DispatchKey<'a>,
    ) -> ReservedKey<'a> {
        let resident = self.0.resident(&key);
        resident.set_kind(Kind::Reserved);
        key.reserve()
    }

    pub(in crate::driver::schedule::ready) fn untask(self, key: TaskKey<'a>) -> ReservedKey<'a> {
        let resident = self.0.resident(&key);
        resident.set_kind(Kind::Reserved);
        key.reserve()
    }

    pub(in crate::driver::schedule::ready) fn renew(self, key: ReservedKey<'a>) -> Renewal<'a> {
        let resident = self.0.resident(&key);
        let index = FreeIndex::from_code(resident.index);
        match resident.generation().next() {
            Some(next) => {
                resident.state().set_epoch(next);
                Renewal::Reusable(ReservedKey::new(index, next))
            }
            None => {
                resident.set_kind(Kind::Retired);
                Renewal::Retired(pool::Retirement::new())
            }
        }
    }

    pub(in crate::driver::schedule::ready) fn publish(self, key: ReservedKey<'a>, next: FreeIndex) {
        let resident = self.0.resident(&key);
        resident.set_free(next);
        resident.set_kind(Kind::Free);
    }
}

const _: () = {
    assert!(mem::size_of::<Lease<DispatchKey<'static>>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Lease<DispatchKey<'static>>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<FreeIndex>() == mem::size_of::<u16>());
    assert!(mem::size_of::<Tile>() == 64);
    assert!(mem::align_of::<Tile>() == 64);
    assert!(mem::size_of::<Resident<'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Entry<'static>>() == mem::size_of::<Resident<'static>>());
    assert!(mem::size_of::<Option<Entry<'static>>>() == mem::size_of::<Entry<'static>>());
    assert!(mem::size_of::<Drain>() == 2 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Resident<'static, state::Task>>() == mem::size_of::<Resident<'static>>()
    );
    assert!(mem::size_of::<Slots>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Table>() == 2 * mem::size_of::<usize>());
};
