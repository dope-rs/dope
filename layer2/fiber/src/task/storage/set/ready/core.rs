use core::{cell, pin, task};

use keyed::lanes;
use o3::{
    depot,
    depot::{
        index::Initialized,
        queue::{indexed::keyed, links},
    },
};

use crate::{
    abi,
    context::wake,
    task::{
        storage,
        storage::{
            Handle,
            set::{
                ready,
                ready::{Queue, drain::Side},
                slot,
                slot::state::State,
                views,
            },
        },
    },
};

pub(super) struct Core<'d, D> {
    work: lanes::Fifo<Handle<'d, D>, 3, storage::Id<'d, D>>,
    active: cell::Cell<Side>,
    pub(super) paused: cell::Cell<Option<Side>>,
}

struct Plane<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> {
    view: views::View<'a, 'd, F, M, D, G>,
    token: &'b mut slot::Token<'a, F, M>,
}

pub(super) struct Resident<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> {
    owner: Plane<'b, 'a, 'd, F, M, D, G>,
    slot: pin::Pin<&'a slot::Slot<'d, F, M, D, G>>,
    key: Handle<'d, D>,
}

pub(super) struct Removal<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> {
    resident: Resident<'b, 'a, 'd, F, M, D, G>,
    discard: slot::Discardable<'a, 'd, F, M, D, G>,
    state: State,
}

pub(super) enum Kind {
    Flat,
    Grouped,
}

impl<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> Removal<'b, 'a, 'd, F, M, D, G> {
    pub(super) fn resolve(
        view: &views::View<'a, 'd, F, M, D, G>,
        token: &'b mut slot::Token<'a, F, M>,
        key: Handle<'d, D>,
    ) -> Option<Self> {
        let slot = view.resolve(key)?;
        let state = slot.state();
        if state == State::External {
            return None;
        }
        let discard = slot.discardable()?;
        Some(Self {
            resident: Resident {
                owner: Plane {
                    view: view.reborrow(),
                    token,
                },
                slot,
                key,
            },
            discard,
            state,
        })
    }

    pub(super) fn runnable(&self) -> Option<ready::raw::Index<D>> {
        matches!(self.state, State::Running | State::Queued).then(|| {
            // SAFETY: the resolved resident retains this exact initialized index.
            unsafe { ready::raw::Index::from_offset(self.resident.key.parts().index()) }
        })
    }

    pub(super) fn remove(self, kind: Kind) -> State {
        let Self {
            resident,
            discard,
            state,
        } = self;
        let Resident {
            owner: Plane { view, token },
            slot,
            key,
        } = resident;
        if state == State::Completed || matches!(kind, Kind::Flat) && state == State::Queued {
            let resident = Resident {
                owner: Plane {
                    view: view.reborrow(),
                    token: &mut *token,
                },
                slot,
                key,
            };
            // SAFETY: the retained phase proves membership in this exact flat
            // queue, while the exclusive token excludes cancellation and reuse.
            lanes::Bound::from_source(unsafe { depot::raw::Proof::new(resident) }).detach();
        }
        view.drop_slot(discard, token);
        state
    }
}

pub(in crate::task::storage::set) struct Queued<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G>(
    lanes::Bound<Resident<'b, 'a, 'd, F, M, D, G>, 3, storage::Id<'d, D>>,
);

pub(in crate::task::storage::set) struct Running<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> {
    vacant: lanes::Bound<Resident<'b, 'a, 'd, F, M, D, G>, 3, storage::Id<'d, D>, false>,
    running: slot::Running<'a, 'd, F, M, D, G>,
}

impl<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> Queued<'b, 'a, 'd, F, M, D, G> {
    pub(in crate::task::storage::set) fn wake(mut self) {
        unsafe { slot::raw::Queued::new(self.0.get()) }.wake();
    }

    pub(super) fn start(self) -> Running<'b, 'a, 'd, F, M, D, G> {
        let mut vacant = self.0.detach();
        let current = vacant.edit();
        let running = unsafe { slot::raw::Queued::new(current.slot) }.start();
        Running { vacant, running }
    }
}

impl<'d, F: abi::Fiber<'d>, M, D, G> Running<'_, '_, 'd, F, M, D, G> {
    pub(super) fn wakers(&self) -> (wake::RootWaker<'d>, wake::Waker<'d>) {
        self.running.wakers()
    }

    pub(super) fn poll(
        &mut self,
        poll: impl FnOnce(pin::Pin<&mut F>) -> task::Poll<F::Output>,
    ) -> task::Poll<F::Output> {
        poll(self.vacant.edit().owner.token.running(&self.running))
    }

    pub(super) fn poll_with(
        &mut self,
        poll: impl FnOnce(pin::Pin<&mut F>, &M) -> task::Poll<F::Output>,
    ) -> task::Poll<F::Output> {
        poll(
            self.vacant.edit().owner.token.running(&self.running),
            self.running.data(),
        )
    }

    pub(super) fn complete(mut self, output: F::Output) {
        self.vacant.edit().owner.token.finish(self.running, output);
        self.vacant.push_back::<2>();
    }
}

impl<'d, D> Core<'d, D> {
    pub(super) const fn new(work: lanes::Fifo<Handle<'d, D>, 3, storage::Id<'d, D>>) -> Self {
        Self {
            work,
            active: cell::Cell::new(Side::First),
            paused: cell::Cell::new(None),
        }
    }

    pub(super) fn enqueue<'a, F: abi::Fiber<'d>, M, G>(
        &self,
        view: &views::View<'a, 'd, F, M, D, G>,
        token: &mut slot::Token<'a, F, M>,
        index: ready::raw::Index<D>,
    ) {
        let slot = view.scheduled(index);
        if !matches!(slot.state(), State::Admitting | State::Running) {
            return;
        }
        let key = slot.current(index.raw());
        let resident = Resident {
            owner: Plane {
                view: view.reborrow(),
                token,
            },
            slot,
            key,
        };
        self.push(lanes::Bound::from_source(unsafe {
            depot::raw::Proof::new(resident)
        }));
        slot.queue_wake();
    }

    pub(super) fn insert<'a, F: abi::Fiber<'d>, M, G>(
        &self,
        view: &views::View<'a, 'd, F, M, D, G>,
        token: &mut slot::Token<'a, F, M>,
        admitted: slot::Counted<'a, 'd, F, M, D, G>,
        key: Handle<'d, D>,
    ) {
        let slot = admitted.slot();
        let resident = Resident {
            owner: Plane {
                view: view.reborrow(),
                token,
            },
            slot,
            key,
        };
        self.push(lanes::Bound::from_source(unsafe {
            depot::raw::Proof::new(resident)
        }));
        admitted.queue();
    }

    fn push<F: abi::Fiber<'d>, M, G>(
        &self,
        vacant: lanes::Bound<Resident<'_, '_, 'd, F, M, D, G>, 3, storage::Id<'d, D>, false>,
    ) {
        match self.active.get() {
            Side::First => {
                vacant.push_back::<0>();
            }
            Side::Second => {
                vacant.push_back::<1>();
            }
        }
    }

    pub(in crate::task::storage::set) fn running<'b, 'a, F: abi::Fiber<'d>, M, G>(
        view: &views::View<'a, 'd, F, M, D, G>,
        token: &'b mut slot::Token<'a, F, M>,
        index: ready::raw::Index<D>,
    ) -> Running<'b, 'a, 'd, F, M, D, G> {
        let slot = view.scheduled(index);
        let resident = Resident {
            owner: Plane {
                view: view.reborrow(),
                token,
            },
            slot,
            key: slot.current(index.raw()),
        };
        // SAFETY: owner-queue selection retains the current runnable slot;
        // its completion link remains detached until Running::complete.
        let vacant = lanes::Bound::from_source(unsafe { depot::raw::Proof::new(resident) });
        let running = unsafe { slot::raw::Queued::new(slot) }.start();
        Running { vacant, running }
    }

    pub(super) fn front<'b, 'a, F: abi::Fiber<'d>, M, G>(
        &self,
        view: &views::View<'a, 'd, F, M, D, G>,
        token: &'b mut slot::Token<'a, F, M>,
        side: Side,
    ) -> Option<Queued<'b, 'a, 'd, F, M, D, G>> {
        let owner = Plane {
            view: view.reborrow(),
            token,
        };
        let member = match side {
            Side::First => lanes::Bound::front::<0, _>(owner),
            Side::Second => lanes::Bound::front::<1, _>(owner),
        }?;
        Some(Queued(member))
    }

    pub(super) fn pop<'b, 'a, F: abi::Fiber<'d>, M, G>(
        &self,
        view: views::View<'a, 'd, F, M, D, G>,
        token: &'b mut slot::Token<'a, F, M>,
    ) -> Option<slot::Done<'b, 'a, 'd, F, M, D, G>> {
        Some(
            lanes::Bound::front::<2, _>(Plane { view, token })?
                .detach()
                .take(),
        )
    }

    pub(super) fn is_empty_on(&self, side: Side) -> bool {
        match side {
            Side::First => self.work.is_empty::<0>(),
            Side::Second => self.work.is_empty::<1>(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.work.is_empty::<0>() && self.work.is_empty::<1>()
    }

    pub(super) fn side(&self) -> Side {
        if let Some(side) = self.paused.get() {
            return side;
        }
        let side = self.active.get();
        self.active.set(side.flip());
        self.paused.set(Some(side));
        side
    }
}

impl<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G>
    AsRef<lanes::Fifo<Handle<'d, D>, 3, storage::Id<'d, D>>> for Plane<'b, 'a, 'd, F, M, D, G>
{
    fn as_ref(&self) -> &lanes::Fifo<Handle<'d, D>, 3, storage::Id<'d, D>> {
        &Queue::as_pin(&self.view.header().ready).backend().work
    }
}

// SAFETY: `project` maps a scheduler link index to the resident of this exact
// plane, whose scheduled view retains the same pinned queue and slot domain.
unsafe impl<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G>
    depot::raw::Project<links::Index, storage::Id<'d, D>> for Plane<'b, 'a, 'd, F, M, D, G>
{
    type Output = Resident<'b, 'a, 'd, F, M, D, G>;

    unsafe fn project(self, index: links::Index) -> Self::Output {
        // SAFETY: the link index was emitted by this plane's scheduled queue,
        // so it names an in-bounds slot of the retained view.
        let index = unsafe { ready::raw::Index::from_offset(index.raw()) };
        let slot = self.view.scheduled(index);
        Resident {
            key: slot.current(index.raw()),
            slot,
            owner: self,
        }
    }
}

#[repr(transparent)]
pub(super) struct Completed<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G>(
    Resident<'b, 'a, 'd, F, M, D, G>,
);

// SAFETY: lane 0 holds only scheduled residents of this plane's exact key domain.
unsafe impl<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> depot::raw::LaneSelect<3, 0, storage::Id<'d, D>>
    for Plane<'b, 'a, 'd, F, M, D, G>
{
    type Key = Handle<'d, D>;
    type Slot = Resident<'b, 'a, 'd, F, M, D, G>;
}

// SAFETY: lane 1 holds only scheduled residents of this plane's exact key domain.
unsafe impl<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> depot::raw::LaneSelect<3, 1, storage::Id<'d, D>>
    for Plane<'b, 'a, 'd, F, M, D, G>
{
    type Key = Handle<'d, D>;
    type Slot = Resident<'b, 'a, 'd, F, M, D, G>;
}

// SAFETY: lane 2 selects the same resident wrapped as the completed slot.
unsafe impl<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> depot::raw::LaneSelect<3, 2, storage::Id<'d, D>>
    for Plane<'b, 'a, 'd, F, M, D, G>
{
    type Key = Handle<'d, D>;
    type Slot = Completed<'b, 'a, 'd, F, M, D, G>;
}

// SAFETY: the exclusive token retains this resident's exact key and pinned link plane.

unsafe impl<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> depot::raw::KeyOwner<storage::Id<'d, D>>
    for Resident<'b, 'a, 'd, F, M, D, G>
{
    type Key = Handle<'d, D>;
    type Fifo = lanes::Fifo<Self::Key, 3, storage::Id<'d, D>>;
    fn fifo(&self) -> &lanes::Fifo<Self::Key, 3, storage::Id<'d, D>> {
        self.owner.as_ref()
    }
    fn parts(&self) -> (Self::Key, &keyed::Link<Self::Key, storage::Id<'d, D>>) {
        (self.key, &self.slot.get_ref().link)
    }
    fn entry<'c>(
        live: depot::raw::Proof<(&'c Self, links::Index), Initialized<storage::Id<'d, D>>>,
    ) -> keyed::Entry<'c, Self::Key, storage::Id<'d, D>> {
        let live = unsafe { live.map(|(resident, index)| (&resident.owner.view, index)) };
        views::View::entry(live)
    }
}

impl<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> lanes::Access<storage::Id<'d, D>>
    for Resident<'b, 'a, 'd, F, M, D, G>
{
    type Read<'c>
        = pin::Pin<&'c slot::Slot<'d, F, M, D, G>>
    where
        Self: 'c;
    type Edit<'c>
        = &'c mut Self
    where
        Self: 'c;

    fn get(&mut self) -> Self::Read<'_> {
        self.slot
    }
    fn edit(&mut self) -> Self::Edit<'_> {
        self
    }
}

unsafe impl<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> depot::raw::KeyOwner<storage::Id<'d, D>>
    for Completed<'b, 'a, 'd, F, M, D, G>
{
    type Key = Handle<'d, D>;
    type Fifo = lanes::Fifo<Self::Key, 3, storage::Id<'d, D>>;
    fn fifo(&self) -> &lanes::Fifo<Self::Key, 3, storage::Id<'d, D>> {
        self.0.owner.as_ref()
    }
    fn parts(&self) -> (Self::Key, &keyed::Link<Self::Key, storage::Id<'d, D>>) {
        depot::raw::KeyOwner::parts(&self.0)
    }
    fn entry<'c>(
        live: depot::raw::Proof<(&'c Self, links::Index), Initialized<storage::Id<'d, D>>>,
    ) -> keyed::Entry<'c, Self::Key, storage::Id<'d, D>> {
        let live = unsafe { live.map(|(owner, index)| (&owner.0, index)) };
        depot::raw::KeyOwner::entry(live)
    }
}

unsafe impl<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> depot::raw::KeyFinish<false, storage::Id<'d, D>>
    for Completed<'b, 'a, 'd, F, M, D, G>
{
    type Output = slot::Done<'b, 'a, 'd, F, M, D, G>;
    unsafe fn finish(self) -> Self::Output {
        let source = self.0;
        slot::Done::new(
            source.owner.view,
            source.owner.token,
            source.slot,
            source.key,
        )
    }
}

impl<'b, 'a, 'd, F: abi::Fiber<'d>, M, D, G> From<Resident<'b, 'a, 'd, F, M, D, G>>
    for Completed<'b, 'a, 'd, F, M, D, G>
{
    fn from(resident: Resident<'b, 'a, 'd, F, M, D, G>) -> Self {
        Self(resident)
    }
}
