use std::{mem, num::NonZero, pin};

use o3::{
    alloc::Error,
    depot,
    depot::queue::{links, round::Join},
};

use crate::{
    core::driver::schedule::{
        app,
        ready::{
            task,
            task::{
                owner,
                topology::{Edge, Slot},
            },
        },
    },
    task::storage::fixed::schedule::{
        queue,
        queue::{OwnerError, RetireError},
        ready::{Global, linked},
    },
};

mod extent;
pub(in crate::task::storage) mod raw;
mod store;
use store::{Hooks, Owner, OwnerHooks, Store};

#[pin_project::pin_project(!Unpin)]
pub(in crate::task::storage) struct Queue<'d, Source> {
    store: Store<Source>,
    #[pin]
    slot: Slot<'d, Global>,
}

pub(in crate::task::storage) struct Entry<'a, 'd, Source, I = Global> {
    pub(in crate::task::storage::fixed::schedule) queue: pin::Pin<&'a Queue<'d, Source>>,
    pub(in crate::task::storage::fixed::schedule) index: I,
    pub(in crate::task::storage::fixed::schedule) lane: owner::Lane,
}

enum Front<'a, 'd, Source> {
    Ready(Entry<'a, 'd, Source>),
    Pending,
}

enum Resident<'a, 'd, Source> {
    Linked(Entry<'a, 'd, Source>),
    Idle(Dequeued<'a, 'd, Source>),
}

pub struct Prepared<'a, 'd, Source, I = Global>(Entry<'a, 'd, Source, I>);

#[must_use = "enqueue, idle, or detach the resident"]
pub struct Dequeued<'a, 'd, Source, I = Global>(
    pub(in crate::task::storage::fixed::schedule) Entry<'a, 'd, Source, I>,
);

impl<'a, 'd, Source> Prepared<'a, 'd, Source> {
    /// Transfers this exact reservation to the parked resident, which
    /// `Node::detach` returns once the resident leaves its lane.
    pub(in crate::task::storage) fn attach(self) -> Dequeued<'a, 'd, Source> {
        let prepared = mem::ManuallyDrop::new(self);
        let entry = Entry {
            queue: prepared.0.queue,
            index: prepared.0.index,
            lane: prepared.0.lane,
        };
        let node = entry.queue.store.node(entry.index);
        node.lane.set(entry.lane);
        // SAFETY: Prepared retains the sole detached resident transition.
        links::Detached::from_source(unsafe {
            depot::raw::Proof::new((&node.hook, key(entry.index)))
        })
        .park();
        Dequeued(entry)
    }
}

impl<Source, I> Drop for Prepared<'_, '_, Source, I> {
    fn drop(&mut self) {
        let state = self.0.queue.store.owner(self.0.lane);
        state.bound.set(state.bound.get() - 1);
    }
}

impl<'a, 'd, Source> Prepared<'a, 'd, Source, ()> {
    pub(in crate::task::storage) fn try_prepare(
        self,
        index: Global,
        parent: task::Parent<'d>,
    ) -> Result<(Edge<'a, 'd, Global>, Prepared<'a, 'd, Source>), task::Error> {
        let queue = self.0.queue;
        if index.get() >= queue.store.block.extent().tasks.get() as usize {
            return Err(task::Error::Capacity {
                requested: NonZero::<usize>::MIN,
                available: 0,
            });
        }
        let edge = queue.get_ref().slot.try_attach(parent, index)?;
        let reserved = mem::ManuallyDrop::new(self);
        Ok((
            edge,
            Prepared(Entry {
                queue,
                index,
                lane: reserved.0.lane,
            }),
        ))
    }
}

impl<Source> Dequeued<'_, '_, Source> {
    pub(in crate::task::storage) fn index(&self) -> Global {
        self.0.index
    }

    pub(in crate::task::storage) fn enqueue(self) {
        let entry = self.0;
        let store = &entry.queue.store;
        store
            .owner(entry.lane)
            .enqueue(store, entry.lane, entry.index);
    }

    pub(in crate::task::storage) fn idle(self) {
        let node = self.0.queue.store.node(self.0.index);
        // SAFETY: Dequeued owns this exact parked resident outside the topology.
        links::Detached::from_source(unsafe {
            depot::raw::Proof::new((&node.hook, key(self.0.index)))
        })
        .idle();
    }

    pub(in crate::task::storage) fn detach(self) {
        let entry = self.0;
        let store = &entry.queue.store;
        store
            .node(entry.index)
            .detach(store.owner(entry.lane), entry.index);
    }
}

impl<'a, 'd, Source> Entry<'a, 'd, Source> {
    pub(in crate::task::storage) fn index(&self) -> Global {
        self.index
    }

    fn dequeue(self) -> Dequeued<'a, 'd, Source> {
        let store = &self.queue.store;
        let state = store.owner(self.lane);
        let hooks = Hooks(store);
        let owners = OwnerHooks(store);
        // SAFETY: Front retains this FIFO's head through callback-free admission
        // and append-only ingress synchronization before this consuming transition.
        Join::head::<true>(unsafe {
            depot::raw::Proof::new((state.join(&hooks, &owners, self.lane), key(self.index)))
        })
        .park();
        Dequeued(self)
    }
}

impl<'d, Source> Queue<'d, Source> {
    pub(in crate::task::storage) fn try_new(
        tasks: NonZero<u32>,
        owners: owner::Capacity,
    ) -> Result<Self, Error> {
        Ok(Self {
            store: Store::try_new(tasks, owners)?,
            slot: Slot::new(),
        })
    }

    /// The topology front retains admission; exclusive selection excludes a parked claim.
    pub(in crate::task::storage) fn sync(self: pin::Pin<&Self>) -> Option<Entry<'_, 'd, Source>> {
        let index = self.slot.pop()?;
        let entry = match self.resident(index) {
            Resident::Linked(entry) => entry,
            Resident::Idle(dequeued) => {
                let entry = dequeued.0;
                self.store
                    .owner(entry.lane)
                    .enqueue(&self.store, entry.lane, index);
                entry
            }
        };
        Some(entry)
    }

    pub(in crate::task::storage) fn try_prepare<'a>(
        self: pin::Pin<&'a Self>,
        index: Global,
        owner: owner::Owner<Source>,
        parent: task::Parent<'d>,
    ) -> Result<(Edge<'a, 'd, Global>, Prepared<'a, 'd, Source>), queue::Error<Source>> {
        let state = self.current(owner).map_err(queue::Error::Owner)?;
        self.reserve(owner.lane(), state)
            .try_prepare(index, parent)
            .map_err(queue::Error::Task)
    }

    /// The occupied entry's exclusive route loan retains its admission to this queue.
    pub(in crate::task::storage) fn detach(self: pin::Pin<&Self>, entry: pin::Pin<&linked::Entry>) {
        // SAFETY: the occupied entry's exclusive route retains attachment to this queue.
        unsafe { raw::detach(self, entry.index()) };
    }

    /// Registers `owner`'s lane for its generation, classifying the exact reasons an
    /// unadmitted identity cannot be registered: a foreign lane, or a generation that
    /// another registration already claimed.
    pub(in crate::task::storage) fn register(
        self: pin::Pin<&Self>,
        owner: owner::Owner<Source>,
    ) -> Result<(), OwnerError<Source>> {
        let state = self
            .store
            .get(owner.lane())
            .ok_or(OwnerError::Foreign { owner })?;
        Self::bind_state(state, owner)
    }

    fn bind_state(state: &Owner, owner: owner::Owner<Source>) -> Result<(), OwnerError<Source>> {
        let epoch = owner.generation();
        match state.generation.get() {
            None => {
                state.generation.set(Some(epoch));
                Ok(())
            }
            Some(current) if current == epoch => Ok(()),
            Some(current) => Err(OwnerError::Stale {
                owner,
                current: Some(current),
            }),
        }
    }

    /// Rebinds a lane only after all admitted and prepared attachments have left.
    pub(in crate::task::storage) fn rebind<'a>(
        self: pin::Pin<&'a Self>,
        owner: owner::Owner<Source>,
    ) -> Result<Prepared<'a, 'd, Source, ()>, OwnerError<Source>> {
        let state = self
            .store
            .get(owner.lane())
            .ok_or(OwnerError::Foreign { owner })?;
        if state.bound.get() == 0 {
            state.generation.set(Some(owner.generation()));
        } else {
            Self::bind_state(state, owner)?;
        }
        Ok(self.reserve(owner.lane(), state))
    }

    fn reserve<'a>(
        self: pin::Pin<&'a Self>,
        lane: owner::Lane,
        state: &Owner,
    ) -> Prepared<'a, 'd, Source, ()> {
        state.bound.set(state.bound.get() + 1);
        Prepared(Entry {
            queue: self,
            index: (),
            lane,
        })
    }

    pub(in crate::task::storage) fn try_retire(
        self: pin::Pin<&Self>,
        owner: owner::Owner<Source>,
    ) -> Result<(), RetireError<Source>> {
        let state = self.current(owner).map_err(RetireError::from)?;
        let Some(tasks) = NonZero::new(state.bound.get()) else {
            state.generation.set(None);
            return Ok(());
        };
        Err(RetireError::Busy {
            owner,
            tasks: queue::Bound::new(tasks),
        })
    }

    /// Called only with a live topology front or an exclusively borrowed occupied
    /// pool entry. Both retain attachment to this queue and exclude a parked claim.
    fn resident(self: pin::Pin<&Self>, index: Global) -> Resident<'_, 'd, Source> {
        let node = self.store.node(index);
        let entry = Entry {
            queue: self,
            index,
            lane: node.lane.get(),
        };
        if key(index).is_linked(&node.hook) {
            Resident::Linked(entry)
        } else {
            Resident::Idle(Dequeued(entry))
        }
    }

    /// The exact registered state of `owner`'s lane, or the classified reason the
    /// infallible owner paths cannot be taken: a foreign lane or a stale generation.
    fn current(&self, owner: owner::Owner<Source>) -> Result<&Owner, OwnerError<Source>> {
        let state = self
            .store
            .get(owner.lane())
            .ok_or(OwnerError::Foreign { owner })?;
        if state.generation.get() != Some(owner.generation()) {
            return Err(OwnerError::Stale {
                owner,
                current: state.generation.get(),
            });
        }
        Ok(state)
    }

    pub(in crate::task::storage) fn readiness(
        self: pin::Pin<&Self>,
        scope: owner::Scope<Source>,
    ) -> queue::Wake {
        match self.front_entry(scope) {
            Some(Front::Ready(_)) => queue::Wake::Ready,
            Some(Front::Pending) => queue::Wake::Pending,
            None => queue::Wake::Idle,
        }
    }

    pub(in crate::task::storage) fn front(
        self: pin::Pin<&Self>,
        scope: owner::Scope<Source>,
    ) -> Option<(Global, queue::Wake)> {
        match self.front_entry(scope)? {
            Front::Ready(entry) => Some((entry.index, queue::Wake::Ready)),
            Front::Pending => self.slot.peek().map(|index| (index, queue::Wake::Pending)),
        }
    }

    pub(in crate::task::storage::fixed::schedule) fn next<'a, 'turn>(
        self: pin::Pin<&'a Self>,
        scope: owner::Scope<Source>,
        work: app::Work<'turn, 'd>,
    ) -> app::Next<'turn, 'd, (Option<Dequeued<'a, 'd, Source>>, Option<Global>), Global>
    where
        Source: Copy,
    {
        let Some(front) = self.front_entry(scope) else {
            return app::Next::Empty;
        };
        let Some(permit) = work.permit() else {
            return match front {
                Front::Ready(entry) => app::Next::Exhausted(entry.index),
                Front::Pending => self
                    .slot
                    .peek()
                    .map_or(app::Next::Empty, app::Next::Exhausted),
            };
        };
        let moved = self.sync().map(|entry| entry.index);
        let dequeued = match front {
            Front::Ready(entry) => Some(entry.dequeue()),
            Front::Pending => self.dequeue(scope),
        };
        let moved =
            moved.filter(|index| dequeued.as_ref().is_none_or(|next| next.index() != *index));
        app::Next::Item((dequeued, moved), permit)
    }

    pub(in crate::task::storage) fn dequeue(
        self: pin::Pin<&Self>,
        scope: owner::Scope<Source>,
    ) -> Option<Dequeued<'_, 'd, Source>> {
        match self.front_entry(scope)? {
            Front::Ready(entry) => Some(entry.dequeue()),
            Front::Pending => None,
        }
    }

    /// The fair ring's front lane always has a first resident: activation and deactivation
    /// follow the chain becoming nonempty and empty, so the ring cannot name an empty lane.
    fn front_entry(
        self: pin::Pin<&Self>,
        scope: owner::Scope<Source>,
    ) -> Option<Front<'_, 'd, Source>> {
        let selected = match scope {
            owner::Scope::Exact(owner) => {
                let lane = owner.lane();
                let state = self.store.get(lane)?;
                if state.generation.get() != Some(owner.generation()) {
                    return None;
                }
                state.chain.first().map(|index| (lane, index))
            }
            owner::Scope::Any => self.store.first.first().map(|first| {
                let lane = owner::Lane::from(first.position());
                let hooks = Hooks(&self.store);
                let owners = OwnerHooks(&self.store);
                // SAFETY: ring membership and FIFO nonemptiness are one Join
                // invariant, maintained by every insertion and removal.
                let head = Join::front(unsafe {
                    depot::raw::Proof::new(self.store.owner(lane).join(&hooks, &owners, lane))
                });
                let (_, index) = head.into_inner();
                (lane, index)
            }),
        };
        if let Some((lane, index)) = selected {
            return Some(Front::Ready(Entry {
                queue: self,
                lane,
                index: global(index),
            }));
        }
        (!self.slot.is_empty()).then_some(Front::Pending)
    }
}

fn key(index: Global) -> links::Index {
    index.position().into()
}

fn global(index: links::Index) -> Global {
    use crate::core::driver::schedule::ready::task::topology;

    // SAFETY: both representations retain the same non-sentinel task index.
    unsafe { <Global as topology::raw::Index>::from_raw(index.raw()) }
}
