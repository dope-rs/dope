use std::{marker, pin};

use o3::{
    alloc,
    base::{cell::region, marker::ThreadBound, num::Epoch},
    depot::{arena::slab::external, capacity},
};

use crate::{
    context::wake,
    core::driver::schedule::ready::task::{self, owner},
    task::storage::{
        fixed::{
            local, schedule,
            schedule::{queue, ready::Global, result::Error},
        },
        scope::Scope,
    },
};

mod poll;
pub use poll::Mode;

pub type Pool<'d, F, D, T = (), S = queue::Flat<'d>> = Storage<'d, F, D, T, (), S>;
pub type Stream<'d, F, D, T = (), S = queue::Flat<'d>> = Storage<'d, F, D, T, poll::Stream, S>;

#[pin_project::pin_project]
pub(super) struct Entry<'d, F, T: Copy, M, E> {
    #[pin]
    pub(super) fiber: F,
    pub(super) binding: task::raw::Root<'d>,
    #[pin]
    pub(super) ready: E,
    pub(super) target: T,
    pub(super) mode: M,
}

pub(super) type Slots<'d, F, T, M, S> =
    external::Pinned<Entry<'d, F, T, M, <S as queue::Backend<'d>>::Entry>, Epoch>;

#[pin_project::pin_project(!Unpin)]
pub struct Storage<'d, F, D, T: Copy = (), M = (), S: queue::Backend<'d> = queue::Flat<'d>> {
    #[pin]
    pub(super) slots: Slots<'d, F, T, M, S>,
    #[pin]
    pub(super) queue: S,
    domain: marker::PhantomData<(fn(&'d (), D) -> (&'d (), D), ThreadBound)>,
}

impl<'d, F, D, T: Copy, M> Storage<'d, F, D, T, M> {
    pub fn try_new(count: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self {
            slots: external::Pinned::try_new(count)?,
            queue: queue::Flat::new(),
            domain: marker::PhantomData,
        })
    }
}

impl<'d, F, D, T: Copy, M, Source> Storage<'d, F, D, T, M, queue::Owned<'d, Source>> {
    pub fn try_owned(
        count: capacity::Count,
        owners: owner::Capacity,
    ) -> Result<Self, queue::PrepareError> {
        Ok(Self {
            slots: external::Pinned::try_new(count).map_err(queue::PrepareError::Routes)?,
            queue: queue::Owned::try_new(count, owners)?,
            domain: marker::PhantomData,
        })
    }

    pub fn owners(self: pin::Pin<&Self>) -> queue::Owners<'_, 'd, Source> {
        queue::Owners::new(self.project_ref().queue)
    }
}

#[must_use = "an admission reserves one task until insertion or drop"]
pub struct Admission<'a, 's, 'd, F, D, T: Copy, M, S: queue::Backend<'d> + 'a> {
    slot: external::Anchor<'a, Entry<'d, F, T, M, S::Entry>, Epoch>,
    binding: task::raw::Root<'d>,
    prepared: S::Prepared<'a>,
    proof: region::Identity<'s>,
    domain: marker::PhantomData<fn(D) -> D>,
}

impl<'s, 'd, F, D, T: Copy, M: Mode<'d, F>, S: queue::Backend<'d>>
    Admission<'_, 's, 'd, F, D, T, M, S>
{
    pub fn insert(self, (fiber, target): (F, T)) -> schedule::Id<'s, 'd, D> {
        let parent = wake::RootWaker::from(self.binding.parent());
        let entry = Entry {
            fiber,
            binding: self.binding,
            ready: S::entry(&self.prepared),
            target,
            mode: M::INITIAL,
        };
        let (key, occupied) = self.slot.occupy(entry);
        S::admit(self.prepared, occupied.into_ref().project_ref().ready);
        parent.wake();
        schedule::Id::new(key, self.proof)
    }
}

struct Member<'a, 'd, F, T: Copy, M, S: queue::Backend<'d>> {
    occupied: external::Anchored<'a, Entry<'d, F, T, M, S::Entry>, Epoch>,
    queue: pin::Pin<&'a S>,
    index: Global,
}

impl<'d, F, T: Copy, M, S: queue::Backend<'d>> local::Member for Member<'_, 'd, F, T, M, S> {
    type Fiber = F;

    fn value(&mut self) -> pin::Pin<&mut F> {
        self.occupied.value().project().fiber
    }

    fn discard(mut self) {
        self.queue
            .detach(self.index, self.occupied.value().project().ready.into_ref());
        self.occupied.discard();
    }
}

impl<'s, 'a, 'd, F, D, T: Copy, M: Mode<'d, F>, S: queue::Backend<'d>>
    Scope<'s, pin::Pin<&'a mut Storage<'d, F, D, T, M, S>>>
{
    pub fn try_admit(
        &mut self,
        input: S::Input,
        parent: wake::RootWaker<'d>,
    ) -> Result<Admission<'_, 's, 'd, F, D, T, M, S>, S::Error> {
        let storage = self.source.as_mut().project();
        let slot = storage.slots.try_grant().map_err(|_| {
            S::Error::from(task::Error::Capacity {
                requested: std::num::NonZero::<usize>::MIN,
                available: 0,
            })
        })?;
        let index = Global::from_slot(slot.index());
        let (binding, prepared) = storage.queue.into_ref().try_prepare(index, input, parent)?;
        Ok(Admission {
            slot,
            binding,
            prepared,
            proof: self.proof,
            domain: marker::PhantomData,
        })
    }

    pub fn entry(
        &mut self,
        id: schedule::Id<'s, 'd, D>,
    ) -> Result<
        local::Entry<impl local::Member<Fiber = F>, schedule::Id<'s, 'd, D>>,
        schedule::Id<'s, 'd, D>,
    > {
        let storage = self.source.as_mut().project();
        let Ok(occupied) = storage.slots.try_occupied(id.key) else {
            return Err(id);
        };
        let member = Member {
            occupied,
            queue: storage.queue.into_ref(),
            index: Global::from_slot(id.key.index()),
        };
        Ok(local::Entry { member, id })
    }

    pub fn remove(&mut self, id: schedule::Id<'s, 'd, D>) -> local::Removal {
        match self.entry(id) {
            Ok(entry) => {
                entry.remove();
                local::Removal::Removed
            }
            Err(_) => local::Removal::Absent,
        }
    }

    fn held_entry<const RESUME: bool>(
        &mut self,
        id: &schedule::Id<'s, 'd, D>,
    ) -> Result<pin::Pin<&mut Entry<'d, F, T, M, S::Entry>>, Error> {
        let mut entry = self
            .source
            .as_mut()
            .project()
            .slots
            .try_occupied(id.key)
            .map_err(|_| Error::Absent)?
            .into_value();
        let fields = entry.as_mut().project();
        let held = fields.mode.held(fields.fiber)?;
        if !*held {
            return Err(Error::Active);
        }
        if RESUME {
            *held = false;
        }
        Ok(entry)
    }

    pub fn get_mut(&mut self, id: &schedule::Id<'s, 'd, D>) -> Result<pin::Pin<&mut F>, Error> {
        Ok(self.held_entry::<false>(id)?.project().fiber)
    }

    pub fn resume(&mut self, id: &schedule::Id<'s, 'd, D>) -> Result<(), Error> {
        let fields = self.held_entry::<true>(id)?.project();
        wake::Waker::from_parent(fields.binding.poll().1).wake();
        Ok(())
    }
}
