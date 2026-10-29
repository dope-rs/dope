use std::{mem, ptr};

use o3::alloc;

use crate::driver::schedule::clock::timer::heap::{
    FIRED, FREE, IDLE, State,
    capacity::Capacity,
    order,
    tokens::{Live, Slot},
};

type Storage<P, R> =
    alloc::raw::Block<Capacity<P, R>, Counts, (ptr::NonNull<u32>, ptr::NonNull<Resident<R>>)>;

pub(super) union Resident<R> {
    pub(super) next: u32,
    pub(super) value: mem::ManuallyDrop<R>,
}

pub(in crate::driver::schedule::clock::timer::heap) struct Core<P: Ord, R> {
    pub(super) storage: Storage<P, R>,
}

pub(super) struct Counts {
    pub(super) len: u32,
    free: u32,
}

#[derive(Clone, Copy)]
pub(super) struct Free(());

enum Phase {
    Armed(order::Position),
    Idle,
    Fired,
}

impl<P: Ord, R> Core<P, R> {
    pub(in crate::driver::schedule::clock::timer::heap) fn try_new(
        cap: usize,
    ) -> Result<Self, alloc::Error> {
        let extent = Capacity::new(cap)?;
        let count = extent.count();
        let counts = Counts {
            len: 0,
            free: if count.get() == 0 { FREE } else { 0 },
        };
        Ok(Self {
            storage: alloc::raw::Block::try_parts(extent, counts, |index| {
                let next = if index + 1 == count.get() {
                    FREE
                } else {
                    (index + 1) as u32
                };
                (FREE, Resident { next })
            })?,
        })
    }

    pub(super) fn free(&self) -> Option<Free> {
        (self.storage.state.free != FREE).then_some(Free(()))
    }

    pub(super) fn lease(&mut self, item: R, free: Free) -> Live {
        let () = free.0;
        let slot = Slot::from_stored(self.storage.state.free);
        // SAFETY: Vacancy retains the exclusive heap loan from free(), so this
        // unchanged free-list head is in capacity and contains its next link.
        let entry = unsafe { self.storage.extra_mut().1.get_unchecked_mut(slot.offset()) };
        // SAFETY: the free-list head has not yet been changed to a live value.
        let next = unsafe { entry.next };
        entry.value = mem::ManuallyDrop::new(item);
        self.storage.state.free = next;
        self.write_state(slot, IDLE);
        Live::from_slot(slot)
    }

    pub(super) fn release(&mut self, live: Live) -> R {
        let priority = match self.phase(live) {
            Phase::Armed(position) => Some(order::Entry::new(self, position).remove()),
            Phase::Idle | Phase::Fired => None,
        };
        let slot = live.slot();
        let free = self.storage.state.free;
        // SAFETY: the consumed branded lease names this live in-capacity slot.
        let entry = unsafe { self.storage.extra_mut().1.get_unchecked_mut(slot.offset()) };
        // SAFETY: the live slot contains one initialized value, moved once here.
        let item = unsafe { mem::ManuallyDrop::take(&mut entry.value) };
        entry.next = free;
        self.storage.state.free = slot.raw();
        self.write_state(slot, FREE);
        drop(priority);
        item
    }

    pub(super) fn state(&self, live: Live) -> State {
        match self.phase(live) {
            Phase::Armed(_) => State::Armed,
            Phase::Idle => State::Idle,
            Phase::Fired => State::Fired,
        }
    }

    pub(super) fn priority(&self, live: Live) -> Option<&P> {
        match self.phase(live) {
            Phase::Armed(position) => Some(order::Order::new(self).priority(position)),
            Phase::Idle | Phase::Fired => None,
        }
    }

    pub(super) fn resident(&self, live: Live) -> &R {
        // SAFETY: the branded lease proves this in-capacity slot contains a value;
        // the shared heap loan prevents release for the returned borrow.
        unsafe {
            &self
                .storage
                .extra()
                .1
                .get_unchecked(live.slot().offset())
                .value
        }
    }

    pub(super) fn set(&mut self, live: Live, prio: P) {
        match self.phase(live) {
            Phase::Armed(position) => order::Entry::new(self, position).set(live.slot(), prio),
            Phase::Idle | Phase::Fired => order::insert(self, live.slot(), prio),
        }
    }

    pub(super) fn clear(&mut self, live: Live) {
        let priority = match self.phase(live) {
            Phase::Armed(position) => Some(order::Entry::new(self, position).remove()),
            Phase::Idle | Phase::Fired => None,
        };
        self.write_state(live.slot(), IDLE);
        drop(priority);
    }

    pub(super) fn fire(&mut self, root: order::Root) -> Live {
        let slot = order::Order::new(self).min_slot(root);
        let priority = order::Entry::new(self, order::Position::ROOT).remove();
        self.write_state(slot, FIRED);
        drop(priority);
        Live::from_slot(slot)
    }

    pub(super) fn write_state(&mut self, slot: Slot, state: u32) {
        // SAFETY: slots originate from this heap's retained lease, free-list head
        // or minimum, all bounded by its unchanged allocation extent.
        unsafe { *self.storage.extra_mut().0.get_unchecked_mut(slot.offset()) = state };
    }

    fn phase(&self, live: Live) -> Phase {
        // SAFETY: the branded live slot is within this heap's initialized plane.
        match unsafe { *self.storage.extra().0.get_unchecked(live.slot().offset()) } {
            IDLE => Phase::Idle,
            FIRED => Phase::Fired,
            state => Phase::Armed(order::Position::from_state(state)),
        }
    }
}

impl<P: Ord, R> Drop for Core<P, R> {
    fn drop(&mut self) {
        if mem::needs_drop::<P>() {
            let (counts, _, values) = self.storage.parts_mut();
            for position in (0..counts.len as usize).rev() {
                // SAFETY: terminal ownership retains the initialized priority
                // prefix; reverse traversal preserves the prior removal order.
                unsafe { values.get_unchecked_mut(position).assume_init_drop() };
            }
        }
        if mem::needs_drop::<R>() {
            let (states, values) = self.storage.extra_mut();
            for (state, resident) in states.iter().zip(values) {
                if *state != FREE {
                    // SAFETY: the equal planes pair each non-free slot with its
                    // initialized resident, owned only by this terminal traversal.
                    unsafe { mem::ManuallyDrop::drop(&mut resident.value) };
                }
            }
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Free>() == 0);
    assert!(mem::size_of::<Counts>() == 2 * mem::size_of::<u32>());
    assert!(mem::size_of::<Core<u64, u8>>() == 6 * mem::size_of::<usize>());
};
