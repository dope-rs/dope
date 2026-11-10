use std::{mem, ptr};

use o3::alloc;

use crate::driver::schedule::clock::timer::heap::{
    FIRED, FREE, IDLE, State, capacity::Capacity, order, tokens::Slot,
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

    pub(super) fn lease(&mut self, item: R, free: Free) -> Slot {
        let () = free.0;
        let raw = self.storage.state.free;
        // SAFETY: Vacancy retains the exclusive heap loan from free(), so this
        // unchanged free-list head is in capacity and contains its next link.
        let entry = unsafe { self.storage.extra_mut().1.get_unchecked_mut(raw as usize) };
        // SAFETY: the free-list head has not yet been changed to a live value.
        let next = unsafe { entry.next };
        entry.value = mem::ManuallyDrop::new(item);
        self.storage.state.free = next;
        let slot = Slot::from_stored(raw);
        self.write_state(slot, IDLE);
        slot
    }

    pub(super) fn release(&mut self, slot: Slot) -> (R, State<P>) {
        let priority = match self.phase(slot) {
            State::Armed(position) => State::Armed(self.remove_at(position)),
            State::Idle => State::Idle,
            State::Fired => State::Fired,
        };
        let free = self.storage.state.free;
        // SAFETY: the consumed branded lease names this live in-capacity slot.
        let entry = unsafe { self.storage.extra_mut().1.get_unchecked_mut(slot.offset()) };
        // SAFETY: the live slot contains one initialized value, moved once here.
        let item = unsafe { mem::ManuallyDrop::take(&mut entry.value) };
        entry.next = free;
        self.storage.state.free = slot.raw();
        self.write_state(slot, FREE);
        (item, priority)
    }

    pub(super) fn state(&self, slot: Slot) -> State<&P> {
        match self.phase(slot) {
            State::Armed(position) => State::Armed(self.priority_at(position)),
            State::Idle => State::Idle,
            State::Fired => State::Fired,
        }
    }

    pub(super) fn priority(&self, slot: Slot) -> Option<&P> {
        match self.state(slot) {
            State::Armed(priority) => Some(priority),
            State::Idle | State::Fired => None,
        }
    }

    pub(super) fn resident(&self, slot: Slot) -> &R {
        // SAFETY: the branded lease proves this in-capacity slot contains a value;
        // the shared heap loan prevents release for the returned borrow.
        unsafe { &self.storage.extra().1.get_unchecked(slot.offset()).value }
    }

    pub(super) fn set(&mut self, slot: Slot, prio: P) {
        match self.phase(slot) {
            State::Armed(position) => self.set_at(position, slot, prio),
            State::Idle | State::Fired => order::insert(self, slot, prio),
        }
    }

    pub(super) fn clear(&mut self, slot: Slot) {
        let priority = match self.phase(slot) {
            State::Armed(position) => Some(self.remove_at(position)),
            State::Idle | State::Fired => None,
        };
        self.write_state(slot, IDLE);
        drop(priority);
    }

    pub(super) fn fire(&mut self, root: order::Root) -> Slot {
        let slot = self.min_slot(root);
        let priority = self.remove_at(order::Position::ROOT);
        self.write_state(slot, FIRED);
        drop(priority);
        slot
    }

    pub(super) fn write_state(&mut self, slot: Slot, state: u32) {
        // SAFETY: slots originate from this heap's retained lease, free-list head
        // or minimum, all bounded by its unchanged allocation extent.
        unsafe { *self.storage.extra_mut().0.get_unchecked_mut(slot.offset()) = state };
    }

    fn phase(&self, slot: Slot) -> State<order::Position> {
        // SAFETY: the branded live slot is within this heap's initialized plane.
        match unsafe { *self.storage.extra().0.get_unchecked(slot.offset()) } {
            IDLE => State::Idle,
            FIRED => State::Fired,
            state => State::Armed(order::Position::from_state(state)),
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
