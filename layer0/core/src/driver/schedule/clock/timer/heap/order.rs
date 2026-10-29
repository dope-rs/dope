use std::{cmp, mem};

use o3::depot::heap::hole;

use crate::driver::schedule::clock::timer::heap::{core::Core, tokens::Slot};

#[repr(transparent)]
pub(super) struct Order<'a, P: Ord, R>(&'a Core<P, R>);

#[repr(transparent)]
struct Storage<'a, P: Ord, R>(&'a mut Core<P, R>);

pub(super) struct Entry<'a, P: Ord, R> {
    core: &'a mut Core<P, R>,
    position: Position,
}

#[derive(Clone, Copy)]
pub(super) struct Root(());

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(super) struct Position(usize);

impl<'a, P: Ord, R> Order<'a, P, R> {
    pub(super) const fn new(core: &'a Core<P, R>) -> Self {
        Self(core)
    }

    pub(super) const fn len(&self) -> u32 {
        self.0.storage.state.len
    }

    pub(super) fn priority(&self, pos: Position) -> &'a P {
        // SAFETY: this position comes from an armed resident's reverse map, a
        // retained nonempty root, or an initialized o3 heap entry. Each names
        // this core's initialized prefix; the shared core loan retains P.
        unsafe {
            self.0
                .storage
                .tail()
                .get_unchecked(pos.get())
                .assume_init_ref()
        }
    }

    pub(super) fn root(&self) -> Option<Root> {
        (self.len() != 0).then_some(Root(()))
    }

    pub(super) fn min_slot(&self, root: Root) -> Slot {
        let Root(()) = root;
        self.slot(Position::ROOT)
    }

    pub(super) fn min_priority(&self, root: Root) -> &'a P {
        let Root(()) = root;
        self.priority(Position::ROOT)
    }

    pub(super) fn peek(&self) -> Option<&'a P> {
        let root = self.root()?;
        Some(self.min_priority(root))
    }

    fn slot(&self, pos: Position) -> Slot {
        // SAFETY: the same initialized heap position proves both slot and
        // priority planes, whose equal extent is fixed by Capacity.
        Slot::from_stored(unsafe {
            self.0
                .storage
                .as_ref()
                .get_unchecked(pos.get())
                .assume_init()
        })
    }

    fn cmp(&self, priority: &P, slot: Slot, position: Position) -> cmp::Ordering {
        priority
            .cmp(self.priority(position))
            .then_with(|| slot.raw().cmp(&self.slot(position).raw()))
    }
}

impl Position {
    pub(super) const ROOT: Self = Self(0);

    pub(super) const fn from_state(state: u32) -> Self {
        Self(state as usize)
    }

    const fn get(self) -> usize {
        self.0
    }
}

impl<'a, P: Ord, R> Entry<'a, P, R> {
    pub(super) const fn new(core: &'a mut Core<P, R>, position: Position) -> Self {
        Self { core, position }
    }

    pub(super) fn set(self, slot: Slot, prio: P) {
        // SAFETY: the armed resident proves this initialized position and unchanged slot identity.
        unsafe {
            hole::raw::Source::update(hole::raw::Source::entry(
                Storage(self.core),
                self.position.get(),
                (slot, prio),
            ))
        };
    }

    pub(super) fn remove(self) -> P {
        // SAFETY: the armed resident or minimum proves this initialized position.
        let (_, (_, priority)) =
            unsafe { hole::raw::Source::entry(Storage(self.core), self.position.get(), ()) }
                .remove();
        priority
    }
}

pub(super) fn insert<P: Ord, R>(core: &mut Core<P, R>, slot: Slot, prio: P) {
    // SAFETY: the idle/fired leased slot is absent and reserves one spare heap position.
    unsafe { hole::raw::Source::vacancy(Storage(core), (slot, prio)) }.store();
}

// SAFETY: the exclusive core loan keeps all four planes and the initialized heap
// prefix together; each engine token admits only accesses within this same owner.
unsafe impl<P: Ord, R> hole::raw::Source for Storage<'_, P, R> {
    type Value = (Slot, P);

    fn len(&self) -> u32 {
        self.0.storage.state.len
    }

    unsafe fn len_mut(&mut self) -> &mut u32 {
        &mut self.0.storage.state.len
    }

    fn compare(entry: hole::At<&Self, &Self::Value>) -> cmp::Ordering {
        let (core, position, (slot, priority)) = entry.into_inner();
        Order::new(core.0).cmp(priority, *slot, Position(position))
    }

    fn direction(entry: hole::At<&Self, &Self::Value>) -> cmp::Ordering {
        let (core, position, (_, priority)) = entry.into_inner();
        priority.cmp(Order::new(core.0).priority(Position(position)))
    }

    fn compare_at(left: hole::At<&Self>, right: hole::At<&Self>) -> cmp::Ordering {
        let (core, left, ()) = left.into_inner();
        let (other, right, ()) = right.into_inner();
        let order = Order::new(core.0);
        let left = Position(left);
        Order::new(other.0).cmp(order.priority(left), order.slot(left), Position(right))
    }

    fn read<const REMOVE: bool>(entry: hole::At<&mut Self>) -> Self::Value {
        let (core, position, ()) = entry.into_inner();
        read(core.0, position)
    }

    fn shift(step: hole::Step<'_, Self>) {
        let (core, source, target) = step.into_inner();
        let value = read(core.0, source);
        write::<true, _, _>(core.0, target, value);
    }

    fn write<const PLACE: bool>(finish: hole::Finish<Self>) -> Self {
        let (core, position, value) = finish.into_inner();
        write::<PLACE, _, _>(core.0, position, value);
        core
    }
}

fn read<P: Ord, R>(core: &mut Core<P, R>, position: usize) -> (Slot, P) {
    let slot = Order::new(core).slot(Position(position));
    // SAFETY: callers consume an initialized At or Step source from the o3
    // engine. Its exclusive loan owns the hole until this moved value is stored.
    let priority = unsafe {
        core.storage
            .tail()
            .get_unchecked(position)
            .assume_init_read()
    };
    (slot, priority)
}

fn write<const PLACE: bool, P: Ord, R>(
    core: &mut Core<P, R>,
    position: usize,
    (slot, priority): (Slot, P),
) {
    if PLACE {
        // SAFETY: Step or Finish retains an in-capacity hole in this exact core;
        // the moved slot is initialized here before its reverse map is updated.
        unsafe {
            *core.storage.as_mut().get_unchecked_mut(position) = mem::MaybeUninit::new(slot.raw())
        };
    }
    // SAFETY: the engine owns this vacant priority position and its replacement.
    // Capacity gives the priority plane the same extent as the slot plane.
    unsafe {
        *core.storage.parts_mut().2.get_unchecked_mut(position) = mem::MaybeUninit::new(priority)
    };
    if PLACE {
        core.write_state(slot, position as u32);
    }
}

const _: () = {
    assert!(mem::size_of::<Root>() == 0);
    assert!(mem::size_of::<Position>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Order<'_, u64, u8>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Entry<'_, u64, u8>>() == 2 * mem::size_of::<usize>());
};
