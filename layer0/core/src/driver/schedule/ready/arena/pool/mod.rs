use std::{cell, num};

use crate::driver::{
    route,
    schedule::ready::{self, task},
};

/// Counts terminate free chains; every tail retains its own nonzero index.
/// Splitting and restoring transfer the exact count with the corresponding links.
#[derive(Clone, Copy)]
#[repr(transparent)]
struct State(Option<Available>);

#[derive(Clone, Copy)]
#[repr(C)]
struct Available {
    head: ready::FreeIndex,
    count: num::NonZero<u32>,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Detached {
    head: ready::FreeIndex,
    tail: ready::FreeIndex,
    count: num::NonZero<u32>,
}

#[repr(C)]
pub(in crate::driver::schedule::ready) struct Chain {
    state: cell::Cell<Option<Detached>>,
}

impl Chain {
    fn new(head: ready::FreeIndex, tail: ready::FreeIndex, count: num::NonZero<u32>) -> Self {
        Self {
            state: cell::Cell::new(Some(Detached { head, tail, count })),
        }
    }

    pub(in crate::driver::schedule::ready) fn try_claim<'a, 'd>(
        &'a self,
        slots: &'d ready::Slots,
    ) -> Option<(ready::ReservedKey<'d>, &'a Chain)> {
        let state = self.state.get()?;
        let rest = match num::NonZero::<u32>::new(state.count.get() - 1) {
            None => None,
            Some(count) => {
                let head = slots.next(state.head);
                Some(Detached {
                    head,
                    tail: state.tail,
                    count,
                })
            }
        };
        let key = slots.reserve(state.head);
        self.state.set(rest);
        Some((key, self))
    }

    pub(in crate::driver::schedule::ready) fn restore<'d>(
        &self,
        slots: &'d ready::Slots,
        key: ready::ReservedKey<'d>,
    ) {
        let index = ready::FreeIndex::from_key(key.key());
        let state = self.state.get();
        let count = match state {
            None => num::NonZero::<u32>::MIN,
            Some(state) => state.count.saturating_add(1),
        };
        let head = state.map_or(index, |state| state.head);
        slots.publish(key, head);
        self.state.set(Some(Detached {
            head: index,
            tail: state.map_or(index, |state| state.tail),
            count,
        }));
    }

    fn take(&self) -> Option<Detached> {
        self.state.replace(None)
    }
}

impl State {
    fn from_layout(layout: ready::Layout) -> Self {
        match layout.available() {
            Some((head, count)) => Self::occupied(head, count),
            None => Self::empty(),
        }
    }

    const fn empty() -> Self {
        Self(None)
    }

    const fn occupied(head: ready::FreeIndex, count: num::NonZero<u32>) -> Self {
        Self(Some(Available { head, count }))
    }

    const fn available(self) -> usize {
        match self.0 {
            Some(state) => state.count.get() as usize,
            None => 0,
        }
    }

    const fn link(self, tail: ready::FreeIndex) -> ready::FreeIndex {
        match self.0 {
            Some(state) => state.head,
            None => tail,
        }
    }

    fn prepend(self, head: ready::FreeIndex, count: num::NonZero<u32>) -> Self {
        let count = match self.0 {
            None => count,
            Some(state) => state.count.saturating_add(count.get()),
        };
        Self(Some(Available { head, count }))
    }
}

#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct Pool {
    state: cell::Cell<State>,
}

impl Pool {
    pub(in crate::driver::schedule::ready) fn new(layout: ready::Layout) -> Self {
        Self {
            state: cell::Cell::new(State::from_layout(layout)),
        }
    }

    fn claim<'d>(&self, slots: &'d ready::Slots) -> Option<ready::ReservedKey<'d>> {
        let state = self.state.get().0?;
        let rest = match num::NonZero::<u32>::new(state.count.get() - 1) {
            None => State::empty(),
            Some(count) => {
                let head = slots.next(state.head);
                State::occupied(head, count)
            }
        };
        let key = slots.reserve(state.head);
        self.state.set(rest);
        Some(key)
    }

    pub(in crate::driver::schedule::ready) fn try_dispatch<'d>(
        &'d self,
        slots: &'d ready::Slots,
        target: route::Token,
    ) -> Result<ready::DispatchKey<'d>, ready::Capacity> {
        let held = self.claim(slots).ok_or(ready::Capacity::new(0))?;
        Ok(slots.dispatch(held, target))
    }

    pub(in crate::driver::schedule::ready) fn try_topology<'d>(
        &'d self,
        slots: &'d ready::Slots,
    ) -> Result<ready::TopologyKey<'d>, ready::Capacity> {
        let held = self.claim(slots).ok_or(ready::Capacity::new(0))?;
        Ok(slots.topology(held))
    }

    pub(in crate::driver::schedule::ready) fn try_task<'d>(
        &'d self,
        slots: &'d ready::Slots,
    ) -> Result<ready::ReservedKey<'d>, task::Error> {
        self.claim(slots).ok_or(task::Error::Capacity {
            requested: num::NonZero::<usize>::MIN,
            available: 0,
        })
    }

    pub(in crate::driver::schedule::ready) fn try_take(
        &self,
        slots: &ready::Slots,
        count: num::NonZero<usize>,
    ) -> Result<Chain, task::Error> {
        let state = self.state.get();
        let Some(state) = state
            .0
            .filter(|state| count.get() <= state.count.get() as usize)
        else {
            return Err(task::Error::Capacity {
                requested: count,
                available: state.available(),
            });
        };
        // SAFETY: admission bounds the nonzero request by the free count.
        let wanted = unsafe { num::NonZero::<u32>::new_unchecked(count.get() as u32) };
        let mut tail = state.head;
        for _ in 1..wanted.get() {
            tail = slots.next(tail);
        }
        let rest = state.count.get() - wanted.get();
        let next = match num::NonZero::<u32>::new(rest) {
            None => State::empty(),
            Some(left) => {
                let head = slots.next(tail);
                State::occupied(head, left)
            }
        };
        slots.link(tail, tail);
        self.state.set(next);
        Ok(Chain::new(state.head, tail, wanted))
    }

    pub(in crate::driver::schedule::ready) fn free_task<'d>(
        &self,
        slots: &'d ready::Slots,
        key: ready::TaskKey<'d>,
    ) {
        let ready::Detached { task, root } = key.detach(slots);
        self.recycle(slots, task);
        if let Some(root) = root {
            self.recycle(slots, root);
        }
    }

    pub(in crate::driver::schedule::ready) fn release_root<'d>(
        &self,
        slots: &'d ready::Slots,
        key: ready::TopologyKey<'d>,
    ) {
        if let Some(held) = ready::Topology::get(slots, &key).close(key) {
            self.recycle(slots, held);
        }
    }

    pub(in crate::driver::schedule::ready) fn recycle<'d>(
        &self,
        slots: &'d ready::Slots,
        held: ready::ReservedKey<'d>,
    ) {
        let key = match slots.renew(held) {
            ready::Renewal::Reusable(key) => key,
            ready::Renewal::Retired => return,
        };
        let index = ready::FreeIndex::from_key(key.key());
        let state = self.state.get();
        slots.publish(key, state.link(index));
        self.state
            .set(state.prepend(index, num::NonZero::<u32>::MIN));
    }

    pub(in crate::driver::schedule::ready) fn restore(&self, slots: &ready::Slots, chain: &Chain) {
        let Some(chain) = chain.take() else {
            return;
        };
        let state = self.state.get();
        slots.link(chain.tail, state.link(chain.tail));
        self.state.set(state.prepend(chain.head, chain.count));
    }
}

const _: () = {
    assert!(std::mem::size_of::<Pool>() == 2 * std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<State>() == 2 * std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<Detached>() == 3 * std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<Option<Detached>>() == std::mem::size_of::<Detached>());
    assert!(std::mem::size_of::<Chain>() == 3 * std::mem::size_of::<u32>());
};
