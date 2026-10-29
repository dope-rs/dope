use std::{cell, marker, num};

use o3::{
    mem,
    mem::{Return, lease::Open},
};
pub(in crate::driver::schedule::ready) use permit::Permit;

use crate::driver::{
    route,
    schedule::ready::{self, task},
};

mod permit;

/// Affine proof that one checked-out slot exhausted its generation.
pub(in crate::driver::schedule::ready) struct Retirement<'d> {
    pub(in crate::driver::schedule::ready) brand:
        marker::PhantomData<fn(&'d ready::Slots) -> &'d ready::Slots>,
}

pub(in crate::driver::schedule::ready) enum Reclaimed<'d> {
    Reusable(Permit<'d>),
    Retired(Retirement<'d>),
}

impl Retirement<'_> {
    pub(in crate::driver::schedule::ready) const fn new() -> Self {
        Self {
            brand: marker::PhantomData,
        }
    }
}

#[must_use = "a task vacancy must be committed or released"]
#[repr(C)]
pub(in crate::driver::schedule::ready) struct Vacancy<'d> {
    pool: &'d Pool,
    slots: &'d ready::Slots,
    index: ready::FreeIndex,
    rest: State,
}

/// Counts terminate free chains; every tail retains its own nonzero index.
/// Splitting and restoring transfer the exact count with the corresponding links.
#[derive(Clone, Copy)]
#[repr(transparent)]
struct State(Option<Available>);

#[derive(Clone, Copy)]
#[repr(C)]
struct Available {
    head: ready::FreeIndex,
    count: num::NonZero<u16>,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Detached {
    head: ready::FreeIndex,
    tail: ready::FreeIndex,
    count: num::NonZero<u16>,
}

struct Returned {
    head: ready::FreeIndex,
    count: num::NonZero<u16>,
}

impl Returned {
    const fn one(head: ready::FreeIndex) -> Self {
        Self {
            head,
            count: num::NonZero::<u16>::MIN,
        }
    }
}

impl From<Detached> for Returned {
    fn from(chain: Detached) -> Self {
        Self {
            head: chain.head,
            count: chain.count,
        }
    }
}

#[repr(C)]
pub(in crate::driver::schedule::ready) struct Chain {
    state: cell::Cell<Option<Detached>>,
}

#[must_use = "a task lease must be committed or released"]
#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct Lease<'d> {
    held: mem::Lease<Restore<'d>, Open>,
}

struct Restore<'d> {
    pool: &'d Pool,
    slots: &'d ready::Slots,
}

impl<'d> Vacancy<'d> {
    pub(in crate::driver::schedule::ready) fn commit(self) -> Permit<'d> {
        let key = self.slots.mutation().reserve(self.index);
        self.pool.state.set(self.rest);
        Permit::new(key)
    }
}

impl Chain {
    fn new(head: ready::FreeIndex, tail: ready::FreeIndex, count: num::NonZero<u16>) -> Self {
        Self {
            state: cell::Cell::new(Some(Detached { head, tail, count })),
        }
    }

    pub(in crate::driver::schedule::ready) fn try_claim<'a, 'd>(
        &'a self,
        slots: &'d ready::Slots,
    ) -> Option<(Permit<'d>, &'a Chain)> {
        let state = self.state.get()?;
        let rest = match num::NonZero::<u16>::new(state.count.get() - 1) {
            None => None,
            Some(count) => {
                let head = slots.mutation().next(state.head);
                Some(Detached {
                    head,
                    tail: state.tail,
                    count,
                })
            }
        };
        let key = slots.mutation().reserve(state.head);
        self.state.set(rest);
        Some((Permit::new(key), self))
    }

    pub(in crate::driver::schedule::ready) fn restore<'d>(
        &self,
        slots: &'d ready::Slots,
        key: ready::ReservedKey<'d>,
    ) {
        let index = ready::FreeIndex::from_key(key.key());
        let state = self.state.get();
        let count = match state {
            None => num::NonZero::<u16>::MIN,
            Some(state) => state.count.saturating_add(1),
        };
        let head = state.map_or(index, |state| state.head);
        slots.mutation().publish(key, head);
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

    const fn occupied(head: ready::FreeIndex, count: num::NonZero<u16>) -> Self {
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

    fn prepend(self, returned: Returned) -> Self {
        let count = match self.0 {
            None => returned.count,
            Some(state) => state.count.saturating_add(returned.count.get()),
        };
        Self(Some(Available {
            head: returned.head,
            count,
        }))
    }
}

impl Lease<'_> {
    pub(in crate::driver::schedule::ready) fn commit(self) -> Chain {
        self.held.into_parts().1
    }
}

impl Return for Restore<'_> {
    type Item = Chain;

    fn return_item(self, chain: Self::Item) {
        self.pool.restore(self.slots, &chain);
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

    fn vacancy<'d>(&'d self, slots: &'d ready::Slots) -> Option<Vacancy<'d>> {
        let state = self.state.get().0?;
        let rest = match num::NonZero::<u16>::new(state.count.get() - 1) {
            None => State::empty(),
            Some(count) => {
                let head = slots.mutation().next(state.head);
                State::occupied(head, count)
            }
        };
        Some(Vacancy {
            pool: self,
            slots,
            index: state.head,
            rest,
        })
    }

    pub(in crate::driver::schedule::ready) fn try_dispatch<'d>(
        &'d self,
        slots: &'d ready::Slots,
        target: route::Token,
    ) -> Result<ready::DispatchKey<'d>, ready::Capacity> {
        let vacancy = self.vacancy(slots).ok_or(ready::Capacity::new(0))?;
        let held = vacancy.commit();
        Ok(slots.mutation().dispatch(held.into_key(), target))
    }

    pub(in crate::driver::schedule::ready) fn try_topology<'d>(
        &'d self,
        slots: &'d ready::Slots,
    ) -> Result<ready::TopologyKey<'d>, ready::Capacity> {
        let vacancy = self.vacancy(slots).ok_or(ready::Capacity::new(0))?;
        let held = vacancy.commit();
        Ok(slots.mutation().topology(held.into_key()))
    }

    pub(in crate::driver::schedule::ready) fn prepare_task<'d>(
        &'d self,
        slots: &'d ready::Slots,
    ) -> Result<Vacancy<'d>, task::Error> {
        self.vacancy(slots).ok_or(task::Error::Capacity {
            requested: num::NonZero::<usize>::MIN,
            available: 0,
        })
    }

    pub(in crate::driver::schedule::ready) fn try_lease<'d>(
        &'d self,
        slots: &'d ready::Slots,
        count: num::NonZero<usize>,
    ) -> Result<Lease<'d>, task::Error> {
        let state = self.state.get();
        let Some(state) = state
            .0
            .filter(|state| count.get() <= usize::from(state.count.get()))
        else {
            return Err(task::Error::Capacity {
                requested: count,
                available: state.available(),
            });
        };
        // SAFETY: admission bounds the nonzero request by the u16 free count.
        let wanted = unsafe { num::NonZero::<u16>::new_unchecked(count.get() as u16) };
        let mut tail = state.head;
        for _ in 1..wanted.get() {
            tail = slots.mutation().next(tail);
        }
        let rest = state.count.get() - wanted.get();
        let next = match num::NonZero::<u16>::new(rest) {
            None => State::empty(),
            Some(left) => {
                let head = slots.mutation().next(tail);
                State::occupied(head, left)
            }
        };
        slots.mutation().link(tail, tail);
        self.state.set(next);
        Ok(Lease {
            held: mem::Lease::open(
                Restore { pool: self, slots },
                Chain::new(state.head, tail, wanted),
            ),
        })
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
        let key = match held.renew(slots) {
            ready::Renewal::Reusable(key) => key,
            ready::Renewal::Retired(Retirement {
                brand: marker::PhantomData,
            }) => return,
        };
        let index = ready::FreeIndex::from_key(key.key());
        let state = self.state.get();
        slots.mutation().publish(key, state.link(index));
        self.state.set(state.prepend(Returned::one(index)));
    }

    pub(in crate::driver::schedule::ready) fn restore(&self, slots: &ready::Slots, chain: &Chain) {
        let Some(chain) = chain.take() else {
            return;
        };
        let state = self.state.get();
        slots.mutation().link(chain.tail, state.link(chain.tail));
        self.state.set(state.prepend(chain.into()));
    }
}

const _: () = {
    assert!(std::mem::size_of::<Pool>() == 2 * std::mem::size_of::<u16>());
    assert!(std::mem::size_of::<State>() == 2 * std::mem::size_of::<u16>());
    assert!(std::mem::size_of::<Detached>() == 3 * std::mem::size_of::<u16>());
    assert!(std::mem::size_of::<Option<Detached>>() == std::mem::size_of::<Detached>());
    assert!(std::mem::size_of::<Chain>() == 3 * std::mem::size_of::<u16>());
    assert!(std::mem::size_of::<Lease<'static>>() == 3 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Option<Lease<'static>>>() == std::mem::size_of::<Lease<'static>>());
    assert!(std::mem::size_of::<Vacancy<'static>>() == 3 * std::mem::size_of::<usize>());
};
