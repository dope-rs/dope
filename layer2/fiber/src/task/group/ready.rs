//! Sealed inline readiness state for a task group.

use core::{cell, pin};

use o3::depot::array::{self, bits};

use crate::{
    core::driver::schedule::{
        app,
        ready::task::{self, topology},
    },
    task::storage::slots::Index,
};

#[derive(Clone, Copy)]
#[repr(C, align(8))]
struct State<const N: usize> {
    current: bits::Set<Index<N>, N>,
    next: bits::Set<Index<N>, N>,
    live: bits::Set<Index<N>, N>,
}

#[repr(transparent)]
struct Members<const N: usize>(cell::Cell<State<N>>);

pub(super) struct Ready<'d, const N: usize> {
    members: Members<N>,
    endpoint: topology::Endpoint<'d, Index<N>>,
}

#[repr(transparent)]
pub(super) struct Vacancy<const N: usize>(Index<N>);

impl<const N: usize> Vacancy<N> {
    pub(super) const fn index(&self) -> Index<N> {
        self.0
    }
}

pub(super) struct Drain<'queue, 'd, const N: usize> {
    members: &'queue Members<N>,
    endpoint: Option<topology::Drain<'queue, 'd, Index<N>>>,
}

pub(super) struct Member<'queue, const N: usize> {
    members: &'queue Members<N>,
    index: Index<N>,
}

impl<const N: usize> Members<N> {
    fn new(live: bits::Set<Index<N>, N>) -> Self {
        Self(cell::Cell::new(State {
            current: bits::Set::EMPTY,
            next: live,
            live,
        }))
    }

    fn ready(&self) -> bits::Set<Index<N>, N> {
        let state = self.0.get();
        state.current.union(state.next).intersect(state.live)
    }

    fn len(&self) -> array::Len<N> {
        self.0.get().live.len()
    }

    fn vacant(&self) -> Option<Index<N>> {
        let state = self.0.get();
        state.live.union(state.next).complement().first()
    }

    fn fill(&self, index: Index<N>) {
        let mut state = self.0.get();
        state.live.insert(index);
        state.next.insert(index);
        self.0.set(state);
    }

    fn begin(&self) {
        let mut state = self.0.get();
        if state.current.is_empty() {
            let next = state.next.intersect(state.live);
            if next.is_empty() {
                return;
            }
            state.current = next;
            state.next = state.next.without(state.live);
            self.0.set(state);
        }
    }

    fn front(&self) -> Option<Index<N>> {
        self.0.get().current.first()
    }

    fn take(&self, index: Index<N>) {
        let mut state = self.0.get();
        state.current.remove(index);
        self.0.set(state);
    }
}

impl<const N: usize> Member<'_, N> {
    pub(super) fn index(&self) -> Index<N> {
        self.index
    }

    pub(super) fn release(self) {
        let mut state = self.members.0.get();
        state.current.remove(self.index);
        state.next.remove(self.index);
        state.live.remove(self.index);
        self.members.0.set(state);
    }

    pub(super) fn retry(self) {
        let mut state = self.members.0.get();
        state.next.insert(self.index);
        self.members.0.set(state);
    }

    pub(super) fn retire(self) {
        let mut state = self.members.0.get();
        state.current.remove(self.index);
        state.next.insert(self.index);
        state.live.remove(self.index);
        self.members.0.set(state);
    }
}

impl<'queue, const N: usize> Drain<'queue, '_, N> {
    pub(super) fn pop(&mut self) -> Option<Member<'queue, N>> {
        let index = if let Some(index) = self.members.front() {
            self.members.take(index);
            index
        } else {
            self.endpoint.as_mut()?.pop()?
        };
        Some(Member {
            members: self.members,
            index,
        })
    }
}

impl<'queue, const N: usize> app::Drain for Drain<'queue, '_, N> {
    type Candidate<'a>
        = Index<N>
    where
        Self: 'a;
    type Item<'a>
        = Member<'queue, N>
    where
        Self: 'a;

    fn next_when<P>(
        &mut self,
        take: impl FnOnce() -> Option<P>,
    ) -> app::Selection<Self::Item<'_>, P, Self::Candidate<'_>> {
        let (index, permit) = if let Some(index) = self.members.front() {
            let Some(permit) = take() else {
                return Some(Err(index));
            };
            self.members.take(index);
            (index, permit)
        } else {
            match self.endpoint.as_mut()?.next_when(take)? {
                Ok(admitted) => admitted,
                Err(index) => return Some(Err(index)),
            }
        };
        Some(Ok((
            Member {
                members: self.members,
                index,
            },
            permit,
        )))
    }
}

impl<'d, const N: usize> Ready<'d, N> {
    pub(super) fn try_new(
        parent: task::Parent<'d>,
        live: bits::Set<Index<N>, N>,
    ) -> Result<Self, task::Error> {
        Ok(Self {
            members: Members::new(live),
            endpoint: topology::Endpoint::try_new(parent)?,
        })
    }

    pub(super) fn vacant(&self) -> Option<Vacancy<N>> {
        self.members.vacant().map(Vacancy)
    }

    pub(super) fn fill(self: pin::Pin<&Self>, slot: Vacancy<N>) {
        self.members.fill(slot.0);
    }

    pub(super) fn attach<'a>(
        self: pin::Pin<&'a Self>,
        index: Index<N>,
    ) -> topology::Edge<'a, 'd, Index<N>> {
        self.get_ref().endpoint.attach(index)
    }

    pub(super) fn begin(&self) -> Drain<'_, 'd, N> {
        self.members.begin();
        Drain {
            members: &self.members,
            endpoint: self.endpoint.drain(),
        }
    }

    pub(super) fn is_ready(&self) -> bool {
        !self.members.ready().is_empty() || !self.endpoint.is_empty()
    }

    pub(super) fn len(&self) -> array::Len<N> {
        self.members.len()
    }
}

const _: () = {
    assert!(core::mem::size_of::<State<16>>() == core::mem::size_of::<u64>());
    assert!(core::mem::align_of::<State<16>>() == core::mem::align_of::<u64>());
    assert!(core::mem::size_of::<Members<16>>() == core::mem::size_of::<u64>());
    assert!(core::mem::size_of::<Ready<'static, 16>>() == 3 * core::mem::size_of::<usize>());
};
