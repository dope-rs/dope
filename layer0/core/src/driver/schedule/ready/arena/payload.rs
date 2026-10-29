use std::{cell, marker, mem, num};

use dope_sys::table::permit;
use o3::base::marker::ThreadBound;

use crate::driver::{
    route,
    schedule::{ready, ready::Index as _},
};

#[derive(Clone, Copy)]
pub(in crate::driver::schedule::ready) struct Link {
    pub(in crate::driver::schedule::ready) next: Option<ready::TaskIndex>,
    pub(in crate::driver::schedule::ready) prev: Option<ready::TaskIndex>,
}

impl Link {
    pub(in crate::driver::schedule::ready) const EMPTY: Self = Self {
        next: None,
        prev: None,
    };

    pub(in crate::driver::schedule::ready) fn from_word(word: u64) -> Self {
        let next = word as u16;
        let prev = (word >> 16) as u16;
        Self {
            next: num::NonZero::<u16>::new(next).map(ready::TaskIndex::from_code),
            prev: num::NonZero::<u16>::new(prev).map(ready::TaskIndex::from_code),
        }
    }

    pub(in crate::driver::schedule::ready) fn word(self) -> u64 {
        let next = self.next.map_or(0, |index| index.code().get());
        let prev = self.prev.map_or(0, |index| index.code().get());
        u64::from(next) | (u64::from(prev) << 16)
    }
}

/// Three-word resident value whose state is owned by the adjacent resident cell.
#[repr(C)]
pub(in crate::driver::schedule::ready) struct Payload {
    words: [cell::Cell<u64>; 3],
    thread: ThreadBound,
}

pub(in crate::driver::schedule::ready) struct Task<'d> {
    payload: Payload,
    driver: marker::PhantomData<fn(ready::Key<'d>) -> ready::Key<'d>>,
}

#[derive(Clone, Copy)]
pub(in crate::driver::schedule::ready) struct Bound<'a> {
    payload: &'a Payload,
}

impl Payload {
    pub(in crate::driver::schedule::ready) fn free(next: ready::FreeIndex) -> Self {
        Self {
            words: [
                cell::Cell::new(u64::from(next.code().get() - 1)),
                cell::Cell::new(0),
                cell::Cell::new(0),
            ],
            thread: ThreadBound::NEW,
        }
    }

    pub(in crate::driver::schedule::ready) fn retired() -> Self {
        Self {
            words: [cell::Cell::new(0), cell::Cell::new(0), cell::Cell::new(0)],
            thread: ThreadBound::NEW,
        }
    }

    pub(in crate::driver::schedule::ready) fn set_free(&self, next: ready::FreeIndex) {
        self.words[0].set(u64::from(next.code().get() - 1));
    }

    /// Restores the admitted nonzero successor from its bounded offset, including tail self-links.
    pub(in crate::driver::schedule::ready) fn reserve(&self) -> ready::FreeIndex {
        ready::FreeIndex::from_code(
            num::NonZero::<u16>::MIN.saturating_add(self.words[0].get() as u16),
        )
    }

    pub(in crate::driver::schedule::ready) fn set_dispatch(&self, target: route::Token) {
        let packed = route::raw::Packed::new(target, route::raw::Auxiliary::from(0));
        let [first, second] = packed.words();
        self.words[0].set(first);
        self.words[1].set(second);
    }

    pub(in crate::driver::schedule::ready) fn dispatch(&self) -> route::Token {
        // SAFETY: this accessor is reachable only in the dispatch payload state,
        // whose constructor stored these words from one Token.
        let packed = unsafe {
            route::raw::Packed::restore([self.words[0].get(), self.words[1].get()], self.thread)
        };
        packed.target()
    }

    pub(in crate::driver::schedule::ready) fn overwrite(&self, source: Self) {
        for (target, source) in self.words.iter().zip(source.words) {
            target.set(source.into_inner());
        }
    }

    pub(in crate::driver::schedule::ready) fn word(&self, index: usize) -> u64 {
        self.words[index].get()
    }

    pub(in crate::driver::schedule::ready) fn set_word(&self, index: usize, value: u64) {
        self.words[index].set(value);
    }
}

impl<'d> Task<'d> {
    pub(in crate::driver::schedule::ready) fn new(
        parent: ready::Key<'d>,
        topology: &ready::TopologyKey<'d>,
        local: u32,
    ) -> Self {
        let topology = ready::RootIndex::new(topology);
        let payload = Payload {
            words: [
                cell::Cell::new(permit::raw::Key::identity(parent.raw())),
                cell::Cell::new(u64::from(topology.code().get()) | (u64::from(local) << 32)),
                cell::Cell::new(Link::EMPTY.word()),
            ],
            thread: ThreadBound::NEW,
        };
        Self {
            payload,
            driver: marker::PhantomData,
        }
    }

    pub(in crate::driver::schedule::ready) fn into_payload(self) -> Payload {
        self.payload
    }
}

impl<'a> Bound<'a> {
    pub(in crate::driver::schedule::ready) const fn new(payload: &'a Payload) -> Self {
        Self { payload }
    }

    pub(in crate::driver::schedule::ready) fn record(self) -> ready::Record {
        let binding = self.payload.word(1);
        // SAFETY: only Resident<Task> creates Bound, and Task::new stores the
        // parent's exact Stored word without modification.
        let word = self.payload.word(0);
        // SAFETY: the stored payload retains a nonzero index in its low half.
        let index = unsafe { num::NonZero::<u16>::new_unchecked(word as u16) };
        let epoch = ready::Generation::from_raw((word >> 32) as u32);
        let parent = ready::Stored::from_parts(index, epoch);
        // SAFETY: the stored payload retains a nonzero binding in its low half.
        let topology = unsafe { num::NonZero::<u16>::new_unchecked(binding as u16) };
        ready::Record::new(
            parent,
            ready::RootIndex::from_code(topology),
            (binding >> 32) as u32,
        )
    }

    pub(in crate::driver::schedule::ready) fn link(self) -> Link {
        Link::from_word(self.payload.word(2))
    }

    pub(in crate::driver::schedule::ready) fn set_link(self, link: Link) {
        self.payload.set_word(2, link.word());
    }

    pub(in crate::driver::schedule::ready) fn clear_link(self) {
        self.set_link(Link::EMPTY);
    }
}

const _: () = {
    assert!(mem::size_of::<Payload>() == 3 * mem::size_of::<u64>());
    assert!(mem::align_of::<Payload>() == mem::align_of::<u64>());
    assert!(mem::size_of::<Task<'static>>() == mem::size_of::<Payload>());
    assert!(mem::size_of::<Link>() == mem::size_of::<u32>());
};
