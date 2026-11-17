use std::{cell, marker, mem::size_of};

use o3::{base::marker::ThreadBound, depot, depot::arena::linked::fifo::ends};

use crate::link::egress::metadata::{batch::chain, pool::store::index::Index};

pub(in crate::link::egress) struct Lane<'d> {
    links: ends::Cell<Option<Index<'d>>>,
    pub(in crate::link::egress) reserve: [u32; 2],
    pub(in crate::link::egress) held: cell::Cell<[u32; 2]>,
    thread: marker::PhantomData<ThreadBound>,
}

/// Owner-bound proof of one retained nonempty lane prefix.
pub(in crate::link::egress) struct Prefix<'a, 'd>(
    ends::Front<&'a ends::Cell<Option<Index<'d>>>, Option<Index<'d>>>,
);

impl Lane<'_> {
    pub(in crate::link::egress) fn new(reserve: [u32; 2]) -> Self {
        Self {
            links: ends::Cell::empty(),
            reserve,
            held: cell::Cell::new([0; 2]),
            thread: marker::PhantomData,
        }
    }
}

impl<'d> Lane<'d> {
    pub(in crate::link::egress) fn is_empty(&self) -> bool {
        self.links.is_empty()
    }

    pub(in crate::link::egress::metadata) fn head(&self) -> Option<Index<'d>> {
        self.links.head()
    }

    pub(in crate::link::egress::metadata) fn push(
        &self,
        source: Index<'d>,
        publish: impl FnOnce(Option<Index<'d>>),
    ) {
        unsafe {
            ends::Cell::push_back(depot::raw::Proof::new((&self.links, source, |prev| {
                publish(prev)
            })));
        }
    }

    pub(in crate::link::egress::metadata) fn append(
        &self,
        source: &mut chain::Chain<'d>,
        join: impl FnOnce(Index<'d>, Index<'d>),
    ) {
        unsafe {
            ends::Cell::append(depot::raw::Proof::new((
                &self.links,
                source.links(),
                |prev, source| {
                    if let Some(prev) = prev {
                        join(prev, source);
                    }
                },
            )));
        }
    }

    pub(in crate::link::egress::metadata) fn restore(
        &self,
        slot: Index<'d>,
        link: impl FnOnce(Option<Index<'d>>),
    ) {
        unsafe {
            ends::Cell::push_front(depot::raw::Proof::new((&self.links, slot, |prev| {
                link(prev);
            })));
        }
    }
}

impl<'a, 'd> Prefix<'a, 'd> {
    pub(in crate::link::egress) fn try_new(lane: &'a Lane<'d>) -> Option<Self> {
        lane.links.front().map(Self)
    }

    pub(in crate::link::egress::metadata) fn index(&self) -> Index<'d> {
        self.0.get()
    }

    pub(in crate::link::egress::metadata) fn pop<T>(
        self,
        take: impl FnOnce(Index<'d>) -> (Option<Index<'d>>, T),
    ) -> T {
        unsafe {
            ends::Front::pop(depot::raw::Proof::new((self.0, (), |slot, ()| take(slot))))
        }
    }

    pub(in crate::link::egress::metadata) fn advance<T>(
        self,
        take: impl FnOnce(Index<'d>) -> (Option<Index<'d>>, T),
    ) -> (T, Option<Self>) {
        // SAFETY: the detached front supplies its exact successor without reentry.
        let (value, next) = unsafe {
            ends::Front::advance(depot::raw::Proof::new((self.0, (), |slot, ()| take(slot))))
        };
        (value, next.map(Self))
    }
}

const _: () = assert!(size_of::<Lane<'static>>() == 6 * size_of::<u32>());
const _: () = assert!(size_of::<Prefix<'static, 'static>>() == 2 * size_of::<usize>());
