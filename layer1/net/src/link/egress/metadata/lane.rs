use std::mem::size_of;

use o3::{depot, depot::arena::linked::fifo::ends, mem};

use crate::link::egress::{
    load::credit,
    metadata::{batch::chain, pool::store::index::Index},
};

pub(in crate::link::egress) struct Lane<'d> {
    links: ends::Cell<Option<Index<'d>>>,
    pub(in crate::link::egress) credit: mem::raw::State<2, credit::Tag, u32>,
}

/// Owner-bound proof of one retained nonempty lane prefix.
pub(in crate::link::egress) struct Prefix<'a, 'd>(
    ends::Front<&'a ends::Cell<Option<Index<'d>>>, Option<Index<'d>>>,
);

impl Lane<'_> {
    pub(in crate::link::egress) fn new(credit: mem::raw::State<2, credit::Tag, u32>) -> Self {
        Self {
            links: ends::Cell::empty(),
            credit,
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

    pub(in crate::link::egress::metadata) fn push<'p>(
        &self,
        source: Index<'d>,
        stored: credit::Stored<'p>,
        publish: impl FnOnce(Option<Index<'d>>, credit::Stored<'p>),
    ) {
        unsafe {
            ends::Cell::push_back(depot::raw::Proof::new((&self.links, source, |prev| {
                publish(prev, stored)
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

    pub(in crate::link::egress::metadata) fn restore<'p>(
        &self,
        slot: Index<'d>,
        stored: credit::Stored<'p>,
        link: impl FnOnce(Option<Index<'d>>, credit::Stored<'p>),
    ) {
        unsafe {
            ends::Cell::push_front(depot::raw::Proof::new((&self.links, slot, |prev| {
                link(prev, stored);
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

    pub(in crate::link::egress::metadata) fn pop<'p, T>(
        self,
        take: impl FnOnce(Index<'d>) -> (Option<Index<'d>>, T, credit::Stored<'p>),
    ) -> (T, credit::Stored<'p>) {
        unsafe {
            ends::Front::pop(depot::raw::Proof::new((self.0, (), |slot, ()| {
                let (next, value, entry) = take(slot);
                (next, (value, entry))
            })))
        }
    }

    pub(in crate::link::egress::metadata) fn advance<'p, T>(
        self,
        take: impl FnOnce(Index<'d>) -> (Option<Index<'d>>, T, credit::Stored<'p>),
    ) -> ((T, credit::Stored<'p>), Option<Self>) {
        // SAFETY: the detached front supplies its exact successor without reentry.
        let (value, next) = unsafe {
            ends::Front::advance(depot::raw::Proof::new((self.0, (), |slot, ()| {
                let (next, value, entry) = take(slot);
                (next, (value, entry))
            })))
        };
        (value, next.map(Self))
    }
}

const _: () = assert!(size_of::<Lane<'static>>() == 6 * size_of::<u32>());
const _: () = assert!(size_of::<Prefix<'static, 'static>>() == 2 * size_of::<usize>());
