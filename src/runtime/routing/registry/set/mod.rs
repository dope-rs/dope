use crate::runtime::routing::registry;
use o3::buffer;
use std::{num::NonZero, time};

use o3::depot::{
    capacity,
    heap::indexed,
    queue::{
        indexed::{
            lanes::runtime,
            planes::{self, bound, groups, resident},
        },
        links,
    },
};
use o3::mem::slot;

use crate::runtime::mux;
use crate::runtime::mux::manage::config::ResizeError;
use crate::runtime::mux::router;
use crate::runtime::mux::router::drive::Step;
use crate::{conn, stream};
use conn::{id, wire::path::reset::Token};
use router::reset;
use router::routing::issuer;

pub(super) mod entry;
pub(super) mod routes;
pub(super) mod scope;

use entry::{Entries, Entry};

type Root<C, const DOMAIN: u8, B, D, T> = planes::Set<
    slot::Storage<registry::Slot<C, DOMAIN, B, D, T>, registry::Epoch>,
    { mux::QUEUE_PLANES },
    groups::Index<time::Instant, capacity::Limit<{ mux::MAX_CONNECTIONS }>, Token, 2, 4>,
>;

type Bound<'a, 'id, C, const DOMAIN: u8, B, D, T> = bound::Scope<
    'a,
    'id,
    slot::Storage<registry::Slot<C, DOMAIN, B, D, T>, registry::Epoch>,
    { mux::QUEUE_PLANES },
    bound::Active<'a, time::Instant, capacity::Limit<{ mux::MAX_CONNECTIONS }>>,
>;

type Live<'a, 'id, C, const DOMAIN: u8, B, D, T> = resident::Scope<
    'a,
    'id,
    registry::Slot<C, DOMAIN, B, D, T>,
    registry::Epoch,
    { mux::QUEUE_PLANES },
    bound::Active<'a, time::Instant, capacity::Limit<{ mux::MAX_CONNECTIONS }>>,
    { mux::FREE_QUEUE },
>;

type Tokens<'a, 'id> =
    groups::Side<'a, 'id, Token, capacity::Limit<{ mux::MAX_CONNECTIONS }>, 2, 4>;

type Group<'a> = runtime::Group<
    'a,
    Token,
    groups::Rows<capacity::Limit<{ mux::MAX_CONNECTIONS }>, 2>,
    groups::Buckets<capacity::Limit<{ mux::MAX_CONNECTIONS }>, 4>,
    links::List,
    2,
>;

type Record<'a, C, const DOMAIN: u8, B, D, T> = planes::Entry<
    'a,
    slot::Storage<registry::Slot<C, DOMAIN, B, D, T>, registry::Epoch>,
    { mux::QUEUE_PLANES },
>;

/// Counts blocked FLUSH heads; queue changes or returned byte credit restart the sweep.
#[derive(Default)]
struct Flush {
    blocked: usize,
    bytes: usize,
}

impl Flush {
    fn blocked_at(&self, bytes: usize) -> usize {
        if self.bytes == bytes { self.blocked } else { 0 }
    }
}

#[derive(Default)]
struct Counts {
    active: u32,
    retained: u32,
}

/// Owns every initialized slot, current routing extent and lifecycle transition.
pub(in crate::runtime) struct Set<C, const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment, T>
{
    entries: Root<C, DOMAIN, B, D, T>,
    cids: registry::Table,
    reset: reset::Reset,
    counts: Counts,
    flush: Flush,
}

impl<C, const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment, T> Set<C, DOMAIN, B, D, T> {
    fn seed(_: links::Index) -> slot::Storage<registry::Slot<C, DOMAIN, B, D, T>, registry::Epoch> {
        slot::Storage::new(registry::Epoch::default())
    }

    pub(in crate::runtime) fn try_new(
        max: capacity::Limit<{ mux::MAX_CONNECTIONS }>,
        issuer: &impl issuer::Source,
    ) -> Result<Self, router::Error> {
        let entries = planes::Set::<_, _, indexed::Min<_, _>>::try_seed_in::<{ mux::FREE_QUEUE }>(
            max,
            Self::seed,
        )
        .map_err(router::Error::from)?;
        let cids = registry::Table::try_new(max, issuer.hash_seed::<{ issuer::CID_HASH_LANE }>())
            .map_err(|error| router::Error::Cids {
            capacity: max,
            error,
        })?;
        let hash = issuer.hash_seed::<{ issuer::RESET_HASH_LANE }>();
        let entries = Root::<C, DOMAIN, B, D, T>::try_from(entries).map_err(|error| {
            router::Error::Reset {
                capacity: max,
                error,
            }
        })?;
        Ok(Self {
            entries,
            cids,
            reset: reset::Reset::new(hash),
            counts: Counts::default(),
            flush: Flush::default(),
        })
    }

    pub(in crate::runtime) fn try_resize(&mut self, max: usize) -> Result<(), ResizeError> {
        if let Some(actual) = NonZero::new(self.counts.active as usize) {
            return Err(ResizeError::Active { actual });
        }
        let max = match capacity::Limit::<{ mux::MAX_CONNECTIONS }>::new(max) {
            Some(max) => max,
            None if max == 0 => return Err(ResizeError::Zero),
            None => {
                return Err(ResizeError::Limit {
                    requested: max,
                    maximum: mux::MAX_CONNECTIONS,
                });
            }
        };
        let reset = groups::Index::<
            time::Instant,
            capacity::Limit<{ mux::MAX_CONNECTIONS }>,
            Token,
            2,
            4,
        >::reserve(max)
        .map_err(|error| router::Error::Reset {
            capacity: max,
            error,
        })
        .map_err(ResizeError::Storage)?;
        let cids = self
            .cids
            .try_reserve(max)
            .map_err(|error| router::Error::Cids {
                capacity: max,
                error,
            })
            .map_err(ResizeError::Storage)?;
        let reset = self
            .entries
            .try_replace::<{ mux::FREE_QUEUE }>(reset, Self::seed)
            .map_err(router::Error::from)
            .map_err(ResizeError::Storage)?;
        cids();
        reset();
        self.flush.blocked = 0;
        for _ in 0..self.entries.len_in::<{ mux::FREE_QUEUE }>() {
            let Some(entry) = self.entries.front_entry::<{ mux::FREE_QUEUE }>() else {
                unreachable!("vacancy");
            };
            Self::recycle(entry.pop());
        }
        Ok(())
    }

    /// Vacant entries remain claimable only while their next generation exists.
    pub(in crate::runtime::routing::registry) fn recycle(
        mut entry: Record<'_, C, DOMAIN, B, D, T>,
    ) {
        if entry.metadata().next().is_some() {
            entry.plane::<{ mux::FREE_QUEUE }>().link_back();
        } else {
            entry.plane::<{ mux::FREE_QUEUE }>().unlink();
        }
    }

    pub(in crate::runtime) fn len(&self) -> usize {
        self.counts.active as usize
    }

    pub(in crate::runtime) fn capacity(&self) -> usize {
        self.entries.len()
    }

    pub(in crate::runtime) fn is_empty_in<const PLANE: usize>(&self) -> bool {
        const { assert!(PLANE != mux::FREE_QUEUE) };
        self.entries.len_in::<PLANE>() == 0
    }

    pub(in crate::runtime) fn can_flush(&self, bytes: usize) -> bool {
        self.flush.blocked_at(bytes) < self.entries.len_in::<{ mux::FLUSH_QUEUE }>()
    }

    pub(in crate::runtime) fn deadline(&self) -> Option<time::Instant> {
        self.entries.heap().peek().map(|(_, at)| *at)
    }

    pub(in crate::runtime) fn get<I: Copy>(
        &self,
        domain: id::Domain<I, DOMAIN>,
        handle: id::Handle<I, DOMAIN>,
    ) -> Option<&registry::Slot<C, DOMAIN, B, D, T>> {
        self.bound(domain, handle)?.get()
    }

    fn bound<I: Copy>(
        &self,
        domain: id::Domain<I, DOMAIN>,
        handle: id::Handle<I, DOMAIN>,
    ) -> Option<&slot::Storage<registry::Slot<C, DOMAIN, B, D, T>, registry::Epoch>> {
        if !handle.belongs_to(domain) {
            return None;
        }
        let entry = self.entries.get(handle.index() as usize)?;
        if !entry.metadata().matches(handle.generation()) {
            return None;
        }
        Some(entry)
    }

    /// Only a reusable terminal owner makes full capacity temporary.
    pub(in crate::runtime) fn admission(&self) -> router::Admission {
        if self.entries.len_in::<{ mux::FREE_QUEUE }>() != 0 {
            return router::Admission::Available;
        }
        if self.counts.retained != 0 {
            return router::Admission::Retained;
        }
        router::Admission::Full
    }

    pub(in crate::runtime) fn sole<I: Copy>(
        &self,
        domain: id::Domain<I, DOMAIN>,
    ) -> Option<id::Handle<I, DOMAIN>> {
        if self.len() != 1 {
            return None;
        }
        (0..self.entries.len()).find_map(|index| {
            let (index, entry) = self.entries.resolve(index)?;
            entry.get()?;
            Some(domain.handle(index, entry.metadata().get()))
        })
    }

    pub(in crate::runtime) fn promote(&mut self, now: time::Instant) -> Step {
        let Some((timer, mut entry)) = self.entries.first() else {
            return Step::Idle;
        };
        if *timer.priority() > now {
            return Step::Idle;
        }
        timer.retire();
        if entry.get().is_some() {
            entry.plane::<{ mux::REAP_QUEUE }>().link_back();
        }
        Step::Driven
    }
}
