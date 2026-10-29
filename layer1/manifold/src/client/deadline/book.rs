use std::mem;

use dope_core::driver::{
    route::{Identity, KeyTag, table},
    schedule::clock,
};
use dope_net::link::pool;
use o3::{
    base::time::Positive,
    depot,
    depot::heap::{self, resident},
};

use crate::client::{conn, deadline};

type Heap<'d, ID> = resident::Min<pool::raw::Key<'d, ID>, Entry<'d>, Lane<'d>>;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Entry<'d> {
    at: clock::Deadline<'d>,
    kind: deadline::Kind,
}

#[derive(Clone, Copy)]
struct Lane<'d> {
    fixed: [Option<clock::Deadline<'d>>; deadline::Fixed::COUNT],
    reception: Option<Reception<'d>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Reception<'d> {
    at: clock::Deadline<'d>,
    since: clock::Moment<'d>,
}

impl<'d> Lane<'d> {
    const EMPTY: Self = Self {
        fixed: [None; deadline::Fixed::COUNT],
        reception: None,
    };

    fn next(&self) -> Option<Entry<'d>> {
        let fixed = deadline::Fixed::ALL
            .into_iter()
            .zip(self.fixed.iter().copied())
            .filter_map(|(kind, at)| {
                at.map(|at| Entry {
                    at,
                    kind: kind.kind(),
                })
            });
        let inbound = self.reception.map(|recv| Entry {
            at: recv.at,
            kind: deadline::Kind::Inbound,
        });
        fixed.chain(inbound).min()
    }

    fn arm<K: depot::raw::Address>(
        entry: resident::Entry<'_, K, Entry<'d>, Self>,
        kind: deadline::Fixed,
        at: clock::Deadline<'d>,
    ) {
        entry.update(
            || {
                let mut lane = Self::EMPTY;
                lane.fixed[kind.index()] = Some(at);
                (
                    lane,
                    Entry {
                        at,
                        kind: kind.kind(),
                    },
                )
            },
            |lane| lane.set(kind, at),
        );
    }

    fn set(&mut self, kind: deadline::Fixed, at: clock::Deadline<'d>) -> Entry<'d> {
        let armed = Entry {
            at,
            kind: kind.kind(),
        };
        self.fixed[kind.index()] = Some(at);
        self.next().unwrap_or(armed)
    }

    fn cancel(&mut self, kind: deadline::Kind) {
        match kind {
            deadline::Kind::Connect => self.fixed[deadline::Fixed::Connect.index()] = None,
            deadline::Kind::Inbound => self.reception = None,
            deadline::Kind::Send => self.fixed[deadline::Fixed::Send.index()] = None,
            deadline::Kind::Lifetime => self.fixed[deadline::Fixed::Lifetime.index()] = None,
        }
    }

    fn is_armed(&self, kind: deadline::Fixed) -> bool {
        self.fixed[kind.index()].is_some()
    }

    fn recv_since(
        &self,
        now: clock::Moment<'d>,
        seen: Option<clock::Moment<'d>>,
    ) -> clock::Moment<'d> {
        match self.reception {
            Some(recv) => match seen {
                Some(seen) if seen > recv.since => seen,
                Some(_) | None => recv.since,
            },
            None => now,
        }
    }

    fn arm_recv(&mut self, since: clock::Moment<'d>, at: clock::Deadline<'d>) {
        self.reception = Some(Reception { at, since });
    }
}

pub(in crate::client) struct Book<'d, ID: Identity> {
    entries: Heap<'d, ID>,
}

/// A popped deadline borrowing its book until the caller resolves the expiry.
#[must_use = "an expired deadline must be resolved"]
pub(in crate::client) struct Expired<'a, 'd, ID: Identity> {
    removed: resident::Removed<'a, pool::raw::Key<'d, ID>, Entry<'d>, Lane<'d>>,
}

#[repr(transparent)]
pub(in crate::client) struct Inbound<'a, 'd, ID: Identity>(Expired<'a, 'd, ID>);

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::client) enum Rearm {
    Future,
    Expired,
}

impl<'d, ID: Identity> Book<'d, ID> {
    pub(in crate::client) fn try_new(capacity: table::Capacity) -> Result<Self, heap::Error> {
        Ok(Self {
            entries: resident::Min::try_repeat(capacity.count(), Lane::EMPTY)?,
        })
    }

    pub(in crate::client) fn earliest(&self) -> Option<clock::Deadline<'d>> {
        self.entries.peek().map(|pair| pair.1.at)
    }

    pub(in crate::client) fn pop_expired(
        &mut self,
        now: clock::Moment<'d>,
    ) -> Option<Expired<'_, 'd, ID>> {
        let entry = self
            .entries
            .first_entry_if(|priority| !(priority.at > now))?;
        let removed = entry.remove();
        Some(Expired { removed })
    }
}

impl<'a, 'd, ID: Identity> Expired<'a, 'd, ID> {
    pub(in crate::client) fn key(&self) -> pool::Key<'d, ID> {
        self.removed.key().into_key()
    }

    pub(in crate::client) fn classify<R>(
        self,
        inbound: impl FnOnce(Inbound<'a, 'd, ID>) -> R,
        other: impl FnOnce(Self, deadline::Kind) -> R,
    ) -> R {
        let kind = self.removed.priority().kind;
        match kind {
            deadline::Kind::Inbound => inbound(Inbound(self)),
            _ => other(self, kind),
        }
    }

    /// Retires the complete connection lane.
    pub(in crate::client) fn clear(mut self) {
        *self.lane_mut() = Lane::EMPTY;
    }

    fn lane(&self) -> &Lane<'d> {
        self.removed.resident()
    }

    fn lane_mut(&mut self) -> &mut Lane<'d> {
        self.removed.resident_mut()
    }

    fn commit_next(self) {
        if let Some(next) = self.lane().next() {
            self.removed.reinsert(next);
        }
    }
}

impl<'d, ID: Identity> Inbound<'_, 'd, ID> {
    /// Resolves a quiescent inbound policy without extending its idle origin.
    pub(in crate::client) fn cancel_recv(self) {
        let mut expired = self.0;
        let lane = expired.lane_mut();
        lane.reception = None;
        expired.commit_next();
    }

    /// Re-evaluates the popped inbound deadline under the same exact key. A
    /// result that is still expired retires the lane for the caller's close;
    /// only a future deadline is restored to the heap.
    pub(in crate::client) fn rearm_recv(
        self,
        now: clock::Moment<'d>,
        seen: Option<clock::Moment<'d>>,
        window: Positive,
    ) -> Rearm {
        let mut expired = self.0;
        let since = expired.lane().recv_since(now, seen);
        let at = since.clamp_add(window);
        if at <= now {
            *expired.lane_mut() = Lane::EMPTY;
            return Rearm::Expired;
        }
        let lane = expired.lane_mut();
        lane.arm_recv(since, at);
        expired.commit_next();
        Rearm::Future
    }
}

const _: () = {
    assert!(
        mem::size_of::<Lane<'static>>()
            <= (deadline::Fixed::COUNT + 1) * mem::size_of::<Option<clock::Deadline<'static>>>()
                + mem::size_of::<Option<clock::Moment<'static>>>()
    );
    if usize::BITS == 64 {
        assert!(mem::size_of::<Book<'static, KeyTag<0>>>() == 4 * mem::size_of::<usize>());
    }
};

pub(in crate::client) struct Slot<'a, 'd, ID: Identity, C> {
    entry: conn::Source<&'a mut Heap<'d, ID>, C, pool::raw::Key<'d, ID>>,
}

impl<'d, ID: Identity, C> Slot<'_, 'd, ID, C> {
    pub(in crate::client) fn closing(self, now: clock::Moment<'d>, window: Positive) -> C {
        let (source, ()) = self.entry.with(|entry| {
            let at = match &entry {
                resident::Entry::Occupied(current) => {
                    current.resident().fixed[deadline::Fixed::Send.index()]
                }
                resident::Entry::Vacant(_) | resident::Entry::Conflict(_) => None,
            }
            .unwrap_or_else(|| now.clamp_add(window));
            let mut lane = Lane::EMPTY;
            let next = lane.set(deadline::Fixed::Send, at);
            entry.update(
                || (lane, next),
                |current| {
                    *current = lane;
                    next
                },
            );
        });
        source
    }

    pub(in crate::client) fn arm_after(
        self,
        kind: deadline::Fixed,
        now: clock::Moment<'d>,
        window: Positive,
    ) -> C {
        self.arm_at(kind, now.clamp_add(window))
    }

    pub(in crate::client) fn ensure_after(
        self,
        kind: deadline::Fixed,
        now: clock::Moment<'d>,
        window: Positive,
    ) -> C {
        let (source, ()) = self.entry.with(|entry| {
            if let resident::Entry::Occupied(current) = &entry
                && current.resident().is_armed(kind)
            {
                return;
            }
            let at = now.clamp_add(window);
            Lane::arm(entry, kind, at);
        });
        source
    }

    pub(in crate::client) fn cancel(self, kind: deadline::Kind) -> C {
        let (source, ()) = self.entry.with(|entry| {
            let mut entry = match entry {
                resident::Entry::Occupied(entry) => entry,
                resident::Entry::Vacant(_) | resident::Entry::Conflict(_) => return,
            };
            let first = entry.priority().kind;
            let next = {
                let lane = entry.resident_mut();
                lane.cancel(kind);
                if first != kind {
                    return;
                }
                lane.next()
            };
            match next {
                Some(next) => {
                    entry.set(next);
                }
                None => {
                    entry.retire();
                }
            }
        });
        source
    }

    pub(in crate::client) fn arm_inbound(
        self,
        now: clock::Moment<'d>,
        seen: Option<clock::Moment<'d>>,
        window: Positive,
    ) -> C {
        let (source, ()) = self.entry.with(|entry| {
            let since = match &entry {
                resident::Entry::Occupied(entry) => entry.resident().recv_since(now, seen),
                resident::Entry::Vacant(_) | resident::Entry::Conflict(_) => now,
            };
            let at = since.clamp_add(window);
            if let resident::Entry::Occupied(entry) = &entry
                && entry.resident().reception == Some(Reception { at, since })
            {
                return;
            }
            let deadline = Entry {
                at,
                kind: deadline::Kind::Inbound,
            };
            entry.update(
                || {
                    let mut lane = Lane::EMPTY;
                    lane.arm_recv(since, at);
                    (lane, deadline)
                },
                |lane| {
                    lane.arm_recv(since, at);
                    lane.next().unwrap_or(deadline)
                },
            );
        });
        source
    }

    pub(in crate::client) fn clear(self) -> C {
        let (source, ()) = self.entry.with(|entry| entry.clear(Lane::EMPTY));
        source
    }

    pub(in crate::client) fn arm_at(self, kind: deadline::Fixed, at: clock::Deadline<'d>) -> C {
        let (source, ()) = self.entry.with(|entry| Lane::arm(entry, kind, at));
        source
    }
}

impl<'a, 'd, ID: Identity, C> Slot<'a, 'd, ID, C> {
    pub(in crate::client) fn new(
        source: conn::Source<&'a mut Book<'d, ID>, C, pool::Key<'d, ID>>,
    ) -> Self {
        // SAFETY: projection preserves the exact source and its physical heap plane.
        let source =
            unsafe { source.map(|((book, owner), key)| ((&mut book.entries, owner), key.into())) };
        Slot { entry: source }
    }
}
