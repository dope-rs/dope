//! One-allocation exact deadline authority for a listener lane.

use std::{mem, ops};

use dope_core::driver::{
    route::{Identity, KeyTag},
    schedule::clock,
};
use dope_net::{
    link::{egress, pool, slot::types},
    wire,
};
use o3::{
    base::time::Positive,
    depot,
    depot::{
        capacity,
        heap::{self, resident},
        index::Initialized,
    },
};

use crate::{
    network::listener::{
        conn::deadline::{Arm, Overflow},
        handler,
        writer::flow,
    },
    timing,
};

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
struct Deadline<'d> {
    at: clock::Deadline<'d>,
    kind: Kind,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub(in crate::network::listener) enum Kind {
    Inbound,
    Send,
    Application,
    Absolute,
    Drain,
}

impl Kind {
    const COUNT: usize = 5;
    const ALL: [Self; Self::COUNT] = [
        Self::Inbound,
        Self::Send,
        Self::Application,
        Self::Absolute,
        Self::Drain,
    ];

    const fn index(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Copy)]
struct Lane<'d> {
    deadlines: [Option<clock::Deadline<'d>>; Kind::COUNT],
}

impl<'d> Lane<'d> {
    const EMPTY: Self = Self {
        deadlines: [None; Kind::COUNT],
    };

    fn next(&self) -> Option<Deadline<'d>> {
        let mut next: Option<Deadline<'d>> = None;
        for kind in Kind::ALL {
            let Some(at) = self.deadlines[kind.index()] else {
                continue;
            };
            let armed = Deadline { at, kind };
            if next.is_none_or(|current| armed < current) {
                next = Some(armed);
            }
        }
        next
    }

    fn set(&mut self, kind: Kind, at: clock::Deadline<'d>) -> Deadline<'d> {
        let armed = Deadline { at, kind };
        self.deadlines[kind.index()] = Some(at);
        self.next().unwrap_or(armed)
    }
}

struct Policy {
    inbound: Positive,
    send: Positive,
    application: handler::Timing,
    termination: handler::Termination,
}

pub(in crate::network::listener) struct Schedule<'d, ID: Identity> {
    entries: resident::Min<pool::raw::Key<'d, ID>, Deadline<'d>, Lane<'d>>,
    policy: Policy,
}

impl<'d, ID: Identity> Schedule<'d, ID> {
    pub(in crate::network::listener::writer) fn try_new<P: timing::Policy>(
        cap: capacity::Count,
        life: handler::Lifetime,
        times: handler::Timing,
    ) -> Result<Self, heap::Error> {
        Ok(Self {
            entries: resident::Min::try_repeat(cap, Lane::EMPTY)?,
            policy: Policy {
                inbound: P::IDLE_WINDOW,
                send: P::SEND_DEADLINE,
                application: times,
                termination: life.termination(),
            },
        })
    }

    pub(in crate::network::listener) fn earliest(&self) -> Option<clock::Deadline<'d>> {
        self.entries.peek().map(|pair| pair.1.at)
    }

    pub(in crate::network::listener) const fn drain(&self) -> Option<Positive> {
        match self.policy.termination {
            handler::Termination::Graceful {
                drain_timeout: drain,
                ..
            } => Some(drain),
            handler::Termination::Persistent => None,
        }
    }

    pub(in crate::network::listener) fn pop(
        &mut self,
        now: clock::Moment<'d>,
    ) -> Option<(pool::Key<'d, ID>, Kind)> {
        let mut entry = self
            .entries
            .first_entry_if(|priority| !(priority.at > now))?;
        let key = entry.key();
        let due = *entry.priority();
        let next = {
            let lane = entry.resident_mut();
            lane.deadlines[due.kind.index()] = None;
            lane.next()
        };
        match next {
            Some(next) => entry.set(next),
            None => entry.retire(),
        }
        Some((key.into_key(), due.kind))
    }
}

struct Entry<'a, 'd, ID: Identity> {
    entry: resident::Entry<'a, pool::raw::Key<'d, ID>, Deadline<'d>, Lane<'d>>,
    policy: &'a Policy,
}

impl<'d, ID: Identity> Entry<'_, 'd, ID> {
    fn open(self, idle: clock::Deadline<'d>, limit: Option<clock::Deadline<'d>>) {
        let mut lane = Lane::EMPTY;
        let mut next = lane.set(Kind::Inbound, idle);
        if let Some(limit) = limit {
            next = lane.set(Kind::Absolute, limit);
        }
        self.entry.update(
            || (lane, next),
            |current| {
                *current = lane;
                next
            },
        );
    }

    fn is_armed(&self, kind: Kind) -> bool {
        match &self.entry {
            resident::Entry::Occupied(entry) => entry.resident().deadlines[kind.index()].is_some(),
            resident::Entry::Vacant(_) | resident::Entry::Conflict(_) => false,
        }
    }

    fn arm_at(self, kind: Kind, at: clock::Deadline<'d>) {
        if let resident::Entry::Occupied(entry) = &self.entry
            && entry.resident().deadlines[kind.index()] == Some(at)
        {
            return;
        }
        self.entry.update(
            || {
                let mut lane = Lane::EMPTY;
                let next = lane.set(kind, at);
                (lane, next)
            },
            |lane| lane.set(kind, at),
        );
    }

    fn cancel(self, kind: Kind) {
        let mut entry = match self.entry {
            resident::Entry::Occupied(entry) => entry,
            resident::Entry::Vacant(_) | resident::Entry::Conflict(_) => return,
        };
        let first = entry.priority().kind;
        let next = {
            let lane = entry.resident_mut();
            lane.deadlines[kind.index()] = None;
            if first != kind {
                return;
            }
            lane.next()
        };
        match next {
            Some(next) => entry.set(next),
            None => entry.retire(),
        }
    }
}

enum Mode<'a, 'd, ID: Identity> {
    Open(&'a mut Schedule<'d, ID>),
    Sealed,
}

/// The original source connection and its paired immutable-capacity schedule.
#[repr(C)]
pub(in crate::network::listener) struct Slot<
    'a,
    'w,
    'd: 'w,
    ID: Identity,
    W: wire::Spec + 'w + 'd,
    S,
> {
    source: &'a mut types::Connection<'w, 'd, ID, W, S>,
    mode: Mode<'a, 'd, ID>,
}

impl<'a, 'w, 'd: 'w, ID: Identity, W: wire::Spec + 'w + 'd, S> Slot<'a, 'w, 'd, ID, W, S> {
    pub(in crate::network::listener) fn closed(
        source: &'a mut types::Connection<'w, 'd, ID, W, S>,
    ) -> Self {
        Self {
            source,
            mode: Mode::Sealed,
        }
    }

    pub(in crate::network::listener) fn reborrow(&mut self) -> Slot<'_, 'w, 'd, ID, W, S> {
        Slot {
            source: self.source,
            mode: match &mut self.mode {
                Mode::Open(book) => Mode::Open(book),
                Mode::Sealed => Mode::Sealed,
            },
        }
    }

    pub(in crate::network::listener) fn open(&mut self, now: clock::Moment<'d>) {
        self.with((), |entry| {
            let policy = entry.policy;
            let idle = now.clamp_add(policy.inbound);
            let limit = match policy.termination {
                handler::Termination::Graceful {
                    absolute_age: age, ..
                } => Some(now.clamp_add(age)),
                handler::Termination::Persistent => None,
            };
            entry.open(idle, limit);
        });
    }

    pub(in crate::network::listener) fn progress(&mut self, now: clock::Moment<'d>) {
        self.with((), |entry| {
            let policy = entry.policy;
            entry.arm_at(Kind::Inbound, now.clamp_add(policy.inbound))
        });
    }

    pub(in crate::network::listener) fn ensure_send(&mut self, now: clock::Moment<'d>) {
        self.with((), |entry| {
            let policy = entry.policy;
            if !entry.is_armed(Kind::Send) {
                entry.arm_at(Kind::Send, now.clamp_add(policy.send));
            }
        });
    }

    pub(in crate::network::listener) fn cancel_send(&mut self) {
        self.with((), |entry| {
            entry.cancel(Kind::Send);
        });
    }

    pub(in crate::network::listener) fn cancel_idle(&mut self, pending: egress::Pending)
    where
        types::Connection<'w, 'd, ID, W, S>: flow::SlotFlow<'d, ID>,
    {
        if matches!(
            flow::SlotFlow::flow(self.source, pending),
            flow::Flow::Clear
        ) {
            self.cancel_send();
        }
    }

    pub(in crate::network::listener) fn try_ensure(&mut self, now: clock::Moment<'d>) -> Arm<'d> {
        self.with(Arm::Sealed, |entry| {
            let policy = entry.policy;
            let Some(window) = policy.application.delay() else {
                return Arm::Disabled;
            };
            if entry.is_armed(Kind::Application) {
                return Arm::Armed;
            }
            match now.try_add(window) {
                Ok(at) => {
                    entry.arm_at(Kind::Application, at);
                    Arm::Armed
                }
                Err(range) => Arm::Overflow(Overflow {
                    now,
                    window: range.duration(),
                }),
            }
        })
    }

    pub(in crate::network::listener) fn try_arm(&mut self, at: clock::Deadline<'d>) -> Arm<'d> {
        self.with(Arm::Sealed, |entry| {
            let policy = entry.policy;
            if !policy.application.is_managed() {
                return Arm::Disabled;
            }
            entry.arm_at(Kind::Application, at);
            Arm::Armed
        })
    }

    pub(in crate::network::listener) fn cancel_app(&mut self) {
        self.with((), |entry| {
            entry.cancel(Kind::Application);
        });
    }

    pub(in crate::network::listener) fn apply(&mut self, update: handler::Deadline<'d>) -> Arm<'d> {
        match update {
            handler::Deadline::Ensure(now) => self.try_ensure(now),
            handler::Deadline::Arm(at) => self.try_arm(at),
            handler::Deadline::Clear => {
                self.cancel_app();
                Arm::Armed
            }
        }
    }

    pub(in crate::network::listener) fn ensure_drain(
        &mut self,
        now: clock::Moment<'d>,
        drain: Positive,
    ) {
        self.with((), |entry| {
            if !entry.is_armed(Kind::Drain) {
                entry.arm_at(Kind::Drain, now.clamp_add(drain));
            }
        });
    }

    pub(in crate::network::listener) fn closing(&mut self, now: clock::Moment<'d>) {
        self.with((), |entry| {
            let mut lane = Lane::EMPTY;
            if let resident::Entry::Occupied(current) = &entry.entry {
                let active = &current.resident().deadlines;
                lane.deadlines[Kind::Send.index()] = active[Kind::Send.index()];
                lane.deadlines[Kind::Drain.index()] = active[Kind::Drain.index()];
            }
            let at = lane.deadlines[Kind::Send.index()]
                .unwrap_or_else(|| now.clamp_add(entry.policy.send));
            let next = lane.set(Kind::Send, at);
            entry.entry.update(
                || (lane, next),
                |current| {
                    *current = lane;
                    next
                },
            );
        });
    }

    pub(in crate::network::listener) fn clear(&mut self) {
        self.with((), |entry| {
            entry.entry.clear(Lane::EMPTY);
        });
    }

    fn with<O>(&mut self, sealed: O, run: impl FnOnce(Entry<'_, 'd, ID>) -> O) -> O {
        let Mode::Open(book) = &mut self.mode else {
            return sealed;
        };
        let key = self.source.key().into();
        // SAFETY: the indivisible slot retains its original native source and paired schedule.
        let slot = unsafe { depot::raw::Proof::new(((&mut book.entries, &mut *self.source), key)) };
        slot.with(|entry| {
            run(Entry {
                entry,
                policy: &book.policy,
            })
        })
        .1
    }
}

impl<'w, 'd: 'w, ID: Identity, W: wire::Spec + 'w + 'd, S> ops::Deref
    for Slot<'_, 'w, 'd, ID, W, S>
{
    type Target = types::Connection<'w, 'd, ID, W, S>;
    fn deref(&self) -> &Self::Target {
        self.source
    }
}

impl<'w, 'd: 'w, ID: Identity, W: wire::Spec + 'w + 'd, S> ops::DerefMut
    for Slot<'_, 'w, 'd, ID, W, S>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.source
    }
}

const _: () = assert!(
    mem::size_of::<Slot<'static, 'static, 'static, KeyTag<0>, wire::Identity, ()>>()
        == 2 * mem::size_of::<usize>()
);

impl<'a, 'w, 'd: 'w, ID: Identity, W: wire::Spec + 'w + 'd, S>
    From<
        depot::raw::Proof<
            (
                (
                    &'a mut types::Connection<'w, 'd, ID, W, S>,
                    &'a mut Schedule<'d, ID>,
                ),
                pool::Key<'d, ID>,
            ),
            Initialized<()>,
        >,
    > for Slot<'a, 'w, 'd, ID, W, S>
{
    fn from(
        proof: depot::raw::Proof<
            (
                (
                    &'a mut types::Connection<'w, 'd, ID, W, S>,
                    &'a mut Schedule<'d, ID>,
                ),
                pool::Key<'d, ID>,
            ),
            Initialized<()>,
        >,
    ) -> Self {
        let ((source, book), _) = proof.into_inner();
        Self {
            source,
            mode: Mode::Open(book),
        }
    }
}
