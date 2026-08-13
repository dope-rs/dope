use std::mem;

use o3::collections::{self, slab::key};

use crate::{connector::attempt, service::pool};

const NONE: u32 = u32::MAX;

pub(super) struct Started<'d, R, const ID: u8> {
    pub(super) key: attempt::Id<'d, ID>,
    pub(super) output: R,
}

#[derive(Clone, Copy)]
enum State {
    Ready,
    Deferred(pool::MemberKey),
    Selected(pool::MemberKey),
    Submitted(pool::MemberKey),
    Connected(pool::MemberKey),
    Retired,
}

struct Dial {
    generation: key::Generation,
    next: u32,
    state: State,
}

enum Head<P> {
    Empty,
    Ready(u32),
    Deferred { index: u32, plan: P },
}

struct Queue<P> {
    head: Head<P>,
    tail: u32,
}

pub(super) struct Dials<P> {
    records: Box<[Dial]>,
    ready: Queue<P>,
    pub(super) needs_poll: bool,
}

impl Dial {
    fn ready() -> Self {
        Self {
            generation: key::Generation::MIN,
            next: NONE,
            state: State::Ready,
        }
    }

    fn matches<const ID: u8>(&self, key: attempt::Id<'_, ID>) -> bool {
        self.generation == key.generation()
    }

    fn requeue(&mut self) -> bool {
        if !matches!(self.state, State::Retired) {
            return false;
        }
        let Some(generation) = self.generation.checked_add(1) else {
            return false;
        };
        self.generation = generation;
        self.state = State::Ready;
        true
    }

    fn select(&mut self, member: pool::MemberKey) -> bool {
        if !matches!(self.state, State::Ready) {
            return false;
        }
        self.state = State::Selected(member);
        true
    }

    fn resume(&mut self) -> bool {
        let State::Deferred(member) = self.state else {
            return false;
        };
        self.state = State::Selected(member);
        true
    }

    fn active<const ID: u8>(&self, key: attempt::Id<'_, ID>) -> Option<pool::MemberKey> {
        if !self.matches(key) {
            return None;
        }
        match self.state {
            State::Selected(member) | State::Submitted(member) | State::Connected(member) => {
                Some(member)
            }
            _ => None,
        }
    }

    fn take_attempt<const ID: u8>(&mut self, key: attempt::Id<'_, ID>) -> Option<pool::MemberKey> {
        if !self.matches(key) {
            return None;
        }
        let member = match self.state {
            State::Selected(member) | State::Submitted(member) => member,
            _ => return None,
        };
        self.state = State::Retired;
        Some(member)
    }

    fn take_active<const ID: u8>(&mut self, key: attempt::Id<'_, ID>) -> Option<pool::MemberKey> {
        if !self.matches(key) {
            return None;
        }
        let member = match self.state {
            State::Selected(member) | State::Submitted(member) | State::Connected(member) => member,
            _ => return None,
        };
        self.state = State::Retired;
        Some(member)
    }

    fn connected<const ID: u8>(&mut self, key: attempt::Id<'_, ID>) -> Option<pool::MemberKey> {
        if !self.matches(key) {
            return None;
        }
        let State::Submitted(member) = self.state else {
            return None;
        };
        self.state = State::Connected(member);
        Some(member)
    }

    fn submitted<const ID: u8>(&mut self, key: attempt::Id<'_, ID>) -> bool {
        if !self.matches(key) {
            return false;
        }
        let State::Selected(member) = self.state else {
            return false;
        };
        self.state = State::Submitted(member);
        true
    }

    fn is_submitted<const ID: u8>(&self, key: attempt::Id<'_, ID>) -> bool {
        self.matches(key) && matches!(self.state, State::Submitted(_))
    }

    fn defer<const ID: u8>(&mut self, key: attempt::Id<'_, ID>) -> bool {
        if !self.matches(key) {
            return false;
        }
        let State::Selected(member) = self.state else {
            return false;
        };
        self.state = State::Deferred(member);
        true
    }

    fn reset_deferred(&mut self, removed: impl FnOnce(pool::MemberKey) -> bool) -> bool {
        let State::Deferred(member) = self.state else {
            return false;
        };
        if !removed(member) {
            return false;
        }
        self.state = State::Ready;
        true
    }
}

impl<P> Head<P> {
    const fn index(&self) -> Option<u32> {
        match self {
            Self::Empty => None,
            Self::Ready(index) | Self::Deferred { index, .. } => Some(*index),
        }
    }
}

impl<P> Queue<P> {
    const EMPTY: Self = Self {
        head: Head::Empty,
        tail: NONE,
    };

    fn defer<const ID: u8>(&mut self, dial: &mut Dial, key: attempt::Id<'_, ID>, plan: P) -> bool {
        let head = mem::replace(&mut self.head, Head::Empty);
        if matches!(head, Head::Deferred { .. }) || !dial.defer(key) {
            self.head = head;
            return false;
        }
        dial.next = match head.index() {
            Some(index) => index,
            None => NONE,
        };
        self.head = Head::Deferred {
            index: key.index(),
            plan,
        };
        if self.tail == NONE {
            self.tail = key.index();
        }
        true
    }

    fn push_back(&mut self, records: &mut [Dial], key: u32) {
        records[key as usize].next = NONE;
        match self.head {
            Head::Empty => self.head = Head::Ready(key),
            Head::Ready(_) | Head::Deferred { .. } => {
                records[self.tail as usize].next = key;
            }
        }
        self.tail = key;
    }

    fn take_deferred(&mut self, records: &mut [Dial]) -> Option<(usize, P)> {
        let index = self.deferred_front()?;
        let dial = records.get_mut(index)?;
        if !dial.resume() {
            return None;
        }
        let head = mem::replace(&mut self.head, Head::Empty);
        match head {
            Head::Deferred { plan, .. } => {
                self.head = if dial.next == NONE {
                    Head::Empty
                } else {
                    Head::Ready(dial.next)
                };
                dial.next = NONE;
                if matches!(self.head, Head::Empty) {
                    self.tail = NONE;
                }
                Some((index, plan))
            }
            head => {
                self.head = head;
                None
            }
        }
    }

    fn select_front(
        &mut self,
        records: &mut [Dial],
        expected: usize,
        member: pool::MemberKey,
    ) -> bool {
        let Head::Ready(index) = &self.head else {
            return false;
        };
        let index = *index as usize;
        if index != expected {
            return false;
        }
        let dial = &mut records[index];
        if !dial.select(member) {
            return false;
        }
        self.head = if dial.next == NONE {
            Head::Empty
        } else {
            Head::Ready(dial.next)
        };
        dial.next = NONE;
        if matches!(self.head, Head::Empty) {
            self.tail = NONE;
        }
        true
    }

    const fn ready_front(&self) -> Option<usize> {
        match self.head {
            Head::Ready(index) => Some(index as usize),
            Head::Empty | Head::Deferred { .. } => None,
        }
    }

    const fn deferred_front(&self) -> Option<usize> {
        match self.head {
            Head::Deferred { index, .. } => Some(index as usize),
            Head::Empty | Head::Ready(_) => None,
        }
    }

    fn clear_deferred(&mut self) {
        let head = mem::replace(&mut self.head, Head::Empty);
        self.head = match head {
            Head::Deferred { index, .. } => Head::Ready(index),
            head => head,
        };
    }

    fn is_empty(&self) -> bool {
        matches!(self.head, Head::Empty)
    }
}

impl<P> Dials<P> {
    pub(super) fn try_with_capacity(capacity: usize) -> Result<Self, collections::AllocationError> {
        assert!(
            u32::try_from(capacity).is_ok(),
            "service connection capacity overflow"
        );
        let mut records: Box<[Dial]> =
            collections::BoxSliceExt::try_box_with(capacity, |_| Dial::ready())?;
        let mut ready = Queue::EMPTY;
        for index in 0..capacity as u32 {
            ready.push_back(&mut records, index);
        }
        Ok(Self {
            records,
            ready,
            needs_poll: false,
        })
    }

    pub(super) fn capacity(&self) -> usize {
        self.records.len()
    }

    pub(super) fn active<const ID: u8>(&self, key: attempt::Id<'_, ID>) -> Option<pool::MemberKey> {
        self.records.get(key.index() as usize)?.active(key)
    }

    pub(super) fn start<'d, E, const ID: u8>(
        &mut self,
        bind: impl FnOnce() -> Result<(pool::MemberKey, P), E>,
    ) -> Result<Option<Started<'d, P, ID>>, E> {
        if self.ready.deferred_front().is_some() {
            let Some((index, output)) = self.ready.take_deferred(&mut self.records) else {
                return Ok(None);
            };
            let key = attempt::Id::from_generation(index as u32, self.records[index].generation);
            return Ok(Some(Started { key, output }));
        }
        let Some(index) = self.ready.ready_front() else {
            return Ok(None);
        };
        let (member, output) = bind()?;
        if !self.ready.select_front(&mut self.records, index, member) {
            return Ok(None);
        }
        let key = attempt::Id::from_generation(index as u32, self.records[index].generation);
        Ok(Some(Started { key, output }))
    }

    pub(super) fn connected<const ID: u8>(
        &mut self,
        key: attempt::Id<'_, ID>,
    ) -> Option<pool::MemberKey> {
        self.records.get_mut(key.index() as usize)?.connected(key)
    }

    pub(super) fn submitted<const ID: u8>(&mut self, key: attempt::Id<'_, ID>) -> bool {
        self.records
            .get_mut(key.index() as usize)
            .is_some_and(|dial| dial.submitted(key))
    }

    pub(super) fn is_submitted<const ID: u8>(&self, key: attempt::Id<'_, ID>) -> bool {
        self.records
            .get(key.index() as usize)
            .is_some_and(|dial| dial.is_submitted(key))
    }

    pub(super) fn defer<const ID: u8>(
        &mut self,
        key: attempt::Id<'_, ID>,
        plan: P,
        has_endpoints: bool,
    ) -> bool {
        let index = key.index() as usize;
        let Some(dial) = self.records.get_mut(index) else {
            return false;
        };
        if !self.ready.defer(dial, key, plan) {
            return false;
        }
        self.refresh_pending(has_endpoints);
        true
    }

    pub(super) fn recycle<const ID: u8>(
        &mut self,
        key: attempt::Id<'_, ID>,
        has_endpoints: bool,
    ) -> Option<pool::MemberKey> {
        let index = key.index() as usize;
        let dial = self.records.get_mut(index)?;
        let member = dial.take_active(key)?;
        self.finish(index, has_endpoints);
        Some(member)
    }

    pub(super) fn retry<const ID: u8>(
        &mut self,
        key: attempt::Id<'_, ID>,
        has_endpoints: bool,
    ) -> Option<pool::MemberKey> {
        let index = key.index() as usize;
        let dial = self.records.get_mut(index)?;
        let member = dial.take_attempt(key)?;
        self.finish(index, has_endpoints);
        Some(member)
    }

    pub(super) fn kill<const ID: u8>(
        &mut self,
        key: attempt::Id<'_, ID>,
    ) -> Option<pool::MemberKey> {
        let index = key.index() as usize;
        let dial = self.records.get_mut(index)?;
        dial.take_active(key)
    }

    fn finish(&mut self, index: usize, has_endpoints: bool) {
        if self.records[index].requeue() {
            self.ready.push_back(&mut self.records, index as u32);
        }
        self.refresh_pending(has_endpoints);
    }

    pub(super) fn reset_front(&mut self, removed: impl FnOnce(pool::MemberKey) -> bool) {
        let Some(index) = self.ready.deferred_front() else {
            return;
        };
        if self.records[index].reset_deferred(removed) {
            self.ready.clear_deferred();
        }
    }

    pub(super) fn refresh_pending(&mut self, has_endpoints: bool) {
        self.needs_poll = !self.ready.is_empty() && has_endpoints;
    }
}
