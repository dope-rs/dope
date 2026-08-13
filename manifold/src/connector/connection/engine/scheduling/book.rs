use std::{cmp, time};

use dope_net::link::pool;
use o3::collections::{self, heap};

use crate::connector::lifecycle;

#[derive(Clone, Copy)]
struct Entry<'d, const ID: u8> {
    at: time::Instant,
    key: pool::Key<'d, ID>,
    kind: lifecycle::TimeoutKind,
}

impl<const ID: u8> PartialEq for Entry<'_, ID> {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.kind == other.kind
    }
}

impl<const ID: u8> Eq for Entry<'_, ID> {}

impl<const ID: u8> PartialOrd for Entry<'_, ID> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<const ID: u8> Ord for Entry<'_, ID> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.at
            .cmp(&other.at)
            .then_with(|| self.kind.cmp(&other.kind))
    }
}

struct Lane<'d, const ID: u8> {
    key: Option<pool::Key<'d, ID>>,
    deadlines: [Option<time::Instant>; lifecycle::TimeoutKind::COUNT],
    inbound_since: Option<time::Instant>,
}

impl<const ID: u8> Lane<'_, ID> {
    const EMPTY: Self = Self {
        key: None,
        deadlines: [None; lifecycle::TimeoutKind::COUNT],
        inbound_since: None,
    };
}

pub(in crate::connector) struct DeadlineBook<'d, const ID: u8> {
    lanes: Box<[Lane<'d, ID>]>,
    earliest: heap::Min<Entry<'d, ID>>,
}

impl<'d, const ID: u8> DeadlineBook<'d, ID> {
    pub(in crate::connector::connection::engine) fn try_new(
        capacity: usize,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            lanes: collections::BoxSliceExt::try_box_with(capacity, |_| Lane::EMPTY)?,
            earliest: heap::Min::try_with_capacity(capacity)?,
        })
    }

    pub(in crate::connector::connection::engine) fn arm_after(
        &mut self,
        key: pool::Key<'d, ID>,
        kind: lifecycle::TimeoutKind,
        now: time::Instant,
        window: time::Duration,
    ) -> bool {
        let Some(at) = now.checked_add(window) else {
            return false;
        };
        self.arm(key, kind, at)
    }

    pub(in crate::connector::connection::engine) fn arm(
        &mut self,
        key: pool::Key<'d, ID>,
        kind: lifecycle::TimeoutKind,
        at: time::Instant,
    ) -> bool {
        let index = key.index();
        let Some(lane) = self.lanes.get_mut(index) else {
            return false;
        };
        if lane.key != Some(key) {
            *lane = Lane::EMPTY;
            lane.key = Some(key);
        }
        lane.deadlines[kind.index()] = Some(at);
        self.refresh(index)
    }

    pub(in crate::connector::connection::engine) fn cancel(
        &mut self,
        key: pool::Key<'d, ID>,
        kind: lifecycle::TimeoutKind,
    ) {
        let index = key.index();
        let Some(lane) = self.lanes.get_mut(index) else {
            return;
        };
        if lane.key != Some(key) {
            return;
        }
        lane.deadlines[kind.index()] = None;
        if kind == lifecycle::TimeoutKind::Inbound {
            lane.inbound_since = None;
        }
        let _ = self.refresh(index);
    }

    pub(in crate::connector::connection::engine) fn arm_inbound(
        &mut self,
        key: pool::Key<'d, ID>,
        now: time::Instant,
        last_recv: Option<time::Instant>,
        window: time::Duration,
    ) -> Option<time::Instant> {
        let index = key.index();
        let lane = self.lanes.get_mut(index)?;
        if lane.key != Some(key) {
            *lane = Lane::EMPTY;
            lane.key = Some(key);
        }
        let since = match lane.inbound_since {
            Some(since) => last_recv.filter(|&recv| recv > since).unwrap_or(since),
            None => now,
        };
        let at = since.checked_add(window)?;
        lane.inbound_since = Some(since);
        lane.deadlines[lifecycle::TimeoutKind::Inbound.index()] = Some(at);
        self.refresh(index).then_some(at)
    }

    pub(in crate::connector::connection::engine) fn clear(&mut self, key: pool::Key<'d, ID>) {
        let index = key.index();
        let Some(lane) = self.lanes.get_mut(index) else {
            return;
        };
        if lane.key != Some(key) {
            return;
        }
        *lane = Lane::EMPTY;
        self.earliest.remove(index);
    }

    pub(in crate::connector::connection::engine) fn earliest(&self) -> Option<time::Instant> {
        self.earliest.peek().map(|(_, entry)| entry.at)
    }

    pub(in crate::connector::connection::engine) fn pop_expired(
        &mut self,
        now: time::Instant,
    ) -> Option<(pool::Key<'d, ID>, lifecycle::TimeoutKind)> {
        let (_, entry) = self.earliest.pop_if(|entry| entry.at <= now)?;
        let index = entry.key.index();
        let lane = self.lanes.get_mut(index)?;
        if lane.key != Some(entry.key) || lane.deadlines[entry.kind.index()] != Some(entry.at) {
            let _ = self.refresh(index);
            return None;
        }
        lane.deadlines[entry.kind.index()] = None;
        let _ = self.refresh(index);
        Some((entry.key, entry.kind))
    }

    fn refresh(&mut self, index: usize) -> bool {
        self.earliest.remove(index);
        let Some(lane) = self.lanes.get(index) else {
            return false;
        };
        let Some(key) = lane.key else {
            return true;
        };
        let next = lifecycle::TimeoutKind::ALL
            .into_iter()
            .zip(lane.deadlines.iter().copied())
            .filter_map(|(kind, at)| at.map(|at| (at, kind)))
            .min_by_key(|&(at, kind)| (at, kind));
        let Some((at, kind)) = next else {
            return true;
        };
        self.earliest.insert(index, Entry { at, key, kind }).is_ok()
    }
}
