use std::num;

use crate::driver::schedule::{
    ready,
    ready::{Resident, Slots, Staged, state, task},
};

const ACTIVE_SHIFT: u32 = 32;
const PAUSED_SHIFT: u32 = 33;
const PAUSED_MASK: u64 = 0b11 << PAUSED_SHIFT;
const CLOSED_MASK: u64 = 1 << 35;
const DRAINING_MASK: u64 = 1 << 36;

#[derive(Clone, Copy)]
struct Lanes<'a>(Resident<'a, state::Topology>);

#[derive(Clone, Copy)]
pub(in crate::driver::schedule::ready) struct Topology<'a> {
    slots: &'a Slots,
    lanes: Lanes<'a>,
}

impl<'a> Topology<'a> {
    fn new(slots: &'a Slots, root: Resident<'a, state::Topology>) -> Self {
        Self {
            slots,
            lanes: Lanes(root),
        }
    }

    pub(in crate::driver::schedule::ready) fn get(
        slots: &'a Slots,
        key: &ready::TopologyKey<'a>,
    ) -> Self {
        let root = slots.resident(key);
        Self::new(slots, root)
    }

    pub(in crate::driver::schedule::ready) fn at(
        slots: &'a Slots,
        index: ready::RootIndex,
    ) -> Self {
        let root = slots.resident(index);
        Self::new(slots, root)
    }

    pub(in crate::driver::schedule::ready) fn attach(
        self,
        staged: Staged<'a>,
    ) -> ready::TaskKey<'a> {
        self.lanes.attach(staged)
    }

    pub(in crate::driver::schedule::ready) fn detach(
        slots: &'a Slots,
        key: &ready::TaskKey<'a>,
    ) -> Option<ready::ReservedKey<'a>> {
        let index = ready::TaskIndex::new(key);
        let task = slots.resident(index);
        let topology = Self::at(slots, task.bound().record().topology());
        if let Some(queued) = task.state().queued() {
            topology.remove(task, queued);
        }
        let root = topology.lanes.0;
        let state = root.payload().word(2);
        // SAFETY: the sole caller owns this attached key and immediately consumes
        // it after this synchronous transition; no callback can detach it again.
        let count = unsafe { num::NonZero::<u32>::new_unchecked(state as u32) };
        let next = count.get() - 1;
        root.payload()
            .set_word(2, (state & !u64::from(u32::MAX)) | u64::from(next));
        if next == 0 && state & CLOSED_MASK != 0 {
            let index = ready::FreeIndex::from_code(root.index);
            let key = ready::TopologyKey::new(index, root.generation());
            Some(root.unroot(key))
        } else {
            None
        }
    }

    /// Queued tasks retain attachments until unlinking; zero count proves empty lanes.
    /// The shared u16 arena bounds the attachment count.
    pub(in crate::driver::schedule::ready) fn close(
        self,
        key: ready::TopologyKey<'a>,
    ) -> Option<ready::ReservedKey<'a>> {
        if self.lanes.0.payload().word(2) as u16 == 0 {
            Some(self.lanes.0.unroot(key))
        } else {
            self.lanes.close();
            None
        }
    }

    pub(in crate::driver::schedule::ready) fn activate(
        self,
        index: ready::TaskIndex,
    ) -> task::Activation {
        let task = self.slots.resident(index);
        let Some(vacant) = task.state().vacant() else {
            return task::Activation::Coalesced;
        };
        let lane = self.lanes.active();
        self.push(index, task, lane, vacant);
        task::Activation::Queued
    }

    pub(in crate::driver::schedule::ready) fn begin(self) -> Option<state::Lane> {
        if self.lanes.0.payload().word(2) & DRAINING_MASK != 0 {
            return None;
        }
        if let Some(side) = self.lanes.paused()
            && self.lanes.head(side).is_some()
        {
            self.lanes.claim();
            return Some(side);
        }
        let side = self.lanes.active();
        self.lanes.head(side)?;
        self.lanes.set_active(side.flip());
        self.lanes.set_paused(Some(side));
        self.lanes.claim();
        Some(side)
    }

    pub(in crate::driver::schedule::ready) fn finish(self) {
        let state = self.lanes.0.payload().word(2);
        self.lanes.0.payload().set_word(2, state & !DRAINING_MASK);
    }

    pub(in crate::driver::schedule::ready) fn peek(self) -> Option<u32> {
        self.lanes
            .paused()
            .and_then(|side| self.peek_side(side))
            .or_else(|| self.peek_side(self.lanes.active()))
    }

    pub(in crate::driver::schedule::ready) fn peek_side(self, side: state::Lane) -> Option<u32> {
        let index = self.lanes.head(side)?;
        Some(self.slots.resident(index).bound().record().local())
    }

    pub(in crate::driver::schedule::ready) fn pop(self, side: state::Lane) -> Option<u32> {
        let index = self.lanes.head(side)?;
        let task = self.slots.resident(index);
        let local = task.bound().record().local();
        self.remove(task, task.state().bind(side));
        Some(local)
    }

    pub(in crate::driver::schedule::ready) fn is_empty(self) -> bool {
        self.lanes.is_empty()
    }

    fn push(
        self,
        index: ready::TaskIndex,
        task: Resident<'a, state::Task>,
        side: state::Lane,
        vacant: state::Vacant<'a>,
    ) {
        let mut links = self.lanes.links(side);
        task.bound().set_link(ready::Link {
            next: None,
            prev: links.prev,
        });
        if let Some(prev) = links.prev {
            let tail = self.slots.resident(prev);
            let mut link = tail.bound().link();
            link.next = Some(index);
            tail.bound().set_link(link);
        } else {
            links.next = Some(index);
        }
        links.prev = Some(index);
        self.lanes.set_links(side, links);
        vacant.queue(side);
    }

    fn remove(self, task: Resident<'a, state::Task>, queued: state::Queued<'a>) {
        let side = queued.lane();
        let link = task.bound().link();
        let mut root = self.lanes.links(side);
        if let Some(prev) = link.prev {
            let prev = self.slots.resident(prev);
            let mut links = prev.bound().link();
            links.next = link.next;
            prev.bound().set_link(links);
        } else {
            root.next = link.next;
        }
        if let Some(next) = link.next {
            let next = self.slots.resident(next);
            let mut links = next.bound().link();
            links.prev = link.prev;
            next.bound().set_link(links);
        } else {
            root.prev = link.prev;
        }
        self.lanes.set_links(side, root);
        task.bound().clear_link();
        queued.clear();
        if root.next.is_none() && self.lanes.paused() == Some(side) {
            self.lanes.set_paused(None);
        }
    }
}

impl Lanes<'_> {
    fn claim(self) {
        let state = self.0.payload().word(2);
        self.0.payload().set_word(2, state | DRAINING_MASK);
    }

    /// The root and Staged own distinct slots in the Plan's u16-bounded arena,
    /// so the prior count is at most u16::MAX - 2. Count updates retain zero
    /// upper count bits.
    fn attach<'d>(self, staged: Staged<'d>) -> ready::TaskKey<'d> {
        let state = self.0.payload().word(2);
        let next = u32::from(state as u16) + 1;
        self.0
            .payload()
            .set_word(2, (state & !u64::from(u32::MAX)) | u64::from(next));
        staged.attach()
    }

    fn close(self) {
        let state = self.0.payload().word(2);
        self.0.payload().set_word(2, state | CLOSED_MASK);
    }

    fn links(self, side: state::Lane) -> ready::Link {
        ready::Link::from_word(self.0.payload().word(side.index()))
    }

    fn set_links(self, side: state::Lane, links: ready::Link) {
        self.0.payload().set_word(side.index(), links.word());
    }

    fn head(self, side: state::Lane) -> Option<ready::TaskIndex> {
        self.links(side).next
    }

    fn active(self) -> state::Lane {
        let second = (self.0.payload().word(2) >> ACTIVE_SHIFT) & 1 != 0;
        state::Lane::from_bit(second)
    }

    fn set_active(self, side: state::Lane) {
        let state = self.0.payload().word(2);
        let state = state & !(1 << ACTIVE_SHIFT) | (u64::from(side.raw()) << ACTIVE_SHIFT);
        self.0.payload().set_word(2, state);
    }

    fn paused(self) -> Option<state::Lane> {
        let raw = ((self.0.payload().word(2) & PAUSED_MASK) >> PAUSED_SHIFT) as u8;
        (raw & 1 != 0).then_some(state::Lane::from_bit(raw & 2 != 0))
    }

    fn set_paused(self, side: Option<state::Lane>) {
        let raw: u8 = match side {
            None => 0,
            Some(side) => 1 | (side.raw() << 1),
        };
        let state = self.0.payload().word(2) & !PAUSED_MASK;
        self.0
            .payload()
            .set_word(2, state | (u64::from(raw) << PAUSED_SHIFT));
    }

    fn is_empty(self) -> bool {
        self.head(state::Lane::First).is_none() && self.head(state::Lane::Second).is_none()
    }
}
