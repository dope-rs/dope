use std::{cell, mem};

use o3::depot::arena::linked::fifo::ends;

use crate::reply::{
    LaneIndex,
    arena::state::Index,
    retain::{pool, pool::state::Entry},
};

pub(super) struct Queue(ends::Cell<pool::raw::Slot<Index>>);

#[derive(Clone, Copy)]
enum LifeState {
    Vacant { dormant: LaneIndex },
    Active { lane: LaneIndex },
    Retired { lane: LaneIndex },
    Queued { lane: LaneIndex },
}

#[repr(transparent)]
pub(super) struct Life(cell::Cell<LifeState>);

pub(super) struct Ready<'a, 'credits> {
    slot: &'a Entry<'credits>,
    lane: LaneIndex,
}

struct Links<'a, 'credits> {
    chain: &'a ends::Cell<pool::raw::Slot<Index>>,
    states: &'a [Entry<'credits>],
}

// SAFETY: every view binds the released chain to the Entry tail owned by the
// same Pool; every stored Index was minted from an entry in that exact tail.
unsafe impl<'credits> pool::raw::Domain<Index> for Links<'_, 'credits> {
    fn chain(&self) -> &ends::Cell<pool::raw::Slot<Index>> {
        self.chain
    }

    fn link(&self, index: Index) -> &pool::raw::Link<Index> {
        // SAFETY: Domain admits only indices minted for this exact Entry tail.
        &unsafe { self.states.get_unchecked(index.get()) }.released
    }
}

impl Queue {
    pub(in crate::reply::retain::pool) const fn new() -> Self {
        Self(ends::Cell::empty())
    }

    pub(in crate::reply::retain::pool) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Consumes a retired slot; its prior queue membership ended before retirement.
    pub(in crate::reply::retain::pool) fn push<'credits>(
        &self,
        states: &[Entry<'credits>],
        ready: Ready<'_, 'credits>,
    ) -> LaneIndex {
        let lane = ready.lane;
        ready.slot.life.0.set(LifeState::Queued { lane });
        pool::raw::Domain::push_back(
            &Links {
                chain: &self.0,
                states,
            },
            ready.slot.index(),
        );
        lane
    }

    pub(in crate::reply::retain::pool) fn pop<'a, 'credits>(
        &self,
        states: &'a [Entry<'credits>],
    ) -> Option<&'a Entry<'credits>> {
        let index = pool::raw::Domain::pop_front(&Links {
            chain: &self.0,
            states,
        })?;
        // SAFETY: the popped Index was stored through the same Entry-tail view.
        Some(unsafe { states.get_unchecked(index.get()) })
    }
}

impl Life {
    pub(in crate::reply::retain::pool) const fn new() -> Self {
        Self(cell::Cell::new(LifeState::Vacant {
            dormant: LaneIndex::ZERO,
        }))
    }

    pub(in crate::reply::retain::pool) fn activate(&self, lane: LaneIndex) {
        if let LifeState::Vacant { .. } = self.0.get() {
            self.0.set(LifeState::Active { lane });
        }
    }

    pub(in crate::reply::retain::pool) fn retire(&self, retained: bool) {
        match self.0.get() {
            LifeState::Vacant { .. } | LifeState::Queued { .. } => {}
            LifeState::Active { lane } | LifeState::Retired { lane } => {
                self.0.set(if retained {
                    LifeState::Retired { lane }
                } else {
                    LifeState::Vacant { dormant: lane }
                });
            }
        }
    }

    pub(in crate::reply::retain::pool) fn is_vacant(&self) -> bool {
        matches!(self.0.get(), LifeState::Vacant { .. })
    }

    pub(in crate::reply::retain::pool) fn settle(&self) {
        let dormant = self.lane();
        self.0.set(LifeState::Vacant { dormant });
    }

    pub(in crate::reply::retain::pool) fn lane(&self) -> LaneIndex {
        match self.0.get() {
            LifeState::Vacant { dormant: lane }
            | LifeState::Active { lane }
            | LifeState::Retired { lane }
            | LifeState::Queued { lane } => lane,
        }
    }
}

impl<'a, 'credits> Ready<'a, 'credits> {
    pub(in crate::reply::retain::pool) fn new(slot: &'a Entry<'credits>) -> Option<Self> {
        match slot.life.0.get() {
            LifeState::Retired { lane } => Some(Self { slot, lane }),
            LifeState::Vacant { .. } | LifeState::Active { .. } | LifeState::Queued { .. } => None,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Queue>() == 2 * mem::size_of::<u32>());
    assert!(mem::size_of::<Life>() == 2 * mem::size_of::<u32>());
    assert!(mem::size_of::<Ready<'static, 'static>>() == 2 * mem::size_of::<usize>());
};
