use dope_core::driver::{
    self,
    life::finish,
    retained::{
        accept::{Entry, Lane, Readiness, Slots},
        arm,
    },
    route::{self, Identity, indices, kind::Accept},
    schedule,
};
use o3::{
    alloc,
    alloc::BoxSliceExt as _,
    base::{cell::region, num::bounded},
    depot::arena::slab::external::Generation as _,
};

#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub(super) struct LaneId(route::Epoch);

impl LaneId {
    const FIRST: Self = Self(route::Epoch::INITIAL);

    fn next(self) -> Option<Self> {
        self.0.next().and_then(Self::from_epoch)
    }

    const fn from_epoch(epoch: route::Epoch) -> Option<Self> {
        if epoch.get().get() - 1 > route::SLOT_MASK {
            None
        } else {
            Some(Self(epoch))
        }
    }

    const fn index(self) -> bounded::U32<0, { route::SLOT_MASK as u32 }> {
        bounded::U32::clamp((self.0.get().get() - 1) as usize)
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
struct Target(u16);

impl Target {
    const ZERO: Self = Self(0);

    fn bounded(want: usize, limit: u16) -> Self {
        Self(want.min(limit as usize) as u16)
    }
}

pub(super) struct Lanes<'d, ID: Identity> {
    pub(super) entries: Slots<'d, <ID as route::Materialize<Accept>>::With>,
    limit: u16,
    target: Target,
    pub(super) cursor: LaneId,
    pub(super) accepting: bool,
}

pub(super) struct Turn<'a, 'd, ID: Identity> {
    pub(super) ordinal: LaneId,
    pub(super) lane: Entry<'a, 'd, <ID as route::Materialize<Accept>>::With>,
    pub(super) cursor: &'a mut LaneId,
}

impl<'d, ID: Identity> Lanes<'d, ID> {
    pub(super) fn demand(&mut self, want: usize) -> usize {
        self.target = Target::bounded(want, self.limit);
        usize::from(self.target.0).saturating_sub(self.entries.active())
    }

    pub(super) fn resolve(&mut self, epoch: route::Epoch) -> Option<Turn<'_, 'd, ID>> {
        let index = usize::try_from(epoch.get().get() - 1).ok()?;
        let lane = self.entries.get_mut(index)?;
        Some(Turn {
            ordinal: LaneId(epoch),
            lane,
            cursor: &mut self.cursor,
        })
    }

    pub(super) fn next(&mut self) -> Option<Turn<'_, 'd, ID>> {
        let ordinal = self.cursor;
        let cursor = match ordinal.next() {
            Some(next) if self.contains(next) => next,
            Some(_) | None => LaneId::FIRST,
        };
        self.cursor = cursor;
        let lane = self.entries.get_mut(ordinal.index().into_usize())?;
        Some(Turn {
            ordinal,
            lane,
            cursor: &mut self.cursor,
        })
    }

    fn contains(&self, ordinal: LaneId) -> bool {
        ordinal.index().into_usize() < self.entries.len()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Interest {
    Ready,
    Full,
    Stopped,
}

const _: () = assert!(std::mem::size_of::<Interest>() == std::mem::size_of::<bool>());

pub(super) enum Mode<'d, ID: Identity> {
    Multishot {
        arm: arm::State<'d, <ID as route::Materialize<Accept>>::With>,
        interest: Interest,
    },
    Oneshot(Lanes<'d, ID>),
}

impl<'d, ID: Identity> Mode<'d, ID> {
    pub(super) fn multishot(
        routes: route::Space<'d, <ID as route::Materialize<Accept>>::With>,
        edge: indices::Edge,
    ) -> Self {
        Self::Multishot {
            arm: arm::State::new(routes.bind(edge.slot(), route::Epoch::INITIAL)),
            interest: Interest::Ready,
        }
    }

    pub(super) fn try_oneshot(
        routes: route::Space<'d, <ID as route::Materialize<Accept>>::With>,
        edge: indices::Edge,
        cap: u16,
    ) -> Result<Self, alloc::Error> {
        let len = usize::from(cap);
        let mut cursor = LaneId::FIRST;
        let lanes = Box::try_init(len, || {
            let lane = cursor;
            if let Some(next) = lane.next() {
                cursor = next;
            }
            Lane::new(routes.bind(edge.slot(), lane.0))
        })?;
        Ok(Self::Oneshot(Lanes {
            entries: Slots::new(lanes),
            limit: cap,
            target: Target(cap),
            cursor: LaneId::FIRST,
            accepting: true,
        }))
    }

    pub(super) fn progress<'a>(
        &self,
        driver: driver::Reference<'d>,
        region: &region::Token<region::Identity<'a>>,
    ) -> schedule::Progress<'a> {
        match self {
            Self::Multishot { arm, interest } => {
                if arm.is_live() {
                    return arm.progress(region);
                }
                if *interest != Interest::Ready {
                    return schedule::Progress::Quiescent;
                }
            }
            Self::Oneshot(lanes) => {
                if !lanes.accepting {
                    return lanes.entries.progress(region);
                }
                if lanes.entries.active() >= usize::from(lanes.target.0) {
                    return if lanes.entries.active() != 0 {
                        schedule::Progress::waiting(region)
                    } else {
                        schedule::Progress::Quiescent
                    };
                }
            }
        }
        Readiness::from(driver).progress(region)
    }

    pub(super) fn is_stopped(&self) -> bool {
        match self {
            Self::Multishot { interest, .. } => *interest == Interest::Stopped,
            Self::Oneshot(lanes) => !lanes.accepting,
        }
    }

    pub(super) fn in_flight(&self) -> usize {
        match self {
            Self::Multishot { arm, .. } => usize::from(arm.is_live()),
            Self::Oneshot(lanes) => lanes.entries.active(),
        }
    }

    pub(super) fn stop(&mut self, driver: &mut driver::Context<'_, 'd>) {
        match self {
            Self::Multishot { arm, interest } => {
                *interest = Interest::Stopped;
                arm.stop(driver);
            }
            Self::Oneshot(lanes) => {
                lanes.accepting = false;
                lanes.target = Target::ZERO;
                lanes.entries.stop(driver);
            }
        }
    }

    pub(super) fn finish(&mut self, proof: &mut finish::Finish<'_, 'd>) {
        match self {
            Self::Multishot { arm, interest } => {
                arm.finish(proof);
                *interest = Interest::Stopped;
            }
            Self::Oneshot(lanes) => {
                lanes.entries.finish(proof);
                lanes.target = Target::ZERO;
                lanes.accepting = false;
            }
        }
    }
}

const _: () = {
    assert!(route::SLOT_MASK >= u16::MAX as u64);
    assert!(std::mem::size_of::<LaneId>() == std::mem::size_of::<route::Epoch>());
    assert!(std::mem::size_of::<Option<LaneId>>() == std::mem::size_of::<route::Epoch>());
};
const _: () = assert!(std::mem::size_of::<Target>() == std::mem::size_of::<u16>());
