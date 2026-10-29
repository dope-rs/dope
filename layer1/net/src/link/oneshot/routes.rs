use core::cmp;

use dope_core::driver::{
    self,
    route::{
        self, Identity, KeyTag, indices, kind,
        table::{
            self,
            bounds::{self, Offset, Segment},
        },
    },
    schedule::clock,
};
use o3::{base::marker::ThreadBound, depot};

use crate::link::oneshot::Cancellation;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub(super) struct Index {
    projected: bounds::Index,
    invariant: ThreadBound,
}

pub(super) struct Routes<'d, ID: Identity> {
    segment: Segment,
    space: route::Space<'d, ID>,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(super) struct Local<'d, ID: Identity> {
    epoch: route::Epoch,
    index: Index,
    space: route::Space<'d, ID>,
}

pub(super) enum Due<'d> {
    Connect {
        epoch: route::Epoch,
        at: clock::Deadline<'d>,
    },
    Send {
        epoch: route::Epoch,
        at: clock::Deadline<'d>,
    },
}

impl<'d> Due<'d> {
    pub(super) const fn epoch(&self) -> route::Epoch {
        match *self {
            Self::Connect { epoch, .. } | Self::Send { epoch, .. } => epoch,
        }
    }

    pub(super) const fn at(&self) -> clock::Deadline<'d> {
        match *self {
            Self::Connect { at, .. } | Self::Send { at, .. } => at,
        }
    }

    pub(super) fn timeout(self, now: clock::Moment<'d>) -> Cancellation {
        match self {
            Self::Connect { at, .. } => Cancellation::ConnectTimeout {
                late_by: now.past_due(at),
            },
            Self::Send { at, .. } => Cancellation::SendTimeout {
                late_by: now.past_due(at),
            },
        }
    }
}

impl PartialEq for Due<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.at() == other.at()
    }
}

impl Eq for Due<'_> {}

impl PartialOrd for Due<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Due<'_> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.at().cmp(&other.at())
    }
}

impl Index {
    const fn new(inner: bounds::Index) -> Self {
        Self {
            projected: inner,
            invariant: ThreadBound::NEW,
        }
    }

    pub(super) const fn local(self) -> indices::SlotIndex {
        self.projected.local()
    }

    const fn physical(self) -> indices::SlotIndex {
        self.projected.physical()
    }
}

// SAFETY: the immutable projection preserves the complete identity and local position.
unsafe impl depot::raw::Address for Index {
    fn index(self) -> usize {
        self.local().index().get() as usize
    }
}

impl<'d, ID: Identity> Routes<'d, ID> {
    pub(super) fn try_new(
        base: indices::Edge,
        cap: table::Capacity,
        driver: &driver::Context<'_, 'd>,
    ) -> crate::Result<Self> {
        let segment = Segment::try_new(base, cap)
            .map_err(crate::ConfigError::RouteSegment)
            .map_err(crate::Resource::Config)?;
        Ok(Self {
            segment,
            space: driver.driver_ref().targets(),
        })
    }

    pub(super) fn try_index(&self, local: indices::SlotIndex) -> Result<Index, bounds::Excluded> {
        Ok(Index::new(self.segment.try_index(local)?))
    }

    pub(super) fn indices(&self) -> impl Iterator<Item = Index> {
        self.segment.indices().map(Index::new)
    }

    pub(super) const fn bind(&self, index: Index, epoch: route::Epoch) -> Local<'d, ID> {
        Local {
            epoch,
            index,
            space: self.space,
        }
    }

    pub(super) fn parse<C: kind::Class>(
        &self,
        target: route::Erased<'d, C>,
    ) -> Result<Local<'d, ID>, route::Erased<'d, C>> {
        let op = match self.space.parse(target) {
            Ok(op) => op,
            Err(mismatch) => return Err(mismatch.into_target()),
        };
        let parts = op.parts();
        let index = match self.segment.offset(parts.slot()) {
            Ok(index) => Index::new(index),
            Err(Offset::Before { .. } | Offset::Capacity(_)) => return Err(target),
        };
        Ok(self.bind(index, parts.epoch()))
    }
}

impl<'d, ID: Identity> Local<'d, ID> {
    pub(super) const fn parts(self) -> table::Parts<ID> {
        table::Parts::new(self.index.local(), self.epoch)
    }

    pub(super) const fn physical(self) -> route::Target<'d, ID> {
        self.space.bind(self.index.physical(), self.epoch)
    }

    pub(super) const fn index(self) -> Index {
        self.index
    }
}

impl<ID: Identity> PartialEq for Local<'_, ID> {
    fn eq(&self, other: &Self) -> bool {
        self.epoch == other.epoch && self.index == other.index
    }
}

impl<ID: Identity> Eq for Local<'_, ID> {}

const _: () = {
    assert!(core::mem::size_of::<Due<'static>>() == 32);
    assert!(core::mem::size_of::<Index>() == 2 * core::mem::size_of::<u32>());
    assert!(core::mem::size_of::<Option<Index>>() == core::mem::size_of::<Index>());
    assert!(
        core::mem::size_of::<Local<'_, KeyTag<1>>>()
            == core::mem::size_of::<route::Target<'_, KeyTag<1>>>()
    );
    assert!(
        core::mem::align_of::<Local<'_, KeyTag<1>>>()
            == core::mem::align_of::<route::Target<'_, KeyTag<1>>>()
    );
};
