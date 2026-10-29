use std::{
    array, marker,
    mem::{align_of, size_of},
    num,
};

use dope_core::driver::{
    self,
    route::{Identity, KeyTag},
    schedule::app,
};
use o3::{alloc, base::cell::region, mem, mem::quota};

use crate::link::{
    egress::{
        self, create,
        load::credit,
        metadata::{self, arena, lane, pool::Pool},
        reserve::demand,
    },
    pool,
};

pub struct Arena<'p, 'd, T, S = (), M = metadata::Local, O = (), D: demand::Mode = demand::Fallible>
{
    pub(in crate::link::egress) pool: Pool<'p, 'd, T, M, D>,
    slots: Box<[S]>,
    lanes: metadata::LaneCount,
    owner: marker::PhantomData<O>,
}

pub struct Slot<'a, 'p, 'd, T, S, M = metadata::Local, D: demand::Mode = demand::Fallible> {
    pool: &'a Pool<'p, 'd, T, M, D>,
    state: &'a S,
    lane: metadata::Lane,
}

pub(in crate::link::egress) struct MutableSlot<'a, 'p, 'd, T, S, M, D: demand::Mode> {
    pool: &'a Pool<'p, 'd, T, M, D>,
    state: &'a mut S,
    lane: metadata::Lane,
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[must_use]
#[repr(u8)]
pub enum Progress {
    Done,
    Retry,
}

/// Failure to construct metadata storage for one exact lane geometry.
#[derive(Debug)]
pub enum CreateError {
    Lanes(metadata::Count),
    Allocation(alloc::Error),
}

impl<'p, 'd, T, S> Arena<'p, 'd, T, S, metadata::Local, (), demand::Fallible> {
    /// Borrows one exclusive accounting epoch from the original store.
    /// A usable detached front keeps that epoch alive.
    pub fn try_new(
        token: &region::Token<region::Identity<'d>>,
        credits: &'p mut Store<'d>,
    ) -> Result<Self, alloc::Error>
    where
        S: Default,
    {
        credits.reopen();
        let slots = alloc::BoxSliceExt::try_default(credits.count.count().get())?;
        let pool = Pool::try_new(token, credits)?;
        Ok(Self {
            pool,
            slots,
            lanes: credits.count,
            owner: marker::PhantomData,
        })
    }
}

impl<'p, 'd, T, S, O, D: demand::Mode> Arena<'p, 'd, T, S, credit::Ready<'p, 'd>, O, D> {
    /// Uses the fresh store initialized by retained egress storage construction.
    pub(in crate::link::egress) fn try_driver(
        token: &region::Token<region::Identity<'d>>,
        mode: D,
        credits: &'p mut Store<'d>,
        owner: driver::Reference<'d>,
    ) -> Result<Self, create::Error>
    where
        S: Default,
    {
        let slots = alloc::BoxSliceExt::try_default(credits.count.count().get())
            .map_err(create::Error::Allocation)?;
        let pool = Pool::try_driver(token, mode, credits, owner)?;
        Ok(Self {
            pool,
            slots,
            lanes: credits.count,
            owner: marker::PhantomData,
        })
    }
}

impl<'p, 'd, T, S, M, O, D: demand::Mode> Arena<'p, 'd, T, S, M, O, D> {
    pub const fn len(&self) -> metadata::LaneCount {
        self.lanes
    }

    /// Recovers the exact retained-credit policy already encoded by this
    /// arena, avoiding a duplicate per-port configuration sidecar.
    pub fn config(&self) -> egress::Config {
        self.pool.config()
    }

    pub fn get(&self, lane: metadata::Lane) -> Option<Slot<'_, 'p, 'd, T, S, M, D>> {
        let state = View::new(&self.slots).get(lane)?;
        Some(Slot {
            pool: &self.pool,
            state,
            lane,
        })
    }
}

impl<'p, 'd, T, S, M, D: demand::Mode, ID: Identity> Arena<'p, 'd, T, S, M, ID, D> {
    pub(in crate::link::egress) fn bind(
        &self,
        key: pool::Key<'d, ID>,
    ) -> Slot<'_, 'p, 'd, T, S, M, D> {
        Slot {
            pool: &self.pool,
            state: View::new(&self.slots).bind(key),
            lane: metadata::Lane::from_slot(key.lane()),
        }
    }

    pub(in crate::link::egress) fn bind_mut(
        &mut self,
        key: pool::Key<'d, ID>,
    ) -> MutableSlot<'_, 'p, 'd, T, S, M, D> {
        let lane = metadata::Lane::from_slot(key.lane());
        let state = Slots::new(&mut self.slots).bind(key);
        MutableSlot {
            pool: &self.pool,
            state,
            lane,
        }
    }
}

impl<'p, T, S, M, D: demand::Mode> Clone for Slot<'_, 'p, '_, T, S, M, D> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'p, T, S, M, D: demand::Mode> Copy for Slot<'_, 'p, '_, T, S, M, D> {}

impl<'a, 'p, 'd, T, S, M, D: demand::Mode> Slot<'a, 'p, 'd, T, S, M, D> {
    pub fn state(self) -> &'a S {
        self.state
    }

    pub fn queue(self) -> metadata::Queue<'a, 'p, 'd, T, M, D> {
        metadata::Queue {
            pool: self.pool,
            lane: self.lane,
        }
    }
}

impl<'a, 'p, 'd, T, S> Slot<'a, 'p, 'd, T, S, metadata::Local, demand::Fallible> {
    pub fn clear_step<'turn>(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        work: app::Work<'turn, 'd>,
        cancel: impl FnOnce(T),
    ) -> Progress {
        let queue = self.queue();
        match work.run(|| {
            let front = queue.take_front(token)?;
            Some(front.into_inner())
        }) {
            quota::Admission::Item(value) => cancel(value),
            quota::Admission::Empty => return Progress::Done,
            quota::Admission::Exhausted => return Progress::Retry,
        }
        if queue.is_empty() {
            Progress::Done
        } else {
            Progress::Retry
        }
    }
}

impl<'a, 'p, 'd, T, S, M, D: demand::Mode> MutableSlot<'a, 'p, 'd, T, S, M, D> {
    pub(in crate::link::egress) fn into_parts(
        self,
    ) -> (&'a mut S, metadata::Queue<'a, 'p, 'd, T, M, D>) {
        (
            self.state,
            metadata::Queue {
                pool: self.pool,
                lane: self.lane,
            },
        )
    }
}

const _: () =
    assert!(size_of::<Slot<'static, 'static, 'static, (), ()>>() == 3 * size_of::<usize>());
const _: () = assert!(size_of::<Progress>() == 1);
const _: () = {
    assert!(
        size_of::<Arena<'static, 'static, (), (), metadata::Local>>()
            == size_of::<Arena<'static, 'static, (), (), metadata::Local, KeyTag<0>>>()
    );
    assert!(
        align_of::<Arena<'static, 'static, (), (), metadata::Local>>()
            == align_of::<Arena<'static, 'static, (), (), metadata::Local, KeyTag<0>>>()
    );
};

pub struct Store<'d> {
    pub(in crate::link::egress) shared: mem::raw::Pool<2, credit::Tag, u32>,
    pub(in crate::link::egress) config: egress::Config,
    pub(in crate::link::egress) count: metadata::LaneCount,
    pub(in crate::link::egress) lanes: Box<[lane::Lane<'d>]>,
}

impl<'d> Store<'d> {
    /// Starts one accounting epoch after the previous arena loan has ended.
    /// Forgotten affine handles cannot retain usable access across this
    /// exclusive borrow; their abandoned accounting does not enter the new epoch.
    fn reopen(&mut self) {
        use mem::raw::Source as _;

        for lane in &mut self.lanes {
            // SAFETY: these are the exact states minted with this shared pool;
            // the exclusive Store borrow excludes every usable prior arena loan.
            // All bound states and their shared surplus are then rebuilt together.
            let reserve = unsafe { self.shared.lane(&lane.credit) }.reserve();
            *lane = lane::Lane::new(mem::raw::State::new(reserve));
        }
        self.shared = mem::raw::Pool::new(self.config.shared_limit());
    }

    pub fn try_new(
        config: egress::Config,
        lanes: num::NonZero<usize>,
    ) -> Result<Self, arena::CreateError> {
        use arena::CreateError;
        let lanes = metadata::LaneCount::try_new(lanes).map_err(CreateError::Lanes)?;
        Self::try_lanes(config, lanes).map_err(CreateError::Allocation)
    }

    /// Distributes the fixed protected capacity exactly; each lane's reserve
    /// fits beside the shared portion and the remainder goes to the first lanes.
    pub fn try_lanes(
        config: egress::Config,
        lanes: metadata::LaneCount,
    ) -> Result<Self, alloc::Error> {
        let reserved = config.reserved();
        let reserved = [reserved.entries().raw(), reserved.resident().raw()];
        let count = lanes.position().get();
        let reserve = reserved.map(|amount| amount / count);
        let extra = reserved.map(|amount| amount % count);
        let states = alloc::BoxSliceExt::try_from_fn(count as usize, |index| {
            let reserve =
                array::from_fn(|axis| reserve[axis] + u32::from((index as u32) < extra[axis]));
            lane::Lane::new(mem::raw::State::new(reserve))
        })?;
        Ok(Self {
            shared: mem::raw::Pool::new(config.shared_limit()),
            config,
            count: lanes,
            lanes: states,
        })
    }
}

#[repr(transparent)]
pub(super) struct Slots<'a, S>(&'a mut [S]);

#[repr(transparent)]
pub(super) struct View<'a, S>(&'a [S]);

impl<'a, S> Slots<'a, S> {
    const fn new(slots: &'a mut [S]) -> Self {
        Self(slots)
    }

    fn bind<ID: Identity>(self, key: pool::Key<'_, ID>) -> &'a mut S {
        // SAFETY: the tagged arena and route slab are allocated together from
        // one connection capacity. `Key` is minted only by a successful slab
        // reservation and retains the affine route-owner brand.
        unsafe { self.0.get_unchecked_mut(key.index().into_usize()) }
    }
}

impl<'a, S> View<'a, S> {
    const fn new(slots: &'a [S]) -> Self {
        Self(slots)
    }

    fn get(self, lane: metadata::Lane) -> Option<&'a S> {
        self.0.get(lane.index().into_usize())
    }

    fn bind<ID: Identity>(self, key: pool::Key<'_, ID>) -> &'a S {
        // SAFETY: the tagged arena and route slab are allocated together from
        // one connection capacity. `Key` is minted only by a successful slab
        // reservation and retains the affine route-owner brand.
        unsafe { self.0.get_unchecked(key.index().into_usize()) }
    }
}
