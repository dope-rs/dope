use std::mem;

use dope_core::driver::schedule;
use o3::{cell::region, collections};

use crate::link::egress::{
    self,
    metadata::{self, pool},
};

pub struct Arena<'d, T, S = ()> {
    pool: pool::Pool<'d, T>,
    slots: Box<[Owned<'d, S>]>,
}

struct Owned<'d, S> {
    lane: metadata::Lane<'d>,
    state: S,
}

pub struct Slot<'a, 'd, T, S> {
    pool: &'a pool::Pool<'d, T>,
    owned: &'a Owned<'d, S>,
}

const _: () =
    assert!(mem::size_of::<Slot<'static, 'static, (), ()>>() == 2 * mem::size_of::<usize>());

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
#[repr(u8)]
pub enum Progress {
    Done,
    Retry,
}

const _: () = assert!(mem::size_of::<Progress>() == 1);

impl<'d, T, S> Arena<'d, T, S> {
    pub fn try_with_slots(
        token: &region::Token<'d>,
        config: egress::Config,
        entries: usize,
        mut state: impl FnMut() -> S,
    ) -> Result<Self, collections::AllocationError> {
        let pool = pool::Pool::try_with_config(token, config, entries)?;
        let slots = collections::BoxSliceExt::try_box_with(entries, |lane| Owned {
            lane: metadata::Lane::new(pool.credit_state(lane)),
            state: state(),
        })?;
        Ok(Self { pool, slots })
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<Slot<'_, 'd, T, S>> {
        Some(Slot {
            pool: &self.pool,
            owned: self.slots.get(index)?,
        })
    }
}

impl<T, S> Clone for Slot<'_, '_, T, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T, S> Copy for Slot<'_, '_, T, S> {}

impl<'a, 'd, T, S> Slot<'a, 'd, T, S> {
    pub fn state(self) -> &'a S {
        &self.owned.state
    }

    pub fn queue(self) -> metadata::Queue<'a, 'd, T> {
        metadata::Queue {
            pool: self.pool,
            state: &self.owned.lane,
            credit: self.pool.credit(&self.owned.lane.credit),
        }
    }

    pub fn clear_step<'turn>(
        self,
        token: &mut region::Token<'d>,
        work: schedule::Application<'turn, 'd>,
    ) -> Progress {
        let queue = self.queue();
        if queue.is_empty() {
            return Progress::Done;
        }
        if !work.take() {
            return Progress::Retry;
        }
        queue.clear_one(token);
        if queue.is_empty() {
            Progress::Done
        } else {
            Progress::Retry
        }
    }
}
