//! Bounded reply storage and lane access.

use std::{cell, marker, num::NonZero};

use dope::core::driver::{route::Identity, schedule::app};
use o3::{alloc, base::cell::region, depot::capacity};

use crate::reply::{
    self, Length, Slot, Storage,
    arena::state::{Advance, Retire, Start},
    layout,
};

pub(in crate::reply) mod backing;
pub(in crate::reply) mod control;
pub(in crate::reply) mod lanes;
pub(in crate::reply) mod progress;
pub(in crate::reply) mod retire;
pub(in crate::reply) mod state;

use backing::Backing;
use lanes::{Error, Lane, LaneIndex, Released};

use crate::reply::arena::progress::Work;

/// A fixed-capacity, multi-lane reply arena protected by a runtime region.
/// `'arena` brands backing and handles; `'credits` lends the accounting epoch; `'region` owns mutations.
/// `'region: 'credits: 'arena` keeps credit state alive through the backing's drop path.
pub struct Arena<'arena, 'credits, 'region, D, E = D, K = ()> {
    state:
        region::Value<region::Identity<'region>, state::Book<'arena, 'credits, 'region, D, E, K>>,
    retire: retire::State,
    pub(in crate::reply) backing: &'arena Backing<'credits, 'region, D>,
    pub(in crate::reply) batches: cell::OnceCell<reply::Batches<'arena, 'credits, 'region, D>>,
    lanes: NonZero<u32>,
    next: cell::Cell<Retirement>,
    owner: marker::PhantomData<fn(&'arena ()) -> &'arena ()>,
}

#[derive(Clone, Copy)]
enum Retirement {
    Batch,
    Released,
    Entry,
}

impl Retirement {
    const fn next(self) -> Self {
        match self {
            Self::Batch => Self::Released,
            Self::Released => Self::Entry,
            Self::Entry => Self::Batch,
        }
    }
}

impl<'arena, 'credits, 'region, D, E, K> Arena<'arena, 'credits, 'region, D, E, K> {
    pub(in crate::reply) fn try_new(
        backing: &'arena Backing<'credits, 'region, D>,
        layout: layout::Layout,
        proof: region::Proof<region::Identity<'region>>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            state: proof.bind(state::Book::try_new(layout)?),
            retire: retire::State::try_new(capacity::Count::new(layout.entries().get()))?,
            backing,
            batches: cell::OnceCell::new(),
            lanes: layout.lane_count(),
            next: cell::Cell::new(Retirement::Batch),
            owner: marker::PhantomData,
        })
    }

    pub const fn domain(&self) -> layout::Domain<'arena> {
        layout::Domain::new(self.lanes)
    }

    pub fn lane<ID: Identity>(
        &self,
        position: layout::Position<'arena, 'region, ID>,
    ) -> Lane<'_, 'arena, 'credits, 'region, D, E, K> {
        Lane {
            arena: self,
            index: LaneIndex::from_conn(position.connection().lane()),
        }
    }

    /// Opens the trusted projection boundary for an owner with a lane-domain proof.
    pub const fn raw(&self) -> layout::raw::View<'_, 'arena, 'credits, 'region, D, E, K> {
        layout::raw::View::new(self)
    }

    pub fn try_lane(
        &self,
        index: LaneIndex,
    ) -> Result<Lane<'_, 'arena, 'credits, 'region, D, E, K>, Error> {
        let index = LaneIndex::try_within(index, self.lanes)?;
        Ok(Lane { arena: self, index })
    }

    /// Returns deferred retirement and waiter work. The owning application's
    /// progress callback must retain this state even when every lane is idle.
    pub fn work(&self) -> Work {
        if self.retire.has_active()
            || !self.retire.is_empty()
            || self.backing.resources.has_released()
            || self.backing.resources.has_pending()
            || self.batches.get().is_some_and(reply::Batches::has_pending)
        {
            Work::Pending
        } else {
            Work::Idle
        }
    }

    /// Examines at most one waiter and retires at most one value. Each ready
    /// retirement class progresses within three permits, independent of batches.
    fn advance(
        &self,
        token: &mut region::Token<region::Identity<'region>>,
    ) -> Option<Released<'arena>> {
        self.backing.resources.advance();
        for _ in 0..3 {
            let next = self.next.get();
            self.next.set(next.next());
            match next {
                Retirement::Batch => {
                    if let Some(batches) = self.batches.get()
                        && let reply::Advance::Dropped = batches.advance()
                    {
                        return None;
                    }
                }
                Retirement::Released => {
                    if let Some(released) = self.backing.resources.pop_released() {
                        return Retire::new(token.borrow_mut(&self.state))
                            .retained(released)
                            .map(|lane| Released::new(lane, self.domain()));
                    }
                }
                Retirement::Entry => {
                    if let Some(released) = self.retire_entry(token) {
                        return released;
                    }
                }
            }
        }
        None
    }

    fn retire_entry(
        &self,
        token: &mut region::Token<region::Identity<'region>>,
    ) -> Option<Option<Released<'arena>>> {
        let active = match self.retire.active.take() {
            Some(active) => active,
            None => {
                let retired = self.retire.pop()?;
                match Retire::new(token.borrow_mut(&self.state)).begin(retired) {
                    Start::Drained(lane) => return Some(Some(Released::new(lane, self.domain()))),
                    Start::Draining(active) => active,
                }
            }
        };
        Some(
            match Retire::new(token.borrow_mut(&self.state)).advance(active) {
                Advance::Draining(active) => {
                    self.retire.active.set(Some(active));
                    None
                }
                Advance::Complete(lane) => lane.map(|lane| Released::new(lane, self.domain())),
            },
        )
    }

    /// Performs waiter and retirement work under the connector's application budget.
    /// Returns the exact lane whose entry capacity was physically released.
    pub fn maintain(
        &self,
        work: app::Work<'_, 'region>,
        token: &mut region::Token<region::Identity<'region>>,
    ) -> Option<Released<'arena>> {
        if matches!(self.work(), Work::Idle) {
            return None;
        }
        work.permit()?.run(|| self.advance(token))
    }

    pub fn pick_active(
        &self,
        token: &mut region::Token<region::Identity<'region>>,
    ) -> Option<Lane<'_, 'arena, 'credits, 'region, D, E, K>> {
        let index = token
            .borrow_mut(&self.state)
            .pick_active(&self.backing.resources)?;
        Some(Lane { arena: self, index })
    }

    pub fn inflight(&self) -> Length {
        self.backing.resources.used()
    }

    /// Whether every reply-entry identity is permanently retired.
    pub fn is_exhausted(&self, token: &region::Token<region::Identity<'region>>) -> bool {
        token.borrow(&self.state).slots.is_exhausted()
    }

    pub(super) fn slot<'a, 's: 'a>(
        &'s self,
        token: &'a mut region::Token<region::Identity<'region>>,
        key: state::EntryKey,
    ) -> Option<(
        Slot<'a, 's, 'arena, 'credits, 'region, D, E>,
        &'a mut state::Signal<'region>,
    )> {
        let state = token.borrow_mut(&self.state);
        let entry = state.slots.get_mut(key)?;
        let state::EntryBody::Attached(slot) = &mut entry.body else {
            return None;
        };
        Some((
            Slot {
                state: slot.state(),
                owners: &mut entry.owners,
                owner: marker::PhantomData,
            },
            &mut entry.signal,
        ))
    }
}

impl<'region, D, E, K> Arena<'_, '_, 'region, D, E, K> {
    /// Lends one arena while its original data and credit storage remain owned.
    /// The scope retains the arena itself, so abandoning a loan cannot skip
    /// teardown of its registered entries and detached batches.
    ///
    /// ```compile_fail
    /// use dope_fiber::reply::{Arena, layout};
    /// use o3::{alloc, base::cell::region};
    ///
    /// fn escape<'region>(layout: layout::Layout, proof: region::Proof<region::Identity<'region>>)
    ///     -> Result<&'region Arena<'region, 'region, 'region, ()>, alloc::Error>
    /// {
    ///     Arena::try_with(layout, proof, |arena| arena)
    /// }
    /// ```
    pub fn try_with<R>(
        layout: layout::Layout,
        proof: region::Proof<region::Identity<'region>>,
        run: impl for<'scope, 'arena, 'credits> FnOnce(
            &'scope Arena<'arena, 'credits, 'region, D, E, K>,
        ) -> R,
    ) -> Result<R, alloc::Error> {
        Storage::try_with(layout, proof, |storage| storage.try_arena(run))?
    }

    /// Lends an arena and its matching fixed-capacity batch retirement owner.
    /// The invariant arena brand prevents detaching into another arena's pool.
    /// Allocation completes before lending; the stored owner cannot be replaced.
    pub fn try_batches<R>(
        layout: layout::Layout,
        proof: region::Proof<region::Identity<'region>>,
        run: impl for<'scope, 'arena, 'credits> FnOnce(
            &'scope Arena<'arena, 'credits, 'region, D, E, K>,
            &'scope reply::Batches<'arena, 'credits, 'region, D>,
        ) -> R,
    ) -> Result<R, alloc::Error> {
        Storage::try_with(layout, proof, |storage| storage.try_batches(run))?
    }
}

impl<D, E, K> Drop for Arena<'_, '_, '_, D, E, K> {
    fn drop(&mut self) {
        if let Some(batches) = self.batches.get_mut() {
            while let reply::Advance::Dropped = batches.advance() {}
        }
        self.state.get_mut().clear();
    }
}
