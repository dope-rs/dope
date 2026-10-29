use std::{cell, marker, num};

use o3::mem::Lease;

use crate::driver::schedule::{
    app, ready,
    ready::{state, task},
};

pub mod raw;

/// Borrowed edge to one driver-owned ready topology.
#[must_use = "an edge must be installed or explicitly dropped"]
pub struct Edge<'queue, 'd, I: raw::Index> {
    endpoint: &'queue Endpoint<'d, I>,
    local: I,
}

impl<'queue, 'd, I: raw::Index> Edge<'queue, 'd, I> {
    pub(super) const fn new(endpoint: &'queue Endpoint<'d, I>, local: I) -> Self {
        Self { endpoint, local }
    }

    #[doc(hidden)]
    pub const fn local(&self) -> I {
        self.local
    }

    pub(in crate::driver::schedule::ready::task) fn into_parts(
        self,
    ) -> (&'queue Endpoint<'d, I>, I) {
        (self.endpoint, self.local)
    }
}

/// Driver-owned stable task-ready topology.
pub struct Endpoint<'d, I: raw::Index> {
    owner: Lease<ready::TopologyKey<'d>>,
    local: marker::PhantomData<fn(I) -> I>,
}

impl<'d, I: raw::Index> Endpoint<'d, I> {
    pub fn try_new(parent: task::Parent<'d>) -> Result<Self, task::Error> {
        let driver = parent.driver();
        let entries = &driver.ready().arena().entries;
        let key = match entries.pool.try_topology(&entries.slots) {
            Ok(key) => key,
            Err(error) => {
                return Err(task::Error::Capacity {
                    requested: num::NonZero::<usize>::MIN,
                    available: error.available(),
                });
            }
        };
        Ok(Self {
            owner: Lease::new(key, driver),
            local: marker::PhantomData,
        })
    }

    pub fn attach(&self, local: I) -> Edge<'_, 'd, I> {
        Edge::new(self, local)
    }

    pub fn drain(&self) -> Option<Drain<'_, 'd, I>> {
        let side = self.topology().begin()?;
        Some(Drain {
            endpoint: self,
            side,
        })
    }

    pub fn peek(&self) -> Option<I> {
        self.topology().peek().map(raw::restore)
    }

    pub fn pop(&self) -> Option<I> {
        self.drain()?.pop()
    }

    pub fn is_empty(&self) -> bool {
        self.topology().is_empty()
    }

    pub(in crate::driver::schedule::ready::task) fn key(&self) -> &ready::TopologyKey<'d> {
        self.owner.sink()
    }

    pub(super) fn topology(&self) -> ready::Topology<'_> {
        let arena = self.owner.item().ready().arena();
        ready::Topology::get(&arena.entries.slots, self.owner.sink())
    }
}

/// Exclusive drain of one stable lane. Task membership is resolved and consumed
/// within each operation, so cancelling a task cannot invalidate a retained head.
pub struct Drain<'queue, 'd, I: raw::Index> {
    endpoint: &'queue Endpoint<'d, I>,
    side: state::Lane,
}

impl<I: raw::Index> Drain<'_, '_, I> {
    pub fn peek(&self) -> Option<I> {
        self.endpoint
            .topology()
            .peek_side(self.side)
            .map(raw::restore)
    }

    pub fn pop(&mut self) -> Option<I> {
        self.endpoint.topology().pop(self.side).map(raw::restore)
    }
}

impl<I: raw::Index> app::Drain for Drain<'_, '_, I> {
    type Candidate<'a>
        = I
    where
        Self: 'a;
    type Item<'a>
        = I
    where
        Self: 'a;

    fn next_when<P>(
        &mut self,
        take: impl FnOnce() -> Option<P>,
    ) -> app::Selection<Self::Item<'_>, P, Self::Candidate<'_>> {
        let item = self.peek()?;
        let Some(permit) = take() else {
            return Some(Err(item));
        };
        self.pop().map(|item| Ok((item, permit)))
    }
}

impl<I: raw::Index> Drop for Drain<'_, '_, I> {
    fn drop(&mut self) {
        self.endpoint.topology().finish();
    }
}

/// Lazily driver-bound owner of one stable task-ready topology.
pub struct Slot<'d, I: raw::Index> {
    endpoint: cell::OnceCell<Endpoint<'d, I>>,
}

impl<'d, I: raw::Index> Default for Slot<'d, I> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'d, I: raw::Index> Slot<'d, I> {
    pub const fn new() -> Self {
        Self {
            endpoint: cell::OnceCell::new(),
        }
    }

    pub fn try_attach(
        &self,
        parent: task::Parent<'d>,
        local: I,
    ) -> Result<Edge<'_, 'd, I>, task::Error> {
        let endpoint = match self.endpoint.get() {
            Some(endpoint) => endpoint,
            None => {
                let endpoint = Endpoint::try_new(parent)?;
                self.endpoint.get_or_init(|| endpoint)
            }
        };
        Ok(endpoint.attach(local))
    }

    pub fn drain(&self) -> Option<Drain<'_, 'd, I>> {
        self.endpoint.get()?.drain()
    }

    pub fn peek(&self) -> Option<I> {
        self.endpoint.get()?.peek()
    }

    pub fn pop(&self) -> Option<I> {
        self.endpoint.get()?.pop()
    }

    pub fn is_empty(&self) -> bool {
        self.endpoint.get().is_none_or(Endpoint::is_empty)
    }
}
