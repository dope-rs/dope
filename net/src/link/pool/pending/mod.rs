use dope_core::driver::{flight, route};

use crate::{
    link::{
        egress::{self, data},
        pool,
        slot::types,
    },
    wire,
};

mod sealed;

pub use sealed::Handle;
pub(super) use sealed::Queue;
pub(in crate::link) use sealed::Vacancy;

#[derive(Clone, Copy)]
pub enum Action {
    Egress,
    Close,
    Ingress,
}

pub struct Pending<
    'a,
    'd,
    const ID: u8,
    T: crate::Transport,
    W: wire::Wire,
    S,
    M,
    B,
    const IOV: usize,
> {
    pool: &'a pool::Connections<'d, ID, T, W, S, M, B, IOV>,
}

pub struct Mut<'a, 'd, const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize>
{
    pool: &'a mut pool::Connections<'d, ID, T, W, S, M, B, IOV>,
}

/// A scheduled egress projection whose fields share one pool borrow.
pub struct ScheduledEgress<'a, 'd, const ID: u8, W: wire::Wire, S, B, const IOV: usize> {
    pub flights: &'a flight::Slots<'d, route::KeyTag<ID>>,
    pub connection: &'a mut types::Connection<'d, ID, W, S>,
    pub pending: Handle<'a>,
    pub queue: egress::Queue<'a, 'd, IOV, B>,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Work(u8);

impl Action {
    const fn bit(self) -> u8 {
        match self {
            Self::Egress => 1,
            Self::Close => 2,
            Self::Ingress => 4,
        }
    }
}

impl<'a, 'd, const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize>
    Pending<'a, 'd, ID, T, W, S, M, B, IOV>
{
    pub fn of(pool: &'a pool::Connections<'d, ID, T, W, S, M, B, IOV>) -> Self {
        Self { pool }
    }

    pub fn at(self, key: pool::Key<'d, ID>) -> Option<Handle<'a>> {
        self.pool.prepared.slab.entries().at_parts(key.parts())?;
        self.pool.prepared.scheduling.pending.handle(key)
    }

    pub fn get(
        self,
        key: pool::Key<'d, ID>,
    ) -> Option<(&'a types::Connection<'d, ID, W, S>, Handle<'a>)> {
        let connection = &self
            .pool
            .prepared
            .slab
            .entries()
            .at_parts(key.parts())?
            .connection;
        let handle = self.pool.prepared.scheduling.pending.handle(key)?;
        Some((connection, handle))
    }

    pub fn by_target(
        self,
        target: route::Token,
    ) -> Option<(
        pool::Key<'d, ID>,
        &'a types::Connection<'d, ID, W, S>,
        Handle<'a>,
    )> {
        let key = self.pool.keys.parse(target)?;
        let connection = &self
            .pool
            .prepared
            .slab
            .entries()
            .at_parts(key.parts())?
            .connection;
        let handle = self.pool.prepared.scheduling.pending.handle(key)?;
        Some((key, connection, handle))
    }

    pub fn pop(self) -> Option<(pool::Key<'d, ID>, Work)> {
        self.pool.prepared.scheduling.pending.pop(self.pool.keys)
    }

    pub fn len(self) -> usize {
        self.pool.prepared.scheduling.pending.len()
    }

    pub fn is_empty(self) -> bool {
        self.pool.prepared.scheduling.pending.is_empty()
    }
}

impl<'a, 'd, const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize>
    Mut<'a, 'd, ID, T, W, S, M, B, IOV>
{
    pub fn of(pool: &'a mut pool::Connections<'d, ID, T, W, S, M, B, IOV>) -> Self {
        Self { pool }
    }

    pub fn get(
        self,
        key: pool::Key<'d, ID>,
    ) -> Option<(&'a mut types::Connection<'d, ID, W, S>, Handle<'a>)> {
        let pending = &self.pool.prepared.scheduling.pending;
        let connection = &mut self
            .pool
            .prepared
            .slab
            .entries_mut()
            .at_parts(key.parts())?
            .connection;
        let handle = pending.handle(key)?;
        Some((connection, handle))
    }

    pub fn egress(self, key: pool::Key<'d, ID>) -> Option<ScheduledEgress<'a, 'd, ID, W, S, B, IOV>>
    where
        B: data::Payload<'d>,
    {
        let pool::Prepared {
            flights,
            slab,
            egress,
            scheduling,
            ..
        } = &mut self.pool.prepared;
        let slot = slab.entries_mut().at_parts(key.parts())?;
        let handle = scheduling.pending.handle(key)?;
        let queue = egress.queue(&mut slot.egress);
        Some(ScheduledEgress {
            flights,
            connection: &mut slot.connection,
            pending: handle,
            queue,
        })
    }

    pub fn by_target(
        self,
        target: route::Token,
    ) -> Option<(
        pool::Key<'d, ID>,
        &'a mut types::Connection<'d, ID, W, S>,
        Handle<'a>,
    )> {
        let key = self.pool.keys.parse(target)?;
        let pending = &self.pool.prepared.scheduling.pending;
        let connection = &mut self
            .pool
            .prepared
            .slab
            .entries_mut()
            .at_parts(key.parts())?
            .connection;
        let handle = pending.handle(key)?;
        Some((key, connection, handle))
    }
}

impl Work {
    pub fn contains(self, action: Action) -> bool {
        self.0 & action.bit() != 0
    }
}
