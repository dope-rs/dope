use std::{cell, mem};

use dope_core::driver::route::{
    Identity, KeyTag,
    table::{self, borrows, conn, entries::vacant},
};
use o3::{
    alloc,
    depot::{arena::slab, queue::indexed::shared},
};
use pending::node::{Next, Node, State, Wake};

use crate::{
    link::{
        egress::reserve::demand,
        pool::{
            self, pending,
            recv::{input, storage::tail},
        },
        slot::types,
    },
    wire::{self, send},
};

mod node;
mod round;

pub use round::Round;

pub struct Pending<
    'a,
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec,
    S,
    M: input::Mode<W>,
    B,
    const IOV: usize,
    D: demand::Mode = demand::Fallible,
> where
    'd: 'w,
{
    pool: &'a pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
}

pub struct Mut<'a, 'd, ID: Identity, C> {
    slab: borrows::Access<'a, C, ID>,
    queue: &'a Queue<'d, ID>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Disposition {
    /// No scheduler-owned work remains. Any future progress has an external wake.
    Idle,
    /// The exact generation no longer exists and must be invalidated.
    Gone,
}

impl<'a, 'p, 'w, 'd, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    Pending<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    'd: 'w,
    T: crate::Transport,
    W: wire::Spec,
    M: input::Mode<W>,
    D: demand::Mode,
{
    pub fn of(pool: &'a pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>) -> Self {
        Self { pool }
    }

    pub fn at(self, key: pool::Key<'d, ID>) -> Option<Handle<'a, 'd, ID>> {
        if !self.pool.prepared.slab.view().contains(key.parts()) {
            return None;
        }
        self.pool.prepared.pending.handle(key)
    }

    pub fn close(self, key: pool::Key<'d, ID>) {
        if let Some(conn) = self.pool.prepared.slab.view().get(key.parts())
            && let Some(handle) = self.pool.prepared.pending.handle(key)
        {
            conn.io().close();
            handle.wake();
        }
    }

    pub fn try_round(self) -> Result<Round<'p, 'd, ID>, Busy> {
        Round::try_new(
            self.pool.prepared.pending,
            *self.pool.keys,
            self.has_capacity(),
        )
    }

    pub fn is_empty(self) -> bool {
        self.pool.prepared.pending.is_empty() && !self.has_capacity()
    }

    fn has_capacity(&self) -> bool {
        self.pool.prepared.pending.has_waiting()
            && <W::Send as send::Policy<W>>::has_capacity(&self.pool.prepared.runtime)
    }
}

impl<'a, 'w, 'd, ID: Identity + 'w, W: wire::Spec, S>
    Mut<'a, 'd, ID, types::Connection<'w, 'd, ID, W, S>>
where
    'd: 'w,
{
    pub fn of<'p, T, M, B, const IOV: usize, D>(
        pool: &'a mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
    ) -> Self
    where
        T: crate::Transport,
        M: input::Mode<W>,
        D: demand::Mode,
    {
        Self {
            queue: pool.prepared.pending,
            slab: pool.prepared.slab.access(),
        }
    }
}

impl<'a, 'd, ID: Identity, C> Mut<'a, 'd, ID, C> {
    pub fn get(self, key: pool::Key<'d, ID>) -> Option<(&'a mut C, Handle<'a, 'd, ID>)> {
        let conn = self.slab.get(key.parts())?;
        let handle = self.queue.handle(key)?;
        Some((conn, handle))
    }
}

pub(super) struct Queue<'d, ID: Identity> {
    nodes: shared::Fifo<pool::Key<'d, ID>, Node<'d, ID>>,
    topology: cell::Cell<Topology<'d, ID>>,
    status: cell::Cell<Status>,
    claimed: cell::Cell<Lock<'d, ID>>,
    capacity_claim: cell::Cell<Lock<'d, ID>>,
}

struct Detached<'a, 'd, ID: Identity> {
    index: pool::raw::Physical<'d, ID>,
    node: &'a Node<'d, ID>,
}

#[derive(Clone, Copy)]
struct Chain<'d, ID: Identity> {
    head: pool::raw::Physical<'d, ID>,
    tail: pool::raw::Physical<'d, ID>,
}

#[derive(Clone, Copy)]
enum Topology<'d, ID: Identity> {
    Empty,
    Occupied(Chain<'d, ID>),
}

#[derive(Clone, Copy)]
enum Lock<'d, ID: Identity> {
    Free,
    Held(pool::raw::Physical<'d, ID>),
}

#[derive(Clone, Copy)]
enum Status {
    Ready,
    Draining,
}

pub(in crate::link) struct Vacancy<'a, 'd, ID: Identity, T> {
    reservation: vacant::Entry<'a, T, ID>,
    key: pool::Key<'d, ID>,
}

pub(super) enum Admission<'q, 'a, 'd, ID: Identity, T> {
    Ready(Grant<'q, 'a, 'd, ID, T>),
    Busy,
    Full,
    Exhausted,
}

pub(super) struct Grant<'q, 'a, 'd, ID: Identity, T> {
    queue: &'q Queue<'d, ID>,
    keys: pool::Keyspace<'d, ID>,
    grant: borrows::Grant<'a, T, ID>,
}

#[derive(Debug)]
pub struct Busy;

#[derive(Clone, Copy)]
/// One generation-exact wake capability tied to its pool borrow and route.
pub struct Handle<'a, 'd, ID: Identity = KeyTag<0>> {
    queue: &'a Queue<'d, ID>,
    key: pool::Key<'d, ID>,
}

impl<'d, ID: Identity> Queue<'d, ID> {
    pub(in crate::link::pool) fn try_new(capacity: conn::Capacity) -> Result<Self, alloc::Error> {
        Ok(Self {
            nodes: shared::Fifo::try_new(capacity.entries().get())?,
            topology: cell::Cell::new(Topology::Empty),
            status: cell::Cell::new(Status::Ready),
            claimed: cell::Cell::new(Lock::Free),
            capacity_claim: cell::Cell::new(Lock::Free),
        })
    }

    pub(in crate::link::pool) fn handle(
        &self,
        key: pool::Key<'d, ID>,
    ) -> Option<Handle<'_, 'd, ID>> {
        let node = self.nodes.get(key.lane().index().get() as usize)?.state();
        (node.epoch.get() == key.epoch()).then_some(Handle { queue: self, key })
    }

    pub(in crate::link::pool) fn admit<'q, 'a, T, S, R>(
        &'q self,
        slab: &'a mut tail::Table<T, ID, S, R>,
        keys: pool::Keyspace<'d, ID>,
        capacity: conn::Capacity,
    ) -> Admission<'q, 'a, 'd, ID, T> {
        let slots = table::Capacity::from(capacity);
        let grant = match self.claimed.get() {
            Lock::Held(claimed) => {
                let claimed = claimed.lane();
                if !slots.contains(claimed) || slab.view().key_at(claimed).is_some() {
                    slab.vacancy().try_head(capacity)
                } else {
                    match slab.vacancy().try_except(capacity, claimed) {
                        Err(slab::Vacancy::Full) => return Admission::Busy,
                        grant => grant,
                    }
                }
            }
            Lock::Free => slab.vacancy().try_head(capacity),
        };
        match grant {
            Ok(grant) => Admission::Ready(Grant {
                queue: self,
                keys,
                grant,
            }),
            Err(slab::Vacancy::Full) => Admission::Full,
            Err(slab::Vacancy::IdentityExhausted) => Admission::Exhausted,
        }
    }

    pub(in crate::link::pool) fn is_empty(&self) -> bool {
        matches!(self.topology.get(), Topology::Empty) && matches!(self.status.get(), Status::Ready)
    }

    pub(in crate::link::pool) fn has_waiting(&self) -> bool {
        !self.nodes.is_empty::<0>()
    }

    pub(in crate::link::pool) fn release_send(&self) {
        if matches!(self.capacity_claim.get(), Lock::Held(_)) {
            return;
        }
        let Some((slot, key)) = self.nodes.next::<0>() else {
            return;
        };
        let node = slot.state();
        let index = pool::raw::Physical::bind(key);
        if node.epoch.get() != key.epoch() || matches!(node.state.get(), State::Retired) {
            return;
        }
        self.capacity_claim.set(Lock::Held(index));
        node.capacity.set(true);
        Handle { queue: self, key }.wake();
    }

    fn clear_wait(&self, index: pool::raw::Physical<'d, ID>) {
        let slot = self.slot(index);
        slot.clear::<0>();
        slot.state().capacity.set(false);
        self.end_capacity(index);
    }

    fn end_capacity(&self, index: pool::raw::Physical<'d, ID>) {
        if let Lock::Held(held) = self.capacity_claim.get()
            && held.lane() == index.lane()
        {
            self.capacity_claim.set(Lock::Free);
        }
    }

    fn enqueue(&self, detached: Detached<'_, 'd, ID>) {
        let Detached { index, node } = detached;
        node.next.set(Next::End);
        node.state.set(State::Queued);
        let chain = match self.topology.get() {
            Topology::Empty => Chain {
                head: index,
                tail: index,
            },
            Topology::Occupied(Chain { head, tail }) => {
                self.node(tail).next.set(Next::Slot(index));
                Chain { head, tail: index }
            }
        };
        self.topology.set(Topology::Occupied(chain));
    }
}

impl<'a, 'd, ID: Identity, T> Grant<'_, 'a, 'd, ID, T> {
    pub(in crate::link::pool) fn reserve(self) -> Vacancy<'a, 'd, ID, T> {
        let entry = self.grant.reserve();
        let key = self.keys.bind_table(entry.key());
        let node = self.queue.node(pool::raw::Physical::bind(key));
        self.queue.clear_wait(pool::raw::Physical::bind(key));
        node.epoch.set(key.epoch());
        Vacancy {
            reservation: entry,
            key,
        }
    }
}

impl<'a, 'd, ID: Identity, T> Vacancy<'a, 'd, ID, T> {
    pub(in crate::link::pool) fn key(&self) -> pool::Key<'d, ID> {
        self.key
    }

    pub(in crate::link::pool) fn commit_with<R>(
        self,
        build: impl FnOnce(pool::Key<'d, ID>) -> (T, R),
    ) -> R {
        let Self {
            reservation: entry,
            key,
        } = self;
        let (value, result) = build(key);
        entry.insert(value);
        result
    }
}

impl<'d, ID: Identity> Queue<'d, ID> {
    fn node(&self, index: pool::raw::Physical<'d, ID>) -> &Node<'d, ID> {
        self.slot(index).state()
    }

    fn slot(
        &self,
        index: pool::raw::Physical<'d, ID>,
    ) -> shared::Slot<'_, pool::Key<'d, ID>, Node<'d, ID>> {
        use o3::depot::{index::Initialized, raw::Proof};

        // SAFETY: admission binds every link to this queue's route and capacity.
        unsafe {
            Proof::<_, Initialized<()>>::new((&self.nodes, index.lane().index().get() as usize))
        }
        .into()
    }
}

impl<'d, ID: Identity> Handle<'_, 'd, ID> {
    /// Synchronizes this exact generation's shared-send-capacity wait.
    pub fn wait_send(self, waiting: bool) {
        let index = pool::raw::Physical::bind(self.key);
        let slot = self.queue.slot(index);
        let node = slot.state();
        if node.epoch.get() != self.key.epoch() || matches!(node.state.get(), State::Retired) {
            return;
        }
        node.capacity.set(false);
        if waiting {
            if slot.get::<0>() != Some(self.key) {
                slot.push::<0>(self.key);
            }
        } else {
            self.queue.clear_wait(index);
        }
    }

    /// Retains a resource wake until its send attempt runs, including across
    /// an unrelated receive barrier. Ordinary pending work does not imply it.
    pub fn has_capacity(self) -> bool {
        let node = self.queue.node(pool::raw::Physical::bind(self.key));
        node.epoch.get() == self.key.epoch() && node.capacity.get()
    }

    pub fn wake(self) {
        let Self { queue, key } = self;
        let index = pool::raw::Physical::bind(key);
        let node = queue.node(index);
        if node.epoch.get() != key.epoch() {
            return;
        }
        match node.state.get() {
            State::Idle => queue.enqueue(Detached { index, node }),
            State::Queued | State::Retired => {}
            State::Claimed => node.wake.set(Wake::Pending),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Queue<'static, KeyTag<0>>>() == 6 * mem::size_of::<u64>());
    assert!(mem::size_of::<Round<'static, 'static, KeyTag<0>>>() == 2 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Handle<'static, 'static, KeyTag<0>>>()
            == mem::size_of::<usize>() + mem::size_of::<pool::Key<'static, KeyTag<0>>>()
    );
};
