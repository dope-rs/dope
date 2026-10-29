use std::{cell, marker, mem::size_of, num};

use o3::{
    alloc,
    base::cell::region,
    depot,
    depot::{capacity, queue::pool},
    mem,
};

use crate::{
    core::{
        driver::route::{indices, table::conn},
        io::transfer,
    },
    manifold::app::receive::limits,
    net::{
        error::read,
        read::{Capacity, Config},
    },
};

pub(crate) mod active;
mod entry;
pub(in crate::net) mod request;
pub(crate) mod result;
mod state;
pub(crate) mod table;

pub(crate) use state::Push;

const BYTES: num::NonZero<u32> = num::NonZero::<u32>::MIN.saturating_add((1 << 20) - 1);
const SLOTS: u32 = 4;
const MINIMUM: u32 = 256;
const CHUNKS: usize = 256;

pub(in crate::net::port) struct Chunk<R> {
    value: R,
    bytes: transfer::NonZero,
}

#[derive(Clone, Copy)]
pub(in crate::net) struct Plan {
    conns: conn::Capacity,
    pool: pool::Plan,
    shared: usize,
    receive: Config,
}

impl Plan {
    pub(in crate::net) const RETENTION: limits::Retention =
        limits::Retention::new(SLOTS as usize, MINIMUM as usize);

    pub(in crate::net) fn new(conns: conn::Capacity) -> Self {
        let slots = conns.scale::<SLOTS>().max(capacity::Count::new(MINIMUM));
        let pool = pool::Plan::from_counts(slots, conns.entries());
        let shared = slots.get() - conns.entries().get();
        Self {
            conns,
            pool,
            shared,
            receive: Config {
                slots: num::NonZero::<u32>::MIN.saturating_add(slots.raw() - 1),
                chunks: num::NonZero::<u32>::MIN.saturating_add(CHUNKS as u32 - 1),
                bytes: BYTES,
            },
        }
    }

    pub(in crate::net) fn with_receive(mut self, receive: Config) -> Result<Self, Capacity> {
        let count = self.conns.count().get();
        if receive.slots.get() < count {
            return Err(Capacity {
                connections: self.conns,
                slots: receive.slots,
            });
        }
        self.pool = pool::Plan::from_counts(
            capacity::Count::new(receive.slots.get()),
            self.conns.entries(),
        );
        self.shared = (receive.slots.get() - count) as usize;
        self.receive = receive;
        Ok(self)
    }

    pub(in crate::net) const fn connections(self) -> conn::Capacity {
        self.conns
    }

    pub(in crate::net) const fn count(self) -> capacity::Count {
        self.conns.entries()
    }
}

impl<R> Chunk<R> {
    pub(in crate::net::port) fn new(value: R) -> Option<Self>
    where
        R: transfer::Length + Unpin,
    {
        let bytes = value.len().nonzero()?;
        Some(Self { value, bytes })
    }

    pub(in crate::net::port) fn into_value(self) -> R {
        self.value
    }
}

#[derive(Default)]
struct State {
    bytes: u32,
}

pub(in crate::net::port) struct Queue<'d, R> {
    storage: cell::UnsafeCell<pool::Lanes<Chunk<R>, State, false>>,
    shared: cell::Cell<usize>,
    region: marker::PhantomData<&'d mut region::Token<region::Identity<'d>>>,
    receive: Config,
}

struct Insertion<'a, R> {
    queue: pool::Lane<'a, Chunk<R>, State, false>,
    chunk: Chunk<R>,
}

// SAFETY: Plan reserves one slot per lane; push charges shared credit for
// every further chunk. Their sum bounds this allocation's live occupancy.
// Only push constructs this entitlement and immediately inserts it; pop_state
// returns its charge before the value escapes, with TRACK=false throughout.
unsafe impl<'a, R: 'a> depot::raw::PoolInsert<'a, false> for Insertion<'a, R> {
    type Item = Chunk<R>;
    type State = State;

    unsafe fn into_parts(self) -> (pool::Lane<'a, Self::Item, State, false>, Self::Item) {
        (self.queue, self.chunk)
    }
}

impl<'d, R> Queue<'d, R> {
    pub(in crate::net::port) fn try_new(
        plan: Plan,
        _: &region::Token<region::Identity<'d>>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            storage: cell::UnsafeCell::new(pool::Lanes::try_new(plan.pool)?),
            shared: cell::Cell::new(plan.shared),
            region: marker::PhantomData,
            receive: plan.receive,
        })
    }

    pub(in crate::net) fn retention(&self) -> limits::Retention {
        limits::Retention::new(0, self.receive.slots.get() as usize)
    }

    pub(in crate::net::port) fn push(
        &self,
        lane: indices::SlotIndex,
        input: Chunk<R>,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), (R, read::Local)>
    where
        R: Unpin,
    {
        let Chunk { value, bytes } = input;
        // SAFETY: only push/pop mutate, under the matching exclusive region token.
        // Neither operation lends a payload reference or invokes user code.
        let storage = unsafe { &mut *self.storage.get() };
        let lanes = storage.len();
        let index = lane.index().into_usize();
        let Some(mut queue) = storage.get_mut(index) else {
            return Err((value, read::Local::RetentionLane { index, lanes }));
        };
        if queue.count().get() >= self.receive.chunks.get() as usize {
            return Err((
                value,
                read::Local::ChunkCountTooLarge {
                    actual: read::Chunks::new(queue.count().get() + 1),
                    maximum: read::Chunks::new(self.receive.chunks.get() as usize),
                },
            ));
        }
        let weight = bytes.into_nonzero();
        let held = queue.state().bytes;
        let Some(total) = weight
            .checked_add(held)
            .filter(|total| *total <= self.receive.bytes)
        else {
            return Err((
                value,
                read::Local::BufferedBytesTooLarge {
                    actual: read::Buffer::new(u64::from(held) + u64::from(weight.get())),
                    maximum: read::Buffer::new(u64::from(self.receive.bytes.get())),
                },
            ));
        };
        if !queue.is_empty() {
            let balance = mem::raw::Balance::new([queue.count().get()], [self.shared.get()]);
            let Ok(balance) = balance.try_acquire([1], [1]) else {
                return Err((value, read::Local::RetentionExhausted));
            };
            let (_, [shared]) = balance.into_parts();
            self.shared.set(shared);
        }
        queue.state_mut().bytes = total.get();
        pool::Lane::insert(Insertion {
            queue,
            chunk: Chunk { value, bytes },
        });
        Ok(())
    }

    /// Pops the FIFO front and refunds its original byte charge to the same lane.
    pub(in crate::net::port) fn pop_state(
        &self,
        lane: indices::SlotIndex,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> (Option<R>, bool)
    where
        R: Unpin,
    {
        // SAFETY: the region token excludes mutation; no borrowed payload escapes.
        let storage = unsafe { &mut *self.storage.get() };
        let Some(mut queue) = storage.get_mut(lane.index().into_usize()) else {
            return (None, true);
        };
        let held = queue.count().get();
        let Some(Chunk { value, bytes }) = queue.pop_front() else {
            return (None, true);
        };
        let state = queue.state_mut();
        state.bytes = state.bytes.wrapping_sub(bytes.into_nonzero().get());
        if held > 1 {
            // SAFETY: the removed front owns one fair charge. The original pool
            // capacity bounds its refund, and the matching token excludes mutation.
            let (_, [shared]) = unsafe {
                mem::raw::Balance::new([held], [self.shared.get()]).release_raw([1], [1])
            }
            .into_parts();
            self.shared.set(shared);
        }
        (Some(value), queue.is_empty())
    }

    pub(in crate::net::port) fn is_empty(&self, lane: indices::SlotIndex) -> bool {
        // SAFETY: mutation is synchronous, has no user callback, and lends no reference.
        unsafe { &*self.storage.get() }
            .get(lane.index().into_usize())
            .is_none_or(pool::View::is_empty)
    }
}

const _: () = {
    assert!(size_of::<Queue<'static, u64>>() == size_of::<[usize; 8]>());
    assert!(size_of::<State>() == size_of::<u32>());
    assert!(size_of::<Chunk<u64>>() == 2 * size_of::<usize>());
};
