use std::{cell, io, process};

use dope::{
    core::driver::route::{self, table},
    manifold::receive,
    net::wire,
};
use o3::{
    cell::region,
    collections::{self, fixed::arena},
    mem::fair,
};

use crate::net::port::recv::queue;

const RECV_SLOTS_PER_CONN: u32 = 4;
const MIN_RECV_SLOTS: u32 = 256;
const RECV_CHUNK_CAP: usize = 256;
const _: () = assert!(route::SLOT_MASK <= (u32::MAX / RECV_SLOTS_PER_CONN) as u64);

struct RecvEntry<R> {
    value: R,
    len: u32,
}

struct Storage<R> {
    entries: arena::Linked<RecvEntry<R>>,
    credits: fair::Credits,
}

pub(in crate::net) struct RecvArena<'d, R: 'd> {
    storage: region::Value<'d, Storage<R>>,
    live: cell::Cell<usize>,
}

pub(in crate::net::port) enum PushError {
    Limit,
    Exhausted,
}

#[derive(Clone, Copy)]
pub(in crate::net) struct RecvLayout {
    connections: table::ConnectionCapacity,
    slots: u32,
}

impl RecvLayout {
    pub(in crate::net) const RETENTION: receive::Retention =
        receive::Retention::new(RECV_SLOTS_PER_CONN as usize, MIN_RECV_SLOTS as usize);

    pub(in crate::net) fn new(connections: usize) -> io::Result<Self> {
        let connections = table::ConnectionCapacity::new(connections).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "fiber: connection capacity must be in 1..=2^24-1",
            )
        })?;
        let slots = u32::try_from(Self::RETENTION.capacity(connections.get())?).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "fiber: receive slot capacity exceeds u32",
            )
        })?;
        Ok(Self { connections, slots })
    }

    pub(in crate::net) fn slots(self) -> usize {
        self.slots as usize
    }

    pub(in crate::net) fn connections(self) -> usize {
        self.connections.get()
    }
}

impl<'d, R: wire::Cursor<'d> + 'd> RecvArena<'d, R> {
    pub(in crate::net::port) fn try_with_layout(
        layout: RecvLayout,
    ) -> Result<Self, collections::AllocationError> {
        use o3::{collections::fixed::arena::Linked, mem::fair::Credits};
        Ok(Self {
            storage: region::Value::new(Storage {
                entries: Linked::try_with_capacity(layout.slots(), layout.connections())?,
                credits: Credits::try_with_reserve(layout.slots(), layout.connections(), 1)?,
            }),
            live: cell::Cell::new(0),
        })
    }

    pub(in crate::net::port) fn push(
        &self,
        lane: usize,
        queue: &queue::RecvQueue,
        value: R,
        region: &mut region::Token<'d>,
    ) -> Result<(), PushError> {
        let len = u32::try_from(value.remaining()).map_err(|_| PushError::Limit)?;
        let Some(update) = queue.prepare_push(len) else {
            return Err(PushError::Limit);
        };
        let storage = self.storage.borrow_mut(region);
        if storage.entries.lane_len(lane) >= RECV_CHUNK_CAP {
            return Err(PushError::Limit);
        }
        if !storage.credits.try_acquire(lane, 1) {
            return Err(PushError::Exhausted);
        }
        match storage.entries.push_back(lane, RecvEntry { value, len }) {
            Ok(()) => {
                update.commit();
                self.live.set(self.live.get() + 1);
                Ok(())
            }
            Err(_) => {
                storage.credits.release(lane, 1);
                Err(PushError::Exhausted)
            }
        }
    }

    fn pop_from(
        &self,
        storage: &mut Storage<R>,
        lane: usize,
        queue: &queue::RecvQueue,
    ) -> Option<R> {
        let chunks = storage.entries.lane_len(lane);
        let len = storage.entries.front(lane)?.len;
        let Some(update) = queue.prepare_pop(len) else {
            process::abort();
        };
        if update.leaves_empty() != (chunks == 1) {
            process::abort();
        }
        let Some(entry) = storage.entries.pop_front(lane) else {
            process::abort();
        };
        storage.credits.release(lane, 1);
        update.commit();
        let Some(live) = self.live.get().checked_sub(1) else {
            process::abort();
        };
        self.live.set(live);
        Some(entry.value)
    }

    pub(in crate::net::port) fn take_front(
        &self,
        lane: usize,
        queue: &queue::RecvQueue,
        region: &mut region::Token<'d>,
    ) -> Option<R> {
        let storage = self.storage.borrow_mut(region);
        self.pop_from(storage, lane, queue)
    }

    pub(in crate::net::port) fn lane_is_empty(
        &self,
        lane: usize,
        queue: &queue::RecvQueue,
        region: &mut region::Token<'d>,
    ) -> bool {
        let storage = self.storage.borrow_mut(region);
        let arena_empty = storage.entries.lane_is_empty(lane);
        if arena_empty != queue.is_empty() {
            process::abort();
        }
        arena_empty
    }
}

impl<R> Drop for RecvArena<'_, R> {
    fn drop(&mut self) {
        if self.live.get() != 0 {
            process::abort();
        }
    }
}
