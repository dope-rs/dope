use std::io::{self, Error, ErrorKind};

use dope_net::wire::{RecvCursor, RecvTarget};
use o3::cell::{RegionCell, RegionToken};
use o3::collections::LinkedArena;
use o3::mem::FairCredits;

use super::NONE;
use super::queue::RecvQueue;

const RECV_SLOTS_PER_CONN: usize = 4;
const MIN_RECV_SLOTS: usize = 256;

struct RecvEntry<R> {
    value: R,
    len: u32,
}

struct Storage<R> {
    entries: LinkedArena<RecvEntry<R>>,
    credits: FairCredits,
}

pub(in crate::net) struct RecvArena<'d, R: 'd> {
    storage: RegionCell<'d, Storage<R>>,
}

pub(in crate::net::port) enum PushError {
    Limit,
    Exhausted,
}

#[derive(Clone, Copy)]
pub(in crate::net) struct RecvLayout {
    connections: usize,
    slots: usize,
}

impl RecvLayout {
    pub(in crate::net) fn new(connections: usize) -> io::Result<Self> {
        if connections == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "fiber: connection capacity must be positive",
            ));
        }
        let slots = connections
            .checked_mul(RECV_SLOTS_PER_CONN)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "fiber: receive capacity overflow"))?
            .max(MIN_RECV_SLOTS);
        if slots > NONE as usize {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "fiber: receive capacity exceeds slot index",
            ));
        }
        Ok(Self { connections, slots })
    }

    pub(in crate::net) fn slots(self) -> usize {
        self.slots
    }

    pub(in crate::net) fn connections(self) -> usize {
        self.connections
    }
}

impl<'d, R: RecvCursor + 'd> RecvArena<'d, R> {
    pub(in crate::net::port) fn with_layout(layout: RecvLayout) -> Self {
        Self {
            storage: RegionCell::new(Storage {
                entries: LinkedArena::with_capacity(layout.slots, layout.connections),
                credits: FairCredits::with_reserve(layout.slots, layout.connections, 1),
            }),
        }
    }

    pub(in crate::net::port) fn push(
        &self,
        lane: usize,
        queue: &RecvQueue,
        value: R,
        region: &mut RegionToken<'d>,
    ) -> Result<(), PushError> {
        let len = u32::try_from(value.remaining()).map_err(|_| PushError::Limit)?;
        let Some(next_state) = queue.state().pushed(len as usize) else {
            return Err(PushError::Limit);
        };
        let storage = self.storage.borrow_mut(region);
        if !storage.credits.try_acquire(lane, 1) {
            return Err(PushError::Exhausted);
        }
        match storage.entries.push_back(lane, RecvEntry { value, len }) {
            Ok(()) => {
                queue.commit(next_state);
                Ok(())
            }
            Err(_) => {
                storage.credits.release(lane, 1);
                Err(PushError::Exhausted)
            }
        }
    }

    fn pop_from(storage: &mut Storage<R>, lane: usize, queue: &RecvQueue) -> Option<R> {
        let entry = storage.entries.pop_front(lane)?;
        let next_state = queue.state().popped(entry.len as usize)?;
        storage.credits.release(lane, 1);
        queue.commit(next_state);
        Some(entry.value)
    }

    pub(in crate::net::port) fn drain_into(
        &self,
        lane: usize,
        queue: &RecvQueue,
        target: &mut RecvTarget<'_>,
        region: &mut RegionToken<'d>,
    ) {
        let storage = self.storage.borrow_mut(region);
        if queue.state().single() {
            let front = storage.entries.front_mut(lane).map(|entry| {
                let len = entry.len as usize;
                let read = (len <= target.remaining())
                    .then(|| target.with_limit(len, |target| entry.value.read_into(target)));
                (len, read)
            });
            if let Some((len, Some(read))) = front {
                if read == len && Self::pop_from(storage, lane, queue).is_some() {
                    return;
                }
                if read == 0 {
                    return;
                }
                let Some(next_state) = queue.state().consumed(read) else {
                    return;
                };
                let Some(entry) = storage.entries.front_mut(lane) else {
                    return;
                };
                entry.len = (len - read) as u32;
                queue.commit(next_state);
                return;
            }
        }

        while target.remaining() != 0 {
            let Some(entry) = storage.entries.front_mut(lane) else {
                break;
            };
            let len = entry.len as usize;
            let want = target.remaining().min(len);
            let read = target.with_limit(want, |target| entry.value.read_into(target));
            if read == 0 {
                break;
            }
            if read < len {
                let Some(next_state) = queue.state().consumed(read) else {
                    break;
                };
                entry.len = (len - read) as u32;
                queue.commit(next_state);
            } else {
                drop(Self::pop_from(storage, lane, queue));
            }
        }
    }

    pub(in crate::net::port) fn reset(
        &self,
        lane: usize,
        queue: &RecvQueue,
        region: &mut RegionToken<'d>,
    ) {
        let storage = self.storage.borrow_mut(region);
        while Self::pop_from(storage, lane, queue).is_some() {}
    }
}
