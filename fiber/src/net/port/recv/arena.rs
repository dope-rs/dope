use std::cell::Cell;
use std::io::{self, Error, ErrorKind};

use crate::io::RecvBuffer;

use super::NONE;
use super::queue::{QueueState, RecvQueue};
use super::raw::{RecvSlot, Recycle};

const RECV_SLOTS_PER_CONN: usize = 4;
const MIN_RECV_SLOTS: usize = 256;

pub(in crate::net) struct RecvArena<'d> {
    slots: Box<[RecvSlot<'d>]>,
    reserved_free: Cell<u32>,
    shared_free: Cell<u32>,
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

    fn slots(self) -> usize {
        self.slots
    }

    pub(in crate::net) fn connections(self) -> usize {
        self.connections
    }
}

impl<'d> RecvArena<'d> {
    pub(in crate::net) fn capacity_for(connections: usize) -> io::Result<usize> {
        RecvLayout::new(connections).map(RecvLayout::slots)
    }

    pub(in crate::net::port) fn with_layout(layout: RecvLayout) -> Self {
        let slots: Box<[_]> = (0..layout.slots)
            .map(|index| {
                RecvSlot::free(
                    if index + 1 == layout.connections || index + 1 == layout.slots {
                        NONE
                    } else {
                        (index + 1) as u32
                    },
                )
            })
            .collect();
        Self {
            reserved_free: Cell::new(0),
            shared_free: Cell::new(layout.connections as u32),
            slots,
        }
    }

    fn reserve(&self, queue: &RecvQueue) -> bool {
        if queue.reserved() != NONE {
            return true;
        }
        let index = self.reserved_free.get();
        if index == NONE {
            return false;
        }
        let slot = &self.slots[index as usize];
        let Some(next) = slot.claim() else {
            return false;
        };
        self.reserved_free.set(next);
        queue.set_reserved(index);
        true
    }

    fn claim_shared(&self) -> Option<u32> {
        let index = self.shared_free.get();
        if index == NONE {
            return None;
        }
        let next = self.slots[index as usize].claim()?;
        self.shared_free.set(next);
        Some(index)
    }

    fn release_claim(&self, index: u32, reserved: u32) {
        if index == reserved {
            return;
        }
        let next = self.shared_free.get();
        if self.slots[index as usize].release(next) {
            self.shared_free.set(index);
        }
    }

    pub(in crate::net::port) fn push(
        &self,
        queue: &RecvQueue,
        value: RecvBuffer<'d>,
        len: u32,
    ) -> Result<(), PushError> {
        let state = queue.state();
        let reserved = queue.reserved();
        if reserved == NONE {
            return Err(PushError::Exhausted);
        }
        let reserved_slot = &self.slots[reserved as usize];
        let index = match state {
            QueueState::Empty if reserved_slot.is_reserved() => reserved,
            QueueState::Empty => return Err(PushError::Exhausted),
            QueueState::Linked { .. } if reserved_slot.is_reserved() => reserved,
            QueueState::Linked { .. } => match self.claim_shared() {
                Some(index) => index,
                None => return Err(PushError::Exhausted),
            },
        };
        let Some(next_state) = state.pushed(index, len as usize) else {
            self.release_claim(index, reserved);
            return Err(PushError::Limit);
        };
        let slot = &self.slots[index as usize];
        if slot.insert(value, len).is_err() {
            self.release_claim(index, reserved);
            return Err(PushError::Exhausted);
        }
        if let Some(tail) = state.tail()
            && !self.slots[tail as usize].set_queued_next(index)
        {
            let recycle = if index == reserved {
                Recycle::Reserved
            } else {
                Recycle::Free {
                    next: self.shared_free.get(),
                }
            };
            let removed = slot.take(recycle);
            if index != reserved && removed.is_some() {
                self.shared_free.set(index);
            }
            drop(removed);
            return Err(PushError::Exhausted);
        }
        queue.commit(next_state);
        Ok(())
    }

    pub(in crate::net::port) fn pop(&self, queue: &RecvQueue) -> Option<RecvBuffer<'d>> {
        let state = queue.state();
        let index = state.head()?;
        let slot = &self.slots[index as usize];
        let (len, next) = slot.queued_meta()?;
        let next_state = state.popped(index, next, len)?;
        let reserved = queue.reserved();
        let recycle = if index == reserved {
            Recycle::Reserved
        } else {
            Recycle::Free {
                next: self.shared_free.get(),
            }
        };
        let value = slot.take(recycle)?;
        if index != reserved {
            self.shared_free.set(index);
        }
        queue.commit(next_state);
        Some(value)
    }

    pub(in crate::net::port) fn drain_into(&self, queue: &RecvQueue, dst: &mut [u8]) -> usize {
        if let Some(head) = queue.state().single()
            && head == queue.reserved()
        {
            let slot = &self.slots[head as usize];
            let Some((len, _)) = slot.queued_meta() else {
                return 0;
            };
            if len <= dst.len() && slot.copy_prefix(&mut dst[..len]) {
                if self.pop(queue).is_none() {
                    return 0;
                }
                return len;
            }
        }

        let mut written = 0usize;
        while written < dst.len() {
            let state = queue.state();
            let Some(index) = state.head() else {
                break;
            };
            let slot = &self.slots[index as usize];
            let Some((len, _)) = slot.queued_meta() else {
                break;
            };
            let want = (dst.len() - written).min(len);
            if !slot.copy_prefix(&mut dst[written..written + want]) {
                break;
            }
            written += want;
            if want < len {
                let Some(next_state) = state.consumed(want) else {
                    break;
                };
                if !slot.advance(want) {
                    break;
                }
                queue.commit(next_state);
            } else {
                drop(self.pop(queue));
            }
        }
        written
    }

    pub(in crate::net::port) fn reset(&self, queue: &RecvQueue) -> bool {
        while self.pop(queue).is_some() {}
        self.reserve(queue)
    }
}
