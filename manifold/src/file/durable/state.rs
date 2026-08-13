use std::{io, process};

use dope_core::driver::{flight, schedule::ready::completion};

use crate::file::{self, durable};

pub(crate) struct Destination {
    pub(crate) file: file::Locked,
    pub(crate) offset: u64,
}

pub(crate) struct Block {
    pub(crate) bytes: Vec<u8>,
    pub(crate) first: Option<usize>,
    pub(crate) last: Option<usize>,
}

pub(crate) struct Ring<const N: usize> {
    indices: [usize; N],
    head: usize,
    len: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Phase {
    Write,
    Sync,
}

pub(crate) struct InFlight<'d> {
    pub(crate) block: usize,
    pub(crate) written: usize,
    pub(crate) phase: Phase,
    pub(crate) flight: Option<flight::Flight<'d>>,
}

pub(crate) struct Queue<'d, const N: usize> {
    pub(crate) blocks: [Block; N],
    pub(crate) free: Ring<N>,
    pub(crate) pending: Ring<N>,
    pub(crate) current: Option<usize>,
    pub(crate) in_flight: Option<InFlight<'d>>,
    pub(crate) closing: bool,
}

pub(crate) enum WaitState {
    Free,
    Pending { next: Option<usize> },
    Abandoned { next: Option<usize> },
    Done(Result<(), durable::Failure>),
}

pub(crate) struct WaitSlot<'d> {
    pub(crate) generation: u32,
    pub(crate) state: WaitState,
    pub(crate) wake: completion::Slot<'d>,
}

pub(crate) struct Inner<'d, const N: usize> {
    pub(crate) destination: Destination,
    pub(crate) queue: Queue<'d, N>,
    pub(crate) waiters: Box<[WaitSlot<'d>]>,
    pub(crate) free_waiters: Vec<usize>,
    pub(crate) failure: Option<durable::Failure>,
    pub(crate) capacity_wake: completion::Slot<'d>,
}

impl<const N: usize> Ring<N> {
    pub(crate) const fn empty() -> Self {
        Self {
            indices: [0; N],
            head: 0,
            len: 0,
        }
    }

    pub(crate) fn push(&mut self, index: usize) {
        if self.len >= N {
            process::abort();
        }
        self.indices[(self.head + self.len) % N] = index;
        self.len += 1;
    }

    pub(crate) fn pop(&mut self) -> Option<usize> {
        if self.len == 0 {
            return None;
        }
        let index = self.indices[self.head];
        self.head = (self.head + 1) % N;
        self.len -= 1;
        Some(index)
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Block {
    pub(crate) fn with_capacity(capacity: usize) -> io::Result<Self> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(io::Error::other)?;
        Ok(Self {
            bytes,
            first: None,
            last: None,
        })
    }

    fn clear(&mut self) {
        self.bytes.clear();
        self.first = None;
        self.last = None;
    }
}

impl WaitSlot<'_> {
    pub(crate) fn free() -> Self {
        Self {
            generation: 0,
            state: WaitState::Free,
            wake: completion::Slot::empty(),
        }
    }

    pub(crate) fn next_mut(&mut self) -> Option<&mut Option<usize>> {
        match &mut self.state {
            WaitState::Pending { next } | WaitState::Abandoned { next } => Some(next),
            WaitState::Free | WaitState::Done(_) => None,
        }
    }
}

impl<const N: usize> Inner<'_, N> {
    pub(crate) fn seal(&mut self) {
        let Some(block) = self.queue.current.take() else {
            return;
        };
        if self.queue.blocks[block].bytes.is_empty() {
            self.queue.free.push(block);
        } else {
            self.queue.pending.push(block);
        }
    }

    pub(crate) fn prepare(&mut self) -> Option<Phase> {
        self.seal();
        if self.queue.in_flight.is_none() {
            let block = self.queue.pending.pop()?;
            self.queue.in_flight = Some(InFlight {
                block,
                written: 0,
                phase: Phase::Write,
                flight: None,
            });
        }
        self.queue
            .in_flight
            .as_ref()
            .filter(|in_flight| in_flight.flight.is_none())
            .map(|in_flight| in_flight.phase)
    }

    pub(crate) fn complete_block(&mut self) {
        let Some(in_flight) = self.queue.in_flight.take() else {
            process::abort();
        };
        let block = &mut self.queue.blocks[in_flight.block];
        let Ok(length) = u64::try_from(block.bytes.len()) else {
            process::abort();
        };
        let Some(offset) = self.destination.offset.checked_add(length) else {
            process::abort();
        };
        self.destination.offset = offset;
        let first = block.first;
        block.clear();
        self.queue.free.push(in_flight.block);
        self.complete_chain(first, Ok(()));
        self.capacity_wake.wake();
    }

    pub(crate) fn fail(&mut self, failure: durable::Failure) {
        if self.failure.is_some() {
            return;
        }
        self.failure = Some(failure);
        if let Some(in_flight) = self.queue.in_flight.take() {
            debug_assert!(in_flight.flight.is_none());
            let block = &mut self.queue.blocks[in_flight.block];
            let first = block.first;
            block.clear();
            self.queue.free.push(in_flight.block);
            self.complete_chain(first, Err(failure));
        }
        if let Some(current) = self.queue.current.take() {
            let block = &mut self.queue.blocks[current];
            let first = block.first;
            block.clear();
            self.queue.free.push(current);
            self.complete_chain(first, Err(failure));
        }
        while let Some(index) = self.queue.pending.pop() {
            let block = &mut self.queue.blocks[index];
            let first = block.first;
            block.clear();
            self.queue.free.push(index);
            self.complete_chain(first, Err(failure));
        }
        self.capacity_wake.wake();
    }

    fn complete_chain(&mut self, mut current: Option<usize>, result: Result<(), durable::Failure>) {
        while let Some(index) = current {
            let Some(slot) = self.waiters.get_mut(index) else {
                process::abort();
            };
            current = match slot.state {
                WaitState::Pending { next } | WaitState::Abandoned { next } => next,
                WaitState::Free | WaitState::Done(_) => process::abort(),
            };
            match slot.state {
                WaitState::Pending { .. } => {
                    slot.state = WaitState::Done(result);
                    if let Some(wake) = slot.wake.take() {
                        wake.wake();
                    }
                }
                WaitState::Abandoned { .. } => {
                    slot.state = WaitState::Free;
                    slot.wake.clear();
                    self.free_waiters.push(index);
                }
                WaitState::Free | WaitState::Done(_) => process::abort(),
            }
        }
    }

    pub(crate) fn close(&mut self) {
        self.queue.closing = true;
        self.seal();
    }
}
