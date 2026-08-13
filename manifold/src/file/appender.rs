use std::{array, fmt, fs, io, marker, os::unix::fs::OpenOptionsExt as _, path, pin, process};

use dope_core::{
    driver::{
        self, flight,
        lifecycle::routing,
        route::{self, kind},
    },
    io::transfer,
};

use crate::file;

const fn empty_indices<const N: usize>() -> [usize; N] {
    [0; N]
}

#[derive(Debug)]
pub enum WriteError {
    Full,
    TooLarge,
    Failed,
    Closed,
}

#[must_use = "an append record must be committed or it is rolled back"]
pub struct Record<'a, 'd> {
    block: &'a mut Vec<u8>,
    start: usize,
    limit: usize,
    committed: bool,
    driver: marker::PhantomData<driver::Reference<'d>>,
}

pub struct Control<'step, 'd, const ID: u8, const N: usize, const B: usize>
where
    'd: 'step,
{
    pub(super) inner: pin::Pin<&'step mut Appender<'d, ID, N, B>>,
}

pub(super) struct Destination {
    pub(super) file: file::Locked,
    pub(super) offset: u64,
    pub(super) failure: Option<io::Error>,
}

struct Ring<const N: usize> {
    indices: [usize; N],
    head: usize,
    len: usize,
}

pub(super) struct InFlight<'d> {
    pub(super) block: usize,
    pub(super) written: usize,
    pub(super) flight: Option<flight::Flight<'d>>,
}

pub(super) struct Queue<'d, const N: usize> {
    pub(super) blocks: [Vec<u8>; N],
    free: Ring<N>,
    pending: Ring<N>,
    current: Option<usize>,
    pub(super) in_flight: Option<InFlight<'d>>,
    closing: bool,
}

#[pin_project::pin_project]
pub struct Appender<'d, const ID: u8, const N: usize, const B: usize> {
    pub(super) route: routing::Route<'d, ID>,
    pub(super) flights: flight::Slots<'d, route::KeyTag<ID, { kind::WRITE }>>,
    pub(super) destination: Destination,
    pub(super) queue: Queue<'d, N>,
}

impl<const N: usize> Ring<N> {
    const fn empty() -> Self {
        Self {
            indices: empty_indices(),
            head: 0,
            len: 0,
        }
    }

    fn push(&mut self, index: usize) {
        if self.len == N {
            process::abort();
        }
        self.indices[(self.head + self.len) % N] = index;
        self.len += 1;
    }

    fn pop(&mut self) -> Option<usize> {
        if self.len == 0 {
            return None;
        }
        let index = self.indices[self.head];
        self.head = (self.head + 1) % N;
        self.len -= 1;
        Some(index)
    }

    const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<'d, const ID: u8, const N: usize, const B: usize> Appender<'d, ID, N, B> {
    pub fn open(
        path: impl AsRef<path::Path>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self> {
        if N == 0 || B == 0 || B > transfer::MAX_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope::file: invalid append buffer geometry",
            ));
        }

        let mut options = fs::OpenOptions::new();
        options.create(true).write(true);
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        let (file, offset) = file::Locked::acquire(options.open(path)?)?;

        let mut blocks = array::from_fn(|_| Vec::new());
        for block in &mut blocks {
            block.try_reserve_exact(B).map_err(io::Error::other)?;
        }
        let mut free = Ring::empty();
        for index in 0..N {
            free.push(index);
        }

        let mut reservation = routing::Route::reserve_transaction(driver)?;
        let flights = reservation
            .driver()
            .flight_slots::<route::KeyTag<ID, { kind::WRITE }>>(1)?;
        let route = reservation.commit();
        Ok(Self {
            route,
            flights,
            destination: Destination {
                file,
                offset,
                failure: None,
            },
            queue: Queue {
                blocks,
                free,
                pending: Ring::empty(),
                current: None,
                in_flight: None,
                closing: false,
            },
        })
    }

    pub fn record<'a>(
        self: pin::Pin<&'a mut Self>,
        limit: usize,
    ) -> Result<Record<'a, 'd>, WriteError> {
        self.get_mut().begin(limit)
    }

    pub fn failure(&self) -> Option<&io::Error> {
        self.destination.failure.as_ref()
    }

    fn begin(&mut self, limit: usize) -> Result<Record<'_, 'd>, WriteError> {
        if self.queue.closing {
            return Err(WriteError::Closed);
        }
        if self.destination.failure.is_some() {
            return Err(WriteError::Failed);
        }
        if limit > B {
            return Err(WriteError::TooLarge);
        }

        loop {
            let block = match self.queue.current {
                Some(block) => block,
                None => {
                    let Some(block) = self.queue.free.pop() else {
                        return Err(WriteError::Full);
                    };
                    self.queue.current = Some(block);
                    block
                }
            };
            if B - self.queue.blocks[block].len() < limit {
                self.seal();
                continue;
            }
            let block = &mut self.queue.blocks[block];
            let start = block.len();
            return Ok(Record {
                block,
                start,
                limit: start + limit,
                committed: false,
                driver: marker::PhantomData,
            });
        }
    }

    pub(super) fn seal(&mut self) {
        let Some(block) = self.queue.current.take() else {
            return;
        };
        if self.queue.blocks[block].is_empty() {
            self.queue.free.push(block);
        } else {
            self.queue.pending.push(block);
        }
    }

    pub(super) fn prepare(&mut self) -> bool {
        self.seal();
        if self.queue.in_flight.is_none() {
            let Some(block) = self.queue.pending.pop() else {
                return false;
            };
            self.queue.in_flight = Some(InFlight {
                block,
                written: 0,
                flight: None,
            });
        }
        self.queue
            .in_flight
            .as_ref()
            .is_some_and(|flight| flight.flight.is_none())
    }

    pub(super) fn complete_block(&mut self) {
        let Some(in_flight) = self.queue.in_flight.take() else {
            process::abort();
        };
        let Ok(block_len) = u64::try_from(self.queue.blocks[in_flight.block].len()) else {
            self.fail(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "dope::file: append block length exceeds file offsets",
            ));
            return;
        };
        self.destination.offset = match self.destination.offset.checked_add(block_len) {
            Some(offset) => offset,
            None => {
                self.fail(io::Error::new(
                    io::ErrorKind::FileTooLarge,
                    "dope::file: append offset overflow",
                ));
                return;
            }
        };
        self.queue.blocks[in_flight.block].clear();
        self.queue.free.push(in_flight.block);
    }

    pub(super) fn fail(&mut self, error: io::Error) {
        if let Some(in_flight) = self.queue.in_flight.take() {
            if in_flight.flight.is_some() {
                process::abort();
            }
            self.queue.blocks[in_flight.block].clear();
            self.queue.free.push(in_flight.block);
        }
        if let Some(current) = self.queue.current.take() {
            self.queue.blocks[current].clear();
            self.queue.free.push(current);
        }
        while let Some(block) = self.queue.pending.pop() {
            self.queue.blocks[block].clear();
            self.queue.free.push(block);
        }
        self.destination.failure = Some(error);
    }

    pub(super) fn progress(&self) -> State {
        if self.destination.failure.is_some() {
            State::Quiescent
        } else if self.queue.current.is_some()
            || !self.queue.pending.is_empty()
            || self
                .queue
                .in_flight
                .as_ref()
                .is_some_and(|in_flight| in_flight.flight.is_none())
        {
            State::Runnable
        } else if self.queue.in_flight.is_some() {
            State::Waiting
        } else {
            State::Quiescent
        }
    }

    pub(super) fn close(&mut self) {
        self.queue.closing = true;
        self.seal();
    }
}

impl<'step, 'd, const ID: u8, const N: usize, const B: usize> Control<'step, 'd, ID, N, B>
where
    'd: 'step,
{
    pub fn record<'a>(&'a mut self, limit: usize) -> Result<Record<'a, 'd>, WriteError> {
        self.inner.as_mut().get_mut().begin(limit)
    }

    pub fn failure(&self) -> Option<&io::Error> {
        self.inner.as_ref().get_ref().failure()
    }
}

impl Record<'_, '_> {
    pub fn push(&mut self, byte: u8) -> fmt::Result {
        self.extend(&[byte])
    }

    pub fn extend(&mut self, bytes: &[u8]) -> fmt::Result {
        let Some(end) = self.block.len().checked_add(bytes.len()) else {
            return Err(fmt::Error);
        };
        if end > self.limit {
            return Err(fmt::Error);
        }
        self.block.extend_from_slice(bytes);
        Ok(())
    }

    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl fmt::Write for Record<'_, '_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.extend(value.as_bytes())
    }
}

impl Drop for Record<'_, '_> {
    fn drop(&mut self) {
        if !self.committed {
            self.block.truncate(self.start);
        }
    }
}

pub(super) enum State {
    Runnable,
    Waiting,
    Quiescent,
}
