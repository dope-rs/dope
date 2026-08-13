use std::{
    array, cell, error, fmt, fs, io, marker, os::unix::fs::OpenOptionsExt as _, path, process,
};

use dope_core::{
    driver::{
        flight,
        lifecycle::routing,
        route::{self, kind},
        schedule::ready::completion,
        storage,
    },
    io::transfer,
};

use crate::file;

mod state;

pub(super) use state::{Destination, Inner, Phase};

type WriteTag<const ID: u8> = route::KeyTag<ID, { kind::WRITE }>;
type SyncTag<const ID: u8> = route::KeyTag<ID, { kind::SYNC }>;

fn persist_parent(path: &path::Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| path::Path::new("."));
    fs::File::open(parent)?.sync_all()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure {
    Os(i32),
    WriteZero,
    FileTooLarge,
}

impl Failure {
    pub fn into_io_error(self) -> io::Error {
        match self {
            Self::Os(errno) => io::Error::from_raw_os_error(errno),
            Self::WriteZero => io::Error::from(io::ErrorKind::WriteZero),
            Self::FileTooLarge => io::Error::new(
                io::ErrorKind::FileTooLarge,
                "dope::file: durable append offset overflow",
            ),
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.into_io_error().fmt(f)
    }
}

impl error::Error for Failure {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendError {
    Full,
    Empty,
    TooLarge,
    Failed(Failure),
    Closed,
}

impl fmt::Display for AppendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => f.write_str("durable append capacity is full"),
            Self::Empty => f.write_str("durable append record is empty"),
            Self::TooLarge => f.write_str("durable append record is too large"),
            Self::Failed(error) => write!(f, "durable appender failed: {error}"),
            Self::Closed => f.write_str("durable appender is closed"),
        }
    }
}

impl error::Error for AppendError {}

pub enum CommitOutcome {
    Pending,
    Done(Result<(), Failure>),
    Expired,
}

#[must_use = "a durable ticket must be polled to completion or abandoned"]
pub struct Ticket<'d, const ID: u8> {
    pub(super) slot: usize,
    pub(super) generation: u32,
    pub(super) driver: marker::PhantomData<fn(&'d route::KeyTag<ID>) -> &'d route::KeyTag<ID>>,
}

pub struct Factory<const ID: u8, const N: usize, const B: usize> {
    destination: Destination,
    blocks: [state::Block; N],
    commit_capacity: usize,
}

pub struct Appender<'d, const ID: u8, const N: usize, const B: usize> {
    pub(super) route: routing::StorageRoute<'d, ID>,
    pub(super) writes: flight::Slots<'d, WriteTag<ID>>,
    pub(super) syncs: flight::Slots<'d, SyncTag<ID>>,
    pub(super) inner: cell::RefCell<Inner<'d, N>>,
}

pub struct Manifold<'d, const ID: u8, const N: usize, const B: usize> {
    pub(super) appender: &'d Appender<'d, ID, N, B>,
}

impl<const ID: u8, const N: usize, const B: usize> Factory<ID, N, B> {
    pub fn open(path: impl AsRef<path::Path>, commit_capacity: usize) -> io::Result<Self> {
        Self::open_with(path, commit_capacity, |_| Ok(()))
    }

    pub fn open_with(
        path: impl AsRef<path::Path>,
        commit_capacity: usize,
        initialize: impl FnOnce(&mut fs::File) -> io::Result<()>,
    ) -> io::Result<Self> {
        if N == 0 || B == 0 || B > transfer::MAX_BYTES || commit_capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope::file: invalid durable append geometry",
            ));
        }
        let path = path.as_ref();
        let mut options = fs::OpenOptions::new();
        options.create(true).truncate(false).read(true).write(true);
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        let (mut file, _) = file::Locked::acquire(options.open(path)?)?;
        initialize(file.file_mut())?;
        let offset = file.file_mut().metadata()?.len();

        persist_parent(path)?;

        let mut blocks = array::from_fn(|_| state::Block {
            bytes: Vec::new(),
            first: None,
            last: None,
        });
        for block in &mut blocks {
            *block = state::Block::with_capacity(B)?;
        }
        Ok(Self {
            destination: Destination { file, offset },
            blocks,
            commit_capacity,
        })
    }
}

impl<const ID: u8, const N: usize, const B: usize> storage::Factory for Factory<ID, N, B> {
    type Output<'d> = Appender<'d, ID, N, B>;
    type Error = io::Error;

    fn build<'d>(
        self,
        context: &mut storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'d>, Self::Error> {
        let route = context.reserve_route::<ID>()?.bind_storage();
        let writes = context.flight_slots::<WriteTag<ID>>(1)?;
        let syncs = context.flight_slots::<SyncTag<ID>>(1)?;
        let mut free = state::Ring::empty();
        for index in 0..N {
            free.push(index);
        }
        let waiters = (0..self.commit_capacity)
            .map(|_| state::WaitSlot::free())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let free_waiters = (0..self.commit_capacity).rev().collect();
        Ok(Appender {
            route,
            writes,
            syncs,
            inner: cell::RefCell::new(Inner {
                destination: self.destination,
                queue: state::Queue {
                    blocks: self.blocks,
                    free,
                    pending: state::Ring::empty(),
                    current: None,
                    in_flight: None,
                    closing: false,
                },
                waiters,
                free_waiters,
                failure: None,
                capacity_wake: completion::Slot::empty(),
            }),
        })
    }
}

impl<'d, const ID: u8, const N: usize, const B: usize> Appender<'d, ID, N, B> {
    pub fn manifold(&'d self) -> Manifold<'d, ID, N, B> {
        Manifold { appender: self }
    }

    pub fn try_append(&'d self, bytes: &[u8]) -> Result<Ticket<'d, ID>, AppendError> {
        if bytes.is_empty() {
            return Err(AppendError::Empty);
        }
        if bytes.len() > B {
            return Err(AppendError::TooLarge);
        }
        let mut inner = self.inner.borrow_mut();
        if inner.queue.closing {
            return Err(AppendError::Closed);
        }
        if let Some(failure) = inner.failure {
            return Err(AppendError::Failed(failure));
        }
        let Some(waiter_index) = inner.free_waiters.pop() else {
            return Err(AppendError::Full);
        };

        let block_index = loop {
            let index = match inner.queue.current {
                Some(index) => index,
                None => {
                    let Some(index) = inner.queue.free.pop() else {
                        inner.free_waiters.push(waiter_index);
                        return Err(AppendError::Full);
                    };
                    inner.queue.current = Some(index);
                    index
                }
            };
            if B - inner.queue.blocks[index].bytes.len() < bytes.len() {
                inner.seal();
                continue;
            }
            break index;
        };

        let generation = inner.waiters[waiter_index]
            .generation
            .wrapping_add(1)
            .max(1);
        inner.waiters[waiter_index].generation = generation;
        inner.waiters[waiter_index].state = state::WaitState::Pending { next: None };
        inner.waiters[waiter_index].wake.clear();

        let previous = inner.queue.blocks[block_index].last;
        if let Some(previous) = previous {
            let Some(next) = inner.waiters[previous].next_mut() else {
                process::abort();
            };
            *next = Some(waiter_index);
        } else {
            inner.queue.blocks[block_index].first = Some(waiter_index);
        }
        inner.queue.blocks[block_index].last = Some(waiter_index);
        inner.queue.blocks[block_index]
            .bytes
            .extend_from_slice(bytes);
        Ok(Ticket {
            slot: waiter_index,
            generation,
            driver: marker::PhantomData,
        })
    }

    pub fn poll_commit(
        &self,
        ticket: &mut Ticket<'d, ID>,
        wake: completion::Waker<'d>,
    ) -> CommitOutcome {
        if ticket.generation == 0 {
            return CommitOutcome::Expired;
        }
        let mut inner = self.inner.borrow_mut();
        let Some(slot) = inner.waiters.get_mut(ticket.slot) else {
            return CommitOutcome::Expired;
        };
        if slot.generation != ticket.generation {
            return CommitOutcome::Expired;
        }
        match slot.state {
            state::WaitState::Pending { .. } => {
                slot.wake.set(wake);
                CommitOutcome::Pending
            }
            state::WaitState::Done(result) => {
                slot.state = state::WaitState::Free;
                slot.wake.clear();
                inner.free_waiters.push(ticket.slot);
                inner.capacity_wake.wake();
                ticket.generation = 0;
                CommitOutcome::Done(result)
            }
            state::WaitState::Free | state::WaitState::Abandoned { .. } => CommitOutcome::Expired,
        }
    }

    pub fn abandon(&self, ticket: Ticket<'d, ID>) {
        let mut inner = self.inner.borrow_mut();
        let Some(slot) = inner.waiters.get_mut(ticket.slot) else {
            return;
        };
        if slot.generation != ticket.generation {
            return;
        }
        slot.wake.clear();
        match slot.state {
            state::WaitState::Pending { next } => slot.state = state::WaitState::Abandoned { next },
            state::WaitState::Done(_) => {
                slot.state = state::WaitState::Free;
                inner.free_waiters.push(ticket.slot);
                inner.capacity_wake.wake();
            }
            state::WaitState::Free | state::WaitState::Abandoned { .. } => {}
        }
    }

    pub fn failure(&self) -> Option<Failure> {
        self.inner.borrow().failure
    }

    pub fn register_capacity(&self, wake: completion::Waker<'d>) {
        self.inner.borrow().capacity_wake.set(wake);
    }
}
