use dope_sys::{
    io::reactor::complete::{self, Drain as _},
    kernel,
    ring::{Access, Complete as _, map, register},
};
use o3::{
    base::{marker::ThreadBound, num::bounded},
    mem::Rejected,
};

use crate::{Register, Ring, reactor::register::Authority};
mod closing;

use std::{mem, num, ops, ptr};

pub(crate) use closing::Closing;
use dope_sys::io;

use crate::{
    error,
    reactor::{buffers, cq, ring, sq},
};

type Unregister = kernel::Errno<register::Unregister>;

/// Failed registration release retains the authority until it can be retried.
pub struct Retained<T>(State<T>);

enum State<T> {
    Registered(Rejected<ring::Release<T>, Unregister>),
    Mapping(ring::Closing<T>),
}

pub struct Pending<T>(Live<T>);

enum Live<T> {
    Registered(ring::Release<T>),
    Mapping(<ring::Closing<T> as io::Retry>::Owner),
}

pub struct Buffer<P, const GROUP: u16> {
    pub(crate) storage: P,
    pub(crate) address: ptr::NonNull<buffers::Entry>,
    pub(crate) entries: num::NonZero<u16>,
}

pub(crate) struct Buffered<P, const GROUP: u16, R> {
    pub(crate) owner: R,
    pub(crate) storage: P,
}

#[derive(Debug)]
pub struct Registered<T> {
    pub(crate) ring: Ring<T>,
    pub(crate) auth: Authority,
    publication: Publication,
}

#[repr(u8)]
#[derive(Debug)]
enum Publication {
    Clean,
    Pending,
}

impl Publication {
    fn publish<T>(self, ring: &mut Ring<T>) {
        if let Self::Pending = self {
            ring.completion().publish();
        }
    }
}

#[must_use = "occupied fixed-file authority must be removed or returned to its owner"]
#[repr(transparent)]
pub struct Occupied {
    slot: sq::Fixed,
    local: ThreadBound,
}

impl Occupied {
    pub(crate) const fn new(slot: sq::Fixed) -> Self {
        Self {
            slot,
            local: ThreadBound::NEW,
        }
    }
}

impl<P, const GROUP: u16> Buffer<P, GROUP> {
    /// # Safety
    /// `storage` must exclusively own `entries` live buffer-ring elements at
    /// `address`; moving `storage` must not move those elements.
    pub(crate) unsafe fn new(
        storage: P,
        address: ptr::NonNull<buffers::Entry>,
        entries: num::NonZero<u16>,
    ) -> Self {
        Self {
            storage,
            address,
            entries,
        }
    }

    pub(crate) fn into_storage(self) -> P {
        self.storage
    }

    pub(crate) fn bind<R>(self, owner: R) -> Buffered<P, GROUP, R> {
        Buffered {
            owner,
            storage: self.storage,
        }
    }
}

impl<P, const GROUP: u16, R> Buffered<P, GROUP, R> {
    pub(crate) fn storage_mut(&mut self) -> &mut P {
        &mut self.storage
    }
}

impl<P, const GROUP: u16, T> Buffered<P, GROUP, Ring<T>> {
    pub(crate) fn split(&mut self) -> (&mut P, &mut Ring<T>) {
        (&mut self.storage, &mut self.owner)
    }
}

impl<P: io::Close, const GROUP: u16, R: io::Close> io::Close for Buffered<P, GROUP, R> {
    type State = Closing<P, GROUP, R::State>;

    fn try_close(self) -> Result<(), Self::State> {
        let Self { owner, storage } = self;
        match owner.try_close() {
            Ok(()) => storage.try_close().map_err(Closing::storage),
            Err(owner) => Err(Closing::ring(Buffered { owner, storage })),
        }
    }
}

impl<T> Registered<T> {
    pub(crate) const fn new(ring: Ring<T>, auth: Authority) -> Self {
        Self {
            ring,
            auth,
            publication: Publication::Clean,
        }
    }
}

impl<T> io::Close for Registered<T> {
    type State = Retained<T>;

    fn try_close(self) -> Result<(), Self::State> {
        let (ring, auth) = self.into_parts();
        ring.into_release(auth).try_close()
    }
}

impl<T> Retained<T> {
    pub(crate) fn registered(owner: ring::Release<T>, error: Unregister) -> Self {
        Self(State::Registered(Rejected::new(owner, error)))
    }

    pub(crate) fn mapping(owner: ring::Closing<T>) -> Self {
        Self(State::Mapping(owner))
    }
}

impl<T> io::Retry for Retained<T> {
    type Failure = map::Storage<kernel::Errno<error::Unmap>>;
    type Owner = Pending<T>;

    fn recover(self) -> Rejected<Self::Owner, Self::Failure> {
        match self.0 {
            State::Registered(failure) => {
                let (owner, cause) = failure.into_parts();
                Rejected::new(Pending(Live::Registered(owner)), cause.into())
            }
            State::Mapping(failure) => {
                let (owner, cause) = io::Retry::recover(failure).into_parts();
                Rejected::new(Pending(Live::Mapping(owner)), cause.into())
            }
        }
    }
}

impl<T> io::Close for Pending<T> {
    type State = Retained<T>;

    fn try_close(self) -> Result<(), Self::State> {
        match self.0 {
            Live::Registered(owner) => owner.try_close(),
            Live::Mapping(owner) => owner.try_close().map_err(Retained::mapping),
        }
    }
}

impl<T> map::Unmap for Retained<T> {
    type Error = kernel::Errno<error::Unmap>;

    fn failure(&self) -> map::Storage<Self::Error> {
        match &self.0 {
            State::Registered(failure) => (*failure.error()).into(),
            State::Mapping(failure) => map::Unmap::failure(failure),
        }
    }
}

impl<T> Registered<T> {
    /// Publishes every consumed CQE before transferring the live registry owner.
    pub(crate) fn into_parts(mut self) -> (Ring<T>, Authority) {
        self.publication.publish(&mut self.ring);
        (self.ring, self.auth)
    }
}

impl<P, const GROUP: u16, R> ops::Deref for Buffered<P, GROUP, R> {
    type Target = R;

    fn deref(&self) -> &Self::Target {
        &self.owner
    }
}

impl<P, const GROUP: u16, R> ops::DerefMut for Buffered<P, GROUP, R> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.owner
    }
}

const _: () = {
    assert!(mem::size_of::<Retained<()>>() == 7 * mem::size_of::<usize>());
    assert!(mem::align_of::<Retained<()>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Buffer<(), 1>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Buffer<(), 1>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Buffered<(), 1, Ring<()>>>() == mem::size_of::<Ring<()>>());
    assert!(mem::align_of::<Buffered<(), 1, Ring<()>>>() == mem::align_of::<Ring<()>>());
    assert!(mem::size_of::<Buffered<(), 1, Registered<()>>>() == mem::size_of::<Registered<()>>());
    assert!(
        mem::size_of::<Buffered<(), 1, ring::Closing<()>>>() == mem::size_of::<ring::Closing<()>>()
    );
    assert!(mem::align_of::<Registered<()>>() == mem::align_of::<Ring<()>>());
    assert!(mem::size_of::<Publication>() == mem::size_of::<bool>());
    assert!(mem::align_of::<Publication>() == mem::align_of::<bool>());
    assert!(
        mem::size_of::<Registered<()>>() <= mem::size_of::<Ring<()>>() + mem::size_of::<usize>()
    );
    assert!(mem::size_of::<Occupied>() == mem::size_of::<sq::Fixed>());
    assert!(mem::align_of::<Occupied>() == mem::align_of::<sq::Fixed>());
};

impl register::Occupied for Occupied {
    type Target = sq::Fixed;
    fn slot(&self) -> sq::Fixed {
        self.slot
    }
}

impl<T> Access for Registered<T> {
    type Token = T;
    type Target = sq::Fixed;
    type File = Occupied;
    type Queue<'a>
        = sq::raw::Queue<'a, T>
    where
        Self: 'a,
        T: complete::raw::Token;
    type Enter<'a>
        = ring::Enter<'a, T>
    where
        Self: 'a;
    type Registry<'a>
        = Register<'a, T>
    where
        Self: 'a;
    fn sq(&mut self) -> sq::raw::Queue<'_, T>
    where
        T: complete::raw::Token,
    {
        self.ring.sq()
    }
    fn enter(&mut self) -> ring::Enter<'_, T> {
        self.ring.enter_for(&self.auth)
    }
    fn registry(&mut self) -> Register<'_, T> {
        self.ring.registry()
    }
}

impl<T> register::Registered for Registered<T> {
    type Entry = cq::Entry<T>;
    fn acquire(&mut self) {
        self.ring.completion().acquire();
    }
    fn pop(&mut self) -> Option<cq::Entry<T>> {
        let entry = self.ring.completion().pop()?;
        self.publication = Publication::Pending;
        Some(entry)
    }
    fn publish(&mut self) {
        mem::replace(&mut self.publication, Publication::Clean).publish(&mut self.ring);
    }
    fn refresh(&mut self) -> bounded::U32<0, { u32::MAX }> {
        self.publish();
        let mut queue = self.ring.completion();
        queue.acquire();
        queue.len()
    }
}
