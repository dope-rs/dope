use std::{cell::Cell, convert::Infallible, num, ops::ControlFlow, task};

use dope_sys::{
    engine::{
        self, admit, life,
        ring::{Backend as _, Context},
    },
    io::{
        Close,
        reactor::{complete, submit::Queue as _},
        request,
        socket::option::tuning::{self, Create as _, Table as _},
    },
    memory::buffer::provided::{self, Context as _},
    probe::Stopped,
    ring::{self, ready},
    table::{Retire as _, closed, reclaim},
};
use dope_uring::Uring;
use o3::alloc;
use ready::Ready as _;

use crate::common::{
    cleanup::{Echo, Ring, Setup, Token},
    receive::Layout,
    ring::try_create,
};

type Ready = <Uring as ring::Source>::Ready<Token, Layout>;
type Storage = <Uring as provided::Source>::Context<Layout>;
type Table = <Uring as tuning::Source>::Table<num::NonZero<u64>, 7>;
type Owner = <Uring as engine::Source<Inputs>>::Backend;
type Producer = <Ready as ready::Ready>::Producer;
type Prepare = ready::Error<
    Ring,
    Producer,
    <Producer as provided::Producer>::Error,
    <Ready as ready::Ready>::Registration,
    <Ready as ready::Ready>::Registered,
    <Ready as ready::Ready>::BufferError,
>;

pub enum Error {
    Tuning(alloc::Error),
    Ring(Setup),
    Memory {
        error: <Storage as provided::Context>::Error,
        ring: Ring,
    },
    Ready {
        error: Prepare,
        storage: Storage,
    },
    Backend {
        error: <Owner as Close>::State,
        storage: Storage,
    },
    Storage(<Storage as Close>::State),
}

pub enum Inputs {}

impl Context for Inputs {
    type Map = Self;
    type Observation = ();
    type Selection = ();
    type Token = Token;
    type Layout = Layout;
    type Tuning = Table;
}

impl complete::Map for Inputs {
    type Entry<T> = T;

    fn map<T, U>(entry: T, map: impl FnOnce(T) -> U) -> U {
        map(entry)
    }
}

impl admit::Context for Inputs {
    type Native = Uring;
    type Inputs = Self;
    type Key = num::NonZero<u64>;
    type Failure = Infallible;
}

impl request::Context for Inputs {
    type Target = [u8; 3];
    type Outbound = [u8; 3];
    type Datagram<'d> = Infallible;

    fn datagram<'d>(fd: &Self::Datagram<'d>) -> Self::Target {
        match *fd {}
    }
}

struct View<'a>(&'a Cell<Vec<Option<u64>>>);

impl reclaim::View for View<'_> {
    type Slot = [u8; 3];
    type Flight = u64;
    type Close = Echo;
    type Retire = Echo;

    fn submitted(&self, flight: u64) {
        let mut observed = self.0.take();
        observed.push(Some(flight));
        self.0.set(observed);
    }

    fn commit(self, _: reclaim::Kind<Echo, Echo>) {
        let mut observed = self.0.take();
        observed.push(None);
        self.0.set(observed);
    }
}

pub fn with(outcome: &mut Result<(), Error>, run: impl FnOnce(&mut Owner)) {
    use ready::raw::Create as _;
    let table = match Table::try_new(num::NonZero::<u32>::MIN.saturating_add(1)) {
        Ok(table) => table,
        Err(error) => return *outcome = Err(Error::Tuning(error)),
    };
    let ring = match try_create::<Token, 16, 32>(false) {
        Ok(ring) => ring,
        Err(error) => return *outcome = Err(Error::Ring(error)),
    };
    let storage = match Storage::new(Layout::fixed::<2, 64>()) {
        Ok(storage) => storage,
        Err(error) => return *outcome = Err(Error::Memory { error, ring }),
    };
    // SAFETY: this exact storage is retained until the registered backend closes,
    // including every failed registration or teardown owner stored in outcome.
    let ready = match unsafe { Ready::new(ring, &storage) } {
        Ok(ready) => ready,
        Err(error) => return *outcome = Err(Error::Ready { error, storage }),
    };
    let mut backend = Owner::new(ready, table);
    run(&mut backend);
    if let Err(error) = backend.try_close() {
        return *outcome = Err(Error::Backend { error, storage });
    }
    *outcome = storage.try_close().map_err(Error::Storage);
}

pub fn occupy(backend: &mut Owner, target: num::NonZero<u64>) -> Option<&mut [libc::c_int; 7]> {
    use tuning::raw::State as _;
    // SAFETY: no tuning SQE is submitted by this fixture; the previous target
    // is cleared before reuse, and the backend keeps this exact pinned array.
    Some(unsafe {
        backend
            .tuning()
            .get(1)?
            .values(target, num::NonZero::<u32>::MIN)
    })
}

pub fn queued(backend: &mut Owner) -> u32 {
    backend.ring().submission().len().get()
}

/// Reserves the one observation allowed by native submit's `FnOnce` callback
/// before retirement can publish any native work.
pub fn try_retire(
    backend: &mut Owner,
    observed: &mut Vec<Option<u64>>,
) -> Result<task::Poll<closed::Submitted>, alloc::Error> {
    alloc::VecExt::try_reserve(observed, 1)?;
    Ok(backend.retire(
        reclaim::Kind::Close(Echo(1)),
        View(Cell::from_mut(observed)),
    ))
}

/// Two terminal censuses exercise each native retirement wait exactly once.
pub struct Retirement<'a> {
    pub owner: &'a mut Owner,
    pub visits: u8,
    pub flight: bool,
    pub reactor: Option<<Owner as engine::raw::Poll>::Error>,
}

impl life::Resolver for Retirement<'_> {
    type Backend = Owner;
    type Fault = Infallible;

    fn backend(&mut self) -> &mut Owner {
        self.owner
    }

    fn retire_one(&mut self) -> life::Step {
        life::Step::Empty
    }

    fn has_pending(&self) -> bool {
        false
    }

    fn has_flights(&self) -> bool {
        self.flight && self.visits == 1
    }

    fn has_inflight(&self) -> bool {
        !self.flight && self.visits == 1
    }
}

impl life::Reclaim for Retirement<'_> {
    fn reclaim(mut self) -> Result<ControlFlow<Self, Self>, Stopped<Self, Infallible>> {
        self.visits += 1;
        Ok(ControlFlow::Break(self))
    }

    fn fault(failure: Stopped<Self, Infallible>) -> Self {
        match failure.cause {}
    }

    fn reactor(&mut self, failure: <Owner as engine::raw::Poll>::Error) {
        self.reactor = Some(failure);
    }
}
