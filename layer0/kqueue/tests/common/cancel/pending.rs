use std::{
    num::{NonZero, NonZeroUsize},
    os::unix::net::UnixStream,
};

use dope_kqueue::Kqueue;
use dope_sys::{
    io::{
        reactor,
        reactor::{
            complete::{self, Queue as _},
            read::{self, Arm as _},
            ready::Poll as _,
        },
    },
    table::{
        self, Files as _, change,
        change::Interest as _,
        registry::{self, Entry as _, State as _, Vacancy as _},
    },
};
use o3::{alloc, depot::map::Plan};

use crate::common::{
    Files, Poll,
    cancel::{Context, Event},
    event::Capacity,
    slots::{
        Slots,
        inputs::{Layout, Missing, Owned, Physical},
    },
    socket,
};

type Entry = <Kqueue as read::Source>::Entry<Context>;
type Registry = <Kqueue as read::Source>::Registry<Context>;
type Arm<'a> = <Kqueue as read::Source>::Arm<'a, Context, Event, Poll>;
type Queue = <Kqueue as complete::Source>::Queue<Event, Capacity>;
type Id = <Entry as registry::Entry>::File;

type Index = <Slots as change::raw::Slots>::Index;

#[derive(Debug)]
pub enum Error {
    Allocation(alloc::Error),
    Socket(socket::Error),
    Install(table::Install<Index>),
    Interest(table::Error<Index, Missing>),
    Registry {
        id: <Entry as registry::Entry>::Id,
        file: Id,
        cause: registry::Failure,
    },
    Poll(reactor::Create),
}

pub struct Pending {
    pub files: Files,
    pub reads: Registry,
    pub poll: Poll,
    pub queue: Queue,
    pub peer: UnixStream,
}

pub fn insert(
    reads: &mut Registry,
    entry: Entry,
) -> Result<NonZeroUsize, (Entry, registry::Failure)> {
    Ok(reads.try_vacant(entry)?.insert())
}

impl Pending {
    pub fn try_new() -> Result<Self, Error> {
        use read::raw::Entry as _;
        let mut files = Files::try_new(Layout).map_err(Error::Allocation)?;
        let mut reads = Registry::try_new(Plan::fixed::<2>()).map_err(Error::Allocation)?;
        let (socket, peer) = socket::try_pair().map_err(Error::Socket)?;
        files
            .install(Owned::from(Physical::at::<1>()), socket)
            .map_err(Error::Install)?;
        let interest = files
            .try_read(Physical::at::<1>())
            .map_err(Error::Interest)?;
        let entry = Entry::recv(interest.id(), NonZeroUsize::MIN);
        let key = insert(&mut reads, entry).map_err(|(entry, cause)| Error::Registry {
            id: entry.id(),
            file: entry.file(),
            cause,
        })?;
        interest.arm(libc::EV_ADD | libc::EV_DISPATCH, key);
        Ok(Self {
            files,
            reads,
            peer,
            poll: Poll::try_new().map_err(Error::Poll)?,
            queue: Queue::try_new(Capacity(NonZero::<u32>::MIN)).map_err(Error::Allocation)?,
        })
    }

    pub fn arm(&mut self) -> Arm<'_> {
        Arm::new(&mut self.files, &mut self.reads, &mut self.poll)
    }

    pub fn is_pending(&self) -> bool {
        self.reads
            .find(NonZeroUsize::MIN, read::Family::Recv)
            .is_some()
            && self.files.borrow(Physical::at::<1>()).is_some()
            && self.queue.is_empty()
    }
}
