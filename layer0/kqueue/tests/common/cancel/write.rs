use std::{
    num::{NonZero, NonZeroUsize},
    os::{
        fd::{AsFd as _, AsRawFd as _, RawFd},
        unix::net::UnixStream,
    },
};

use dope_kqueue::Kqueue;
use dope_sys::{
    Errno,
    io::reactor::{
        self,
        complete::{self, Queue as _},
        ready::Poll as _,
        retry::{self, State as _},
    },
    memory,
    table::{self, Files as _, change},
};
use o3::{alloc, buffer::bytes, depot::map::Plan};

use crate::common::{
    Files, Poll,
    cancel::{Context, Extent},
    event::Capacity,
    slots::{
        Slots,
        inputs::{Layout, Owned, Physical},
    },
    socket,
};

type Length = <Extent as memory::raw::Extent>::Length;
type Index = <Slots as change::raw::Slots>::Index;

type Pending = <Kqueue as retry::Source>::Pending<Context>;
type State = <Kqueue as retry::Source>::State<Pending>;
type Retry<'a> = <Kqueue as retry::Source>::Retry<'a, Context, Write, Poll>;
type Data = <Kqueue as retry::Source>::Send<Context>;
type Queue = <Kqueue as complete::Source>::Queue<Write, Capacity>;

static PAYLOAD: [u8; 4096] = [43; 4096];

#[derive(Debug)]
pub enum Error {
    Allocation(alloc::Error),
    Socket(socket::Error),
    Install(table::Install<Index>),
    Poll(reactor::Create),
    Vacancy,
}

pub struct Blocked {
    pub files: Files,
    pub state: State,
    pub poll: Poll,
    pub queue: Queue,
    pub peer: UnixStream,
    pub raw: RawFd,
    pub buffered: NonZeroUsize,
}

impl Blocked {
    pub fn try_new() -> Result<Self, Error> {
        use retry::raw::{Send as _, Write as _};

        let mut files = Files::try_new(Layout).map_err(Error::Allocation)?;
        let mut state = State::try_new(Plan::fixed::<2>()).map_err(Error::Allocation)?;
        let mut poll = Poll::try_new().map_err(Error::Poll)?;
        let mut queue = Queue::try_new(Capacity(NonZero::<u32>::MIN)).map_err(Error::Allocation)?;
        let (socket, peer, buffered) = socket::try_blocked().map_err(Error::Socket)?;
        let raw = socket.as_fd().as_raw_fd();
        files
            .install(Owned::from(Physical::at::<1>()), socket)
            .map_err(Error::Install)?;
        let vacant = queue.vacant().ok_or(Error::Vacancy)?;
        // SAFETY: the retained pending write points to immutable static storage.
        unsafe {
            Retry::new(&mut files, &mut state, &mut poll).send(
                NonZeroUsize::MIN,
                Physical::at::<1>(),
                || Data::bytes(bytes::Prefix::new(&PAYLOAD)),
                vacant,
            );
        }
        Ok(Self {
            files,
            state,
            poll,
            queue,
            peer,
            raw,
            buffered,
        })
    }
}

#[derive(Debug)]
pub enum Outcome {
    Sent(Result<Length, Errno>),
    Connected(Result<(), Errno>),
}

#[derive(Debug)]
pub struct Write {
    pub key: NonZeroUsize,
    pub result: Outcome,
}

impl complete::Event for Write {
    type Key = NonZeroUsize;
    fn token(&self) -> Option<NonZeroUsize> {
        Some(self.key)
    }
    fn is_terminal(&self) -> bool {
        true
    }
}

impl retry::Complete<Context> for Write {
    fn sent(key: NonZeroUsize, result: Result<Length, Errno>) -> Self {
        Self {
            key,
            result: Outcome::Sent(result),
        }
    }
    fn connected(key: NonZeroUsize, result: Result<(), Errno>) -> Self {
        Self {
            key,
            result: Outcome::Connected(result),
        }
    }
}
