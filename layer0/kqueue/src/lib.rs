use std::{
    convert, iter, num,
    os::fd::{AsFd, BorrowedFd, OwnedFd},
};

use dope_sys::{
    Cloexec, Errno, Nonblock, engine,
    engine::{admit, life},
    io::{
        fd::{self, Fd as _},
        file::File,
        pipe,
        reactor::{
            complete,
            complete::Observe,
            read, ready,
            ready::{
                Backend as _, delivery,
                delivery::entry,
                store::{Context, Create, Quota, config},
            },
            retry,
        },
        request, socket,
        socket::{
            accept, addr, gso, option,
            option::{Name, fast, tuning::device},
            record, route,
            route::install,
        },
    },
    kernel,
    kernel::errno,
    memory,
    memory::buffer,
    probe::Stopped,
    table,
    table::permit,
    thread::{Thread, affinity},
};
use o3::{base::num::bounded, buffer::bytes::bounded::MAX, depot};

use crate::{
    file::Options,
    output::{Data, Pending, Retry, State},
    reactor::{
        Engine, Poll, Trace, input,
        input::{Arm, Slot},
    },
    sockets::{Handle, Operation},
    store::{buffer::Pool, done, permit::Queue, registry::Registry, table::Files},
    system::signal,
};

mod command;
pub(crate) use command::Command;
mod sockets;

mod file;

mod reactor;

mod output;

mod system;

mod store;

#[derive(Debug)]
pub struct Kqueue;

impl Kqueue {
    /// Fresh native descriptors need only the close-on-exec assignment.
    fn cloexec(file: BorrowedFd<'_>) -> Result<(), Cloexec> {
        Self::set_retry::<{ libc::F_SETFD }, Errno>(file, libc::FD_CLOEXEC).map_err(Cloexec::Set)
    }

    /// Suppresses `SIGPIPE` for one live socket end, retrying the interrupted
    /// assignment before surfacing its exact failure.
    fn nosigpipe(file: BorrowedFd<'_>) -> Result<(), Errno> {
        loop {
            match <Self as socket::Socket>::set_option::<Errno>(
                file,
                libc::SOL_SOCKET,
                libc::SO_NOSIGPIPE,
                &1i32,
            ) {
                Ok(()) => return Ok(()),
                Err(error) if error.is_intr() => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

impl accept::Source for Kqueue {
    type Failure<F: From<Errno>> = F;

    const MAX_IN_FLIGHT: num::NonZero<usize> = num::NonZero::<usize>::MIN;

    fn is_retryable(error: Errno) -> bool {
        error.is_block() || matches!(error.code().get(), libc::EINTR | libc::ECONNABORTED)
    }

    fn can_defer(error: Errno) -> bool {
        matches!(error.code().get(), libc::EMFILE | libc::ENFILE)
    }
}

impl<L: Copy + Eq> buffer::Selection<L> for Kqueue {
    type Error = convert::Infallible;
}

impl<C: ready::Driver> engine::Source<C> for Kqueue {
    type Map = complete::Dequeued<C::Map>;
    type Backend = reactor::Engine<C>;
    type Closing = convert::Infallible;
}

impl<C, P: Copy, R: Copy> engine::Storage<C, P, R> for Kqueue {
    type Cleanup = convert::Infallible;
    type Release = convert::Infallible;
}

impl engine::Removal for Kqueue {
    type Slot = convert::Infallible;
}

impl<'a, C, H> engine::Dispatch<'a, C, H> for Kqueue
where
    C: ready::Driver<Context: ready::raw::Decode + 'a>
        + delivery::Context<H, Backend = reactor::Engine<C>>,
    H: 'a,
{
    type Turn<Q: delivery::Quota<H>> = reactor::Dispatch<'a, H, Q>;
}

impl route::Source for Kqueue {
    type Install = convert::Infallible;

    fn try_prepare(plan: route::Plan) -> Result<Self::Install, install::Failure> {
        Err(install::Failure::Unsupported(plan))
    }
}

impl gso::Source for Kqueue {
    type Mode = convert::Infallible;

    const LIMITS: Option<(
        bounded::U32<1, { MAX as u32 }>,
        bounded::U32<2, { u16::MAX as u32 }>,
    )> = None;
}

impl<T: Copy + Eq, S: Copy + Eq, F: Copy + Eq> Observe<T, S, F> for Kqueue {
    type Observation = Trace<T, S, F>;
}

impl complete::Source for Kqueue {
    type Queue<E: complete::Event, C: complete::raw::Capacity> = done::Queue<E, C>;
}

impl kernel::Source for Kqueue {
    type Code = convert::Infallible;
    type Failure<S: Copy + Eq> = kernel::Diagnostic<S>;
    type Returned = convert::Infallible;
}

impl Thread for Kqueue {
    type Timer = convert::Infallible;
    type Receiver = signal::raw::Receiver;
    type Cpus = iter::Empty<affinity::raw::Cpu>;

    fn is_main() -> bool {
        // SAFETY: pthread_main_np only queries the caller's native thread.
        unsafe { libc::pthread_main_np() != 0 }
    }

    fn current() -> Result<Self::Cpus, affinity::Error> {
        Err(affinity::Error::DiscoveryUnsupported)
    }

    fn bind(cpu: affinity::raw::Cpu) -> Result<(), affinity::Error> {
        Err(affinity::Error::BindingUnsupported { cpu })
    }
}

impl errno::Source for Kqueue {
    fn clear() {
        // SAFETY: this native TLS cell is written synchronously on its owning thread.
        unsafe { libc::__error().write(0) };
    }
}

impl pipe::Pipe for Kqueue {
    fn open() -> Result<(OwnedFd, OwnedFd), pipe::Error> {
        let mut fds = [0; 2];
        // SAFETY: `fds` is writable storage for exactly two descriptors.
        let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        // SAFETY: pipe just returned on this thread and `fds` is its exact destination.
        let (read, write) = unsafe {
            fd::raw::Pair::new(fds)
                .try_adopt::<Errno>(rc, || kernel::raw::Code::last::<Self>().into())
        }
        .map_err(pipe::Error::Open)?;
        for (file, end) in [(&read, pipe::End::Read), (&write, pipe::End::Write)] {
            Self::cloexec(AsFd::as_fd(file))
                .map_err(|failure| pipe::Error::CloseOnExec { end, failure })?;
        }
        Ok((read, write))
    }

    /// Fresh ends have only received descriptor flags, not status flags.
    fn nonblocking() -> Result<(OwnedFd, OwnedFd), pipe::Error> {
        let pipe = <Self as pipe::Pipe>::open()?;
        for (file, end) in [(&pipe.0, pipe::End::Read), (&pipe.1, pipe::End::Write)] {
            Self::set_retry::<{ libc::F_SETFL }, Errno>(AsFd::as_fd(file), libc::O_NONBLOCK)
                .map_err(Nonblock::Set)
                .map_err(|failure| pipe::Error::Nonblocking { end, failure })?;
        }
        Ok(pipe)
    }
}

impl File for Kqueue {
    type Options = Options;
    type Request<'a> = file::Request<'a>;
}

impl permit::Source for Kqueue {
    type Queue<K: permit::raw::Key, I: depot::raw::Compact> = Queue<K, I>;
}

impl buffer::Source for Kqueue {
    type Pool<L: memory::raw::Layout> = Pool<L>;
}

impl table::Source for Kqueue {
    type Files<S: table::raw::Slots> = Files<S>;
}

impl socket::Socket for Kqueue {
    const MAX_IOVECS: usize = libc::IOV_MAX as usize;
    type Error = socket::Failure;
    type Records = record::Stream;

    fn socket(domain: libc::c_int, kind: libc::c_int) -> Result<OwnedFd, Self::Error> {
        let file = Self::try_open::<Self::Error>(domain, kind, 0)?;
        Kqueue::cloexec(file.as_fd()).map_err(Self::Error::from)?;
        Ok(file)
    }

    fn pair() -> Result<(OwnedFd, OwnedFd), record::Error> {
        let (left, right) =
            Self::try_pair::<Errno>(libc::SOCK_STREAM).map_err(record::Error::Open)?;
        for (file, end) in [
            (left.as_fd(), record::End::Left),
            (right.as_fd(), record::End::Right),
        ] {
            Kqueue::cloexec(file).map_err(|error| record::Error::Cloexec { end, error })?;
            Kqueue::nosigpipe(file).map_err(|error| record::Error::Sigpipe { end, error })?;
        }
        Ok((left, right))
    }
}

impl option::Source for Kqueue {
    const KEEP_IDLE: Name = Name::new(libc::TCP_KEEPALIVE);
}

impl addr::Source for Kqueue {
    const UNIX_TERMINATED: bool = true;
    const UNIX_ABSTRACT: bool = false;

    fn header(domain: socket::Domain, len: addr::Socklen) -> [u8; 2] {
        [len.get() as u8, domain.raw() as u8]
    }
}

impl socket::Prepare for Kqueue {
    type Handle = Handle;
    type Fast = convert::Infallible;

    fn fast_open(depth: Option<num::NonZero<i32>>) -> Result<Option<Self::Fast>, fast::Error> {
        match depth {
            Some(depth) => Err(fast::Error(depth)),
            None => Ok(None),
        }
    }

    fn open(domain: socket::Domain, kind: socket::Kind) -> Result<Self::Handle, socket::Create> {
        <Handle as socket::raw::Open>::open(domain, kind)
    }

    fn listener(domain: socket::Domain) -> Result<Self::Handle, socket::Create> {
        <Handle as socket::raw::Open>::open(domain, socket::Kind::Stream)
    }

    fn adopt(file: OwnedFd) -> Result<Self::Handle, socket::Setup> {
        <Handle as socket::raw::Open>::adopt(file)
    }

    fn into_fd(handle: Self::Handle) -> OwnedFd {
        handle.0
    }

    fn set_fast(_: BorrowedFd<'_>, fast: Self::Fast) -> Result<(), Errno> {
        match fast {}
    }
}

impl ready::Source for Kqueue {
    const LANES: num::NonZero<u8> = num::NonZero::<u8>::MIN.saturating_add(2);

    type Work<C, Q>
        = reactor::Work<Q>
    where
        C: ready::Driver<Context: ready::raw::Decode>,
        Q: Quota;
    type State<C: Context> = reactor::State<C>;
    type Poll<S: table::raw::Slots, F: ready::Failure<Index = S::Index, Missing = S::Missing>> =
        Poll<S, F, { Self::LANES.get() }>;
}

impl retry::Source for Kqueue {
    type State<E: retry::Entry> = State<E>;
    type Send<C: retry::Context> = Data<C>;
    type Pending<C: retry::Context> = Pending<C>;
    type Retry<'a, C, E, P>
        = Retry<'a, C, E, P>
    where
        C: retry::Context + 'a,
        C::Slots: 'a,
        E: retry::Complete<C>,
        P: ready::Poll<
                C::Slots,
                Files = Self::Files<C::Slots>,
                Failure: ready::Failure<Operation = C::Operation>,
            > + 'a,
        Self::Files<C::Slots>: 'a,
        Self::State<Self::Pending<C>>: 'a;
}

impl read::Source for Kqueue {
    type Registry<C: read::Context> = Registry<C>;
    type Entry<C: read::Context> = Slot<C>;
    type Arm<'a, C, E, P>
        = Arm<'a, C, E, P>
    where
        C: read::Context + 'a,
        C::Slots: 'a,
        E: read::Complete<Key = C::Key, Flow = C::Flow>,
        P: ready::Poll<
                C::Slots,
                Files = Self::Files<C::Slots>,
                Failure: ready::Failure<Operation = C::Operation>,
            > + 'a,
        Self::Files<C::Slots>: 'a,
        Self::Registry<C>: 'a;
    type Poll<'a, C, L, E, P>
        = input::Poll<'a, C, L, E, P>
    where
        C: read::Context + 'a,
        C::Slots: 'a,
        L: memory::raw::Layout + 'a,
        E: read::Complete<
                Key = C::Key,
                Flow = C::Flow,
                Accepted = <C::Slots as table::raw::Slots>::Accepted,
                Buffer = <Self::Pool<L> as buffer::Pool>::Buffer,
            >,
        P: ready::Poll<
                C::Slots,
                Files = Self::Files<C::Slots>,
                Failure: ready::Failure<Operation = C::Operation>,
            > + 'a,
        Self::Files<C::Slots>: 'a,
        Self::Registry<C>: 'a,
        Self::Pool<L>: 'a;
}

impl<C: request::Context> request::Source<C> for Kqueue {
    type Raw = Command<
        command::Lowered<C::Target, C::Outbound, socket::Domain, command::Message<C::Target>>,
    >;
    const GROUP: u16 = 0;

    fn target(slot: C::Target) -> C::Target {
        slot
    }

    fn outbound(slot: C::Outbound) -> C::Outbound {
        slot
    }

    fn datagram<'d>(fd: &C::Datagram<'d>) -> command::Message<C::Target> {
        command::Message::new(C::datagram(fd))
    }
}

impl entry::Source for Kqueue {
    type Event<C: entry::Context, B: dope_sys::Buffer> = reactor::Event<C, B>;
}

impl device::Source for Kqueue {
    type Socket<H> = Operation<H>;
}

impl admit::Native for Kqueue {
    type Error<F> = admit::Cause<F>;
}

/// The native reactor has stopped accessing this resolver's request owners.
pub struct Quiesced<R: life::Reclaim> {
    resolver: R,
}

impl<R: life::Reclaim> life::Quiesced for Quiesced<R> {
    type Resolver = R;

    fn into_inner(self) -> R {
        self.resolver
    }
}

impl<'a, C, R> life::Finish<'a, R> for Kqueue
where
    C: ready::Driver<Context: ready::raw::Decode + 'a>,
    <Engine<C> as ready::Backend<C>>::Failure: Copy + Eq,
    R: life::Reclaim<Backend = Engine<C>> + 'a,
{
    type Quiesced = Quiesced<R>;

    fn try_finish(
        mut resolver: R,
    ) -> Result<
        Self::Quiesced,
        Stopped<R, life::Error<<Engine<C> as engine::raw::Poll>::Error, R::Fault>>,
    > {
        use life::Error;

        let quiesce = match resolver.backend().quiesce() {
            Ok(()) => Ok(()),
            Err(ready::Failures { first, rest }) => {
                resolver.reactor(first);
                for failure in rest.into_iter().flatten() {
                    resolver.reactor(failure);
                }
                Err(first)
            }
        };
        let (mut resolver, first) = match resolver.drain() {
            Ok(resolver) => (resolver, None),
            Err(Stopped { state, cause }) => (state, Some(cause)),
        };
        let cause = match (quiesce, first) {
            (Ok(()), None) => {
                resolver.retire();
                return Ok(Quiesced { resolver });
            }
            (Ok(()), Some(fault)) => Error::Completion(fault),
            (Err(cause), done) => Error::Reactor {
                cause,
                completion: done,
            },
        };
        Err(Stopped {
            state: resolver,
            cause,
        })
    }
}

impl<'a, C, R> life::Quiesce<'a, R> for Kqueue
where
    C: ready::Driver<Context: ready::raw::Decode + 'a>,
    <Engine<C> as ready::Backend<C>>::Failure: Copy + Eq,
    R: life::Cancel<Backend = Engine<C>> + 'a,
    R::Cancelling: 'a,
    R::Closing: 'a,
{
    type Quiesced = Quiesced<R::Closing>;

    fn try_quiesce(
        resolver: R,
    ) -> Result<
        Self::Quiesced,
        Stopped<R::Closing, life::Error<<Engine<C> as engine::raw::Poll>::Error, R::Fault>>,
    > {
        use life::Close as _;

        let resolver = resolver.cancel().into_closing();
        <Self as life::Finish<'a, R::Closing>>::try_finish(resolver)
    }
}

mod raw;
