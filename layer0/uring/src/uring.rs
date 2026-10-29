use std::{
    alloc::Layout,
    os::fd::{AsFd, BorrowedFd, OwnedFd},
    ptr,
};

use dope_sys::{
    Cloexec, Nonblock,
    engine::{life, life::Error, ring::Backend as _},
    io::{
        fd::{self, Fd},
        socket::{
            Setup, Socket as _, addr,
            option::{Name, tuning::Quiesce as _},
        },
    },
    probe::Stopped,
    ring::ready::Ready as _,
    thread::Thread,
};
use o3::depot;

use crate::{
    Accepted, Borrowed, Canary, Cleanup, Closing, Context, Control, Cpus, Encoded, Engine, Enter,
    Entry, Errno, Fault, Fifo, Infallible, Installer, MAX, Observe, Operation, Outcome, Payload,
    Poll, Primary, Producer, Range, Ready, Recover, Released, Resources, Ring, Selected, Session,
    Source, Table, Timer, Turn, Waiting, accept, admit, affinity, bounded, buffer, buffers,
    complete, cq, create, delivery,
    driver::{Quiesced, tuning::Progress},
    engine, enter, errno, error, fast, file, gso, host, install, kernel, mem, memory, num, op,
    option, packet, permit, pipe, probe, provided, record, ring, route, socket, sq, submit,
    system::signal,
    token, tuning, types,
};

#[derive(Debug)]
pub struct Uring;

impl Uring {
    const ALIGNMENT: num::NonZero<usize> = num::NonZero::<usize>::MIN.saturating_add(4095);

    fn open_with(flags: libc::c_int) -> Result<(OwnedFd, OwnedFd), pipe::Error> {
        let mut fds = [0; 2];
        // SAFETY: fds names writable storage for exactly two descriptors.
        let result = unsafe { libc::pipe2(fds.as_mut_ptr(), flags) };
        // SAFETY: pipe2 just returned on this thread and `fds` is its exact destination.
        unsafe {
            fd::raw::Pair::new(fds)
                .try_adopt::<Errno>(result, || kernel::raw::Code::last::<Self>().into())
        }
        .map_err(pipe::Error::Open)
    }

    pub fn layout<T>(count: usize) -> Result<num::NonZero<usize>, memory::Error> {
        let invalid = || memory::Error::Length {
            element: mem::size_of::<T>(),
            count,
        };
        let layout = Layout::array::<T>(count).map_err(|_| invalid())?;
        let bytes = num::NonZero::new(layout.size()).ok_or_else(invalid)?;
        let element = layout.align();
        let limit = Self::ALIGNMENT.get();
        if element > limit {
            return Err(memory::Error::Alignment {
                element,
                supported: limit,
            });
        }
        Ok(bytes)
    }

    /// # Safety
    /// `raw` is the unchanged return of a syscall whose success fits `0..=i32::MAX`.
    pub(crate) unsafe fn status(raw: libc::c_long) -> kernel::raw::Status<-1, { i32::MAX }> {
        const { assert!(mem::size_of::<libc::c_long>() == mem::size_of::<libc::ssize_t>()) };
        // SAFETY: the caller retains this syscall's nonnegative c_int success
        // domain or native -1; the size assertion preserves that exact scalar.
        unsafe { kernel::raw::Status::from_count(raw as libc::ssize_t) }
    }

    /// Transfers an anonymous writable mapping, including a successful null address.
    pub(crate) fn try_map<const FLAGS: libc::c_int>(
        bytes: num::NonZero<usize>,
    ) -> Result<*mut libc::c_void, Errno> {
        const { assert!(FLAGS & !libc::MAP_NORESERVE == 0) };
        // SAFETY: mmap validates this nonempty anonymous mapping request.
        let address = unsafe {
            libc::mmap(
                ptr::null_mut(),
                bytes.get(),
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | FLAGS,
                -1,
                0,
            )
        };
        if address == libc::MAP_FAILED {
            // SAFETY: mmap just failed on this thread without an intervening call.
            Err(unsafe { kernel::raw::Code::last::<Self>().into() })
        } else {
            Ok(address)
        }
    }

    /// # Safety
    /// `address` and `bytes` name one complete live mapping owned by the caller.
    /// Success consumes the mapping; failure leaves ownership with the caller.
    pub(crate) unsafe fn try_unmap<E: From<Errno>>(
        address: *mut libc::c_void,
        bytes: num::NonZero<usize>,
    ) -> Result<(), E> {
        // SAFETY: the caller owns this complete live mapping.
        let result = unsafe { libc::munmap(address, bytes.get()) };
        // SAFETY: munmap returns zero or -1; failure retains its unchanged errno.
        unsafe {
            kernel::raw::Status::new(result).try_zero(|| kernel::raw::Code::last::<Self>().into())
        }
    }
}

impl accept::Source for Uring {
    type Failure<F: From<Errno>> = Errno;

    const MAX_IN_FLIGHT: num::NonZero<usize> = num::NonZero::<usize>::MAX;

    fn is_retryable(error: Errno) -> bool {
        error.is_block()
            || matches!(
                error.code().get(),
                libc::EINTR
                    | libc::ECONNABORTED
                    | libc::ENETDOWN
                    | libc::EPROTO
                    | libc::ENOPROTOOPT
                    | libc::EHOSTDOWN
                    | libc::ENONET
                    | libc::EHOSTUNREACH
                    | libc::EOPNOTSUPP
                    | libc::ENETUNREACH
            )
    }

    fn can_defer(error: Errno) -> bool {
        error.code().get() == libc::ENFILE
    }
}

impl<L: Copy + Eq> buffer::Selection<L> for Uring {
    type Error = (<Self as provided::Source>::Index, L);
}

impl<C: Context> engine::Source<C> for Uring {
    type Map = C::Map;
    type Backend = Engine<C>;
    type Closing = Closing<C>;
}

impl<C, P, R> engine::Storage<C, P, R> for Uring
where
    C: Context<Layout: memory::raw::Canary>,
    P: Copy + From<Primary> + From<memory::Error>,
    R: Copy,
{
    type Cleanup = <Self as create::Source>::Resources<C::Token, C::Layout, P>;
    type Release = <Self as probe::Source>::Release<C::Token, C::Layout, R>;
}

impl engine::Removal for Uring {
    type Slot = <Self as submit::Control>::Target;
}

impl<'a, C, H> engine::Dispatch<'a, C, H> for Uring
where
    C: Context + delivery::Context<H, Backend = Engine<C>>,
    C::Tuning: Progress<C::Token>,
    H: 'a,
{
    type Turn<Q: delivery::Quota<H>> = Turn<'a, H, Q>;
}

impl route::Source for Uring {
    type Install = Installer;

    fn try_prepare(plan: route::Plan) -> Result<Self::Install, install::Failure> {
        Installer::try_new(plan).map_err(install::Failure::Syscall)
    }
}

impl gso::Source for Uring {
    type Mode = Control;

    const LIMITS: Option<(
        bounded::U32<1, { MAX as u32 }>,
        bounded::U32<2, { u16::MAX as u32 }>,
    )> = Some((bounded::U32::exact::<65_535>(), bounded::U32::exact::<64>()));
}

impl kernel::Source for Uring {
    type Code = op::Code;
    type Failure<S: Copy + Eq> = error::Failure<S>;
    type Returned = error::Returned;
}

impl Thread for Uring {
    type Timer = Timer;
    type Receiver = signal::raw::Receiver;
    type Cpus = Cpus;

    fn is_main() -> bool {
        // SAFETY: both calls are pure identity queries and do not dereference
        // any caller-provided pointer.
        unsafe { libc::syscall(libc::SYS_gettid) == libc::getpid() as libc::c_long }
    }

    fn current() -> Result<Self::Cpus, affinity::Error> {
        // SAFETY: zero gives the kernel initialized storage even when the CPU
        // set representation contains padding.
        let mut mask: libc::cpu_set_t = unsafe { mem::zeroed() };
        // SAFETY: mask points to writable storage of the exact size supplied.
        let result =
            unsafe { libc::sched_getaffinity(0, mem::size_of::<libc::cpu_set_t>(), &mut mask) };
        // SAFETY: sched_getaffinity returns zero or -1 with errno.
        unsafe {
            kernel::raw::Status::new(result)
                .try_zero::<Errno>(|| kernel::raw::Code::last::<Uring>().into())
        }
        .map_err(affinity::Error::Current)?;
        Ok(Cpus::new(&mask))
    }

    fn bind(cpu: affinity::raw::Cpu) -> Result<(), affinity::Error> {
        const MAX: u32 = Cpus::LIMIT as u32 - 1;
        let index = bounded::U32::<0, MAX>::new(u32::from(cpu.raw())).ok_or(
            affinity::Error::BindingRange {
                cpu,
                maximum: MAX as u16,
            },
        )?;
        // SAFETY: zero is the empty set.
        let mut mask: libc::cpu_set_t = unsafe { mem::zeroed() };
        // SAFETY: index retains this native mask's admitted bound and mask is initialized.
        unsafe { libc::CPU_SET(index.into_usize(), &mut mask) };
        // SAFETY: mask is an initialized cpu_set_t of the exact ABI size.
        let result =
            unsafe { libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &mask) };
        // SAFETY: sched_setaffinity returns zero or -1 with errno.
        unsafe {
            kernel::raw::Status::new(result)
                .try_zero::<Errno>(|| kernel::raw::Code::last::<Uring>().into())
        }
        .map_err(|fault| affinity::Error::Bind { cpu, fault })?;
        Ok(())
    }
}

impl errno::Source for Uring {
    fn clear() {
        // SAFETY: this native TLS cell is written synchronously on its owning thread.
        unsafe { libc::__errno_location().write(0) };
    }
}

impl pipe::Pipe for Uring {
    fn open() -> Result<(OwnedFd, OwnedFd), pipe::Error> {
        Self::open_with(libc::O_CLOEXEC)
    }

    fn nonblocking() -> Result<(OwnedFd, OwnedFd), pipe::Error> {
        Self::open_with(libc::O_CLOEXEC | libc::O_NONBLOCK)
    }
}

impl permit::Source for Uring {
    type Queue<K: permit::raw::Key, I: depot::raw::Compact> = Fifo<K, I>;
}

impl provided::Source for Uring {
    const PROVIDED: u16 = 1;
    const CANARY: u16 = 2;
    type Entry = Entry;
    type Index = cq::Buffer;
    type Context<L: memory::raw::Receive> = buffers::raw::Context<L>;
    type Canary<L: memory::raw::Canary> = Canary<L>;
    type Cleanup<L: memory::raw::Receive> = Cleanup<L>;
    type Producer<L: memory::raw::Receive> = Producer<L>;
    type Pending<E: memory::raw::Extent> = Selected<E, E::Capacity>;
    type Complete<E: memory::raw::Extent> = Selected<E, E::Length>;
}

impl file::File for Uring {
    type Options = types::OpenHow;
    type Request<'a> = Borrowed<'a>;
}

impl socket::Socket for Uring {
    const MAX_IOVECS: usize = libc::UIO_MAXIOV as usize;
    type Error = Errno;
    type Records = record::Packet;

    fn socket(domain: libc::c_int, kind: libc::c_int) -> Result<OwnedFd, Self::Error> {
        Self::try_open(domain, kind | libc::SOCK_CLOEXEC, 0)
    }

    fn pair() -> Result<(OwnedFd, OwnedFd), record::Error> {
        Self::try_pair::<Errno>(libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC)
            .map_err(record::Error::Open)
    }
}

impl addr::Source for Uring {
    const UNIX_TERMINATED: bool = false;
    const UNIX_ABSTRACT: bool = true;

    fn header(domain: socket::Domain, _: addr::Socklen) -> [u8; 2] {
        (domain.raw() as u16).to_ne_bytes()
    }
}

impl option::Source for Uring {
    const KEEP_IDLE: Name = Name::new(libc::TCP_KEEPIDLE);
}

impl socket::Prepare for Uring {
    type Handle = OwnedFd;
    type Fast = num::NonZero<i32>;

    fn fast_open(depth: Option<num::NonZero<i32>>) -> Result<Option<Self::Fast>, fast::Error> {
        Ok(depth)
    }

    fn open(domain: socket::Domain, kind: socket::Kind) -> Result<Self::Handle, socket::Create> {
        Self::socket(domain.raw(), kind.raw()).map_err(socket::Create::socket)
    }

    fn listener(domain: socket::Domain) -> Result<Self::Handle, socket::Create> {
        Self::open(domain, socket::Kind::Stream)
    }

    fn adopt(file: OwnedFd) -> Result<Self::Handle, socket::Setup> {
        <Self as Fd>::update::<Cloexec>(file.as_fd()).map_err(Setup::cloexec)?;
        <Self as Fd>::remove::<Nonblock>(file.as_fd()).map_err(Setup::nonblock)?;
        Ok(file)
    }

    fn into_fd(handle: Self::Handle) -> OwnedFd {
        handle
    }

    fn set_fast(file: BorrowedFd<'_>, fast: Self::Fast) -> Result<(), Errno> {
        Self::set_option(file, libc::IPPROTO_TCP, libc::TCP_FASTOPEN, &fast.get())
    }
}
impl file::Completion for Uring {
    type Opened = crate::Opened;
}

impl<T: Copy + Eq, S: Copy + Eq, F: Copy + Eq> Observe<T, S, F> for Uring {
    type Observation = crate::Observed<T>;
}
impl tuning::Source for Uring {
    type State<E: Copy, const N: usize> = crate::State<E, N>;
    type Table<E: Copy, const N: usize> = Table<E, N>;
}

impl ring::Source for Uring {
    type Decoded<T: Copy, E> = packet::Decoded<T, E, ()>;
    type Ready<T: complete::raw::Token, L: memory::raw::Receive> = Ready<T, L>;
    type Packet<T: Copy, B> = packet::Packet<T, B>;
    type Poll = enter::Poll<kernel::Errno<error::Enter>, kernel::Errno<error::Register<24>>>;
    type Enter<'a, T: 'a> = Enter<'a, T>;
    type Entry<T> = cq::Entry<T>;
    type Message<'a, const NAME: usize, const CONTROL: usize> = cq::Output<'a, NAME, CONTROL>;
    type Ring<T: complete::raw::Token> = Ring<T>;
}

impl submit::Control for Uring {
    type Target = sq::Fixed;
    type Owned<T: complete::raw::Token> = sq::Owned<T>;
    type Poll<'a, T: complete::raw::Token> = Poll<'a, T>;

    fn nop<T: complete::raw::Token>(complete: T) -> Self::Owned<T> {
        sq::Entry::new(sq::Entry::NOP, sq::Descriptor::File(-1)).user_data(complete)
    }

    fn cancel<T: complete::raw::Token>(target: T, complete: T) -> Self::Owned<T> {
        use crate::reactor::op::controls;

        controls::cancel(target, false)
            .flags(sq::Flags::SKIP_SUCCESS)
            .user_data(complete)
    }

    fn close<T: complete::raw::Token>(slot: Self::Target, complete: T) -> Self::Owned<T> {
        use crate::reactor::op::files;

        let mut entry = sq::Entry::new(files::CLOSE, sq::Descriptor::File(0));
        entry.fields().index(slot.encode());
        entry.user_data(complete)
    }

    fn shutdown<T: complete::raw::Token>(slot: Self::Target, complete: T) -> Self::Owned<T> {
        use crate::reactor::op::sockets::{self, Direction};

        sockets::shutdown(slot, Direction::Both)
            .flags(sq::Flags::HARD_LINK | sq::Flags::SKIP_SUCCESS)
            .user_data(complete)
    }
}

impl probe::Source for Uring {
    type Range = Range;
    type Release<T: token::Codec, L: memory::raw::Canary, P: Copy> = Released<T, L, P>;
    type Cleanup<T: token::Codec, L: memory::raw::Canary, P: Copy> = Recover<T, L, P>;
    type Payload = Payload;
    type Accept<T: token::Codec> = Accepted<T>;

    type Session<T: token::Codec, L: memory::raw::Canary> = Session<T, L>;

    type Outcome<T: complete::raw::Token, P: Copy> = Outcome<T, P>;
    type Waiting<T: token::Codec, S> = Waiting<S, T>;
}

impl create::Source for Uring {
    type Primary = Primary;
    type Failure<T: token::Codec> = Fault<T>;
    type Resources<
        T: token::Codec,
        L: memory::raw::Canary,
        P: Copy + From<Self::Primary> + From<memory::Error>,
    > = Resources<T, L, P>;
}

impl token::Source for Uring {
    type Token<C: host::Context> = Encoded<C>;
}

impl Source for Uring {
    type Socket<H> = Operation<H>;
}

impl admit::Native for Uring {
    type Error<F> = Infallible;
}

impl<'a, C, R> life::Finish<'a, R> for Uring
where
    C: Context + 'a,
    C::Tuning: Progress<C::Token>,
    R: life::Reclaim<Backend = Engine<C>> + 'a,
{
    type Quiesced = Quiesced<R>;

    fn try_finish(
        resolver: R,
    ) -> Result<Self::Quiesced, Stopped<R, Error<<Self as ring::Source>::Poll, R::Fault>>> {
        Engine::try_finish(resolver)
    }
}

impl<'a, C, R> life::Quiesce<'a, R> for Uring
where
    C: Context + 'a,
    C::Tuning: Progress<C::Token>,
    R: life::Cancel<Backend = Engine<C>> + 'a,
    R::Cancelling: 'a,
    R::Closing: 'a,
{
    type Quiesced = Quiesced<R::Closing>;

    fn try_quiesce(
        resolver: R,
    ) -> Result<Self::Quiesced, Stopped<R::Closing, Error<<Self as ring::Source>::Poll, R::Fault>>>
    {
        use life::{Close as _, Finish as _, Resolver as _};

        let mut resolver = resolver.cancel();
        resolver.backend().tuning().cancel_all();
        let result = resolver.backend().ring().submit().map_err(Error::File);
        (resolver, ()) = Stopped::attach(resolver, result)
            .map_err(|stopped| stopped.map(life::Close::into_closing))?;
        let result = resolver.backend().ring().cancel_all().map_err(Error::File);
        (resolver, ()) = Stopped::attach(resolver, result)
            .map_err(|stopped| stopped.map(life::Close::into_closing))?;
        resolver =
            Engine::drain(resolver).map_err(|stopped| stopped.map(life::Close::into_closing))?;
        if !resolver.backend().tuning().is_quiescent() {
            return Err(Stopped {
                state: resolver.into_closing(),
                cause: Error::Tuning,
            });
        }
        Self::try_finish(resolver.into_closing())
    }
}
