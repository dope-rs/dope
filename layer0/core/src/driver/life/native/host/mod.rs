use dope_sys::{io::file::File, memory};

use self::poll::file;
pub(crate) mod context;
pub(crate) mod poll;
use std::{marker, mem, num};

use dope_sys::{
    engine,
    engine::{
        admit,
        ring::{
            Context,
            config::{Build, Config},
        },
    },
    io::{
        reactor::{
            complete, read, ready,
            ready::{
                delivery,
                delivery::entry,
                store,
                store::{Exec, config, submit},
            },
            retry,
        },
        request::{Lower, Source},
        socket::{Prepare, option::tuning},
    },
    memory::{buffer, buffer::provided},
    probe, ring, table,
    table::{change, permit, token},
};
use o3::depot::map::Plan;

use crate::{
    Backend, driver,
    driver::{
        life::{
            domain::runtime,
            native::{
                host::poll::{events, files},
                request,
            },
        },
        receive,
        result::poll::{Cause, Dispatch, fault},
        retained::flight,
        route::{indices::SlotIndex, table::Capacity},
        schedule::ready::Stored,
        settings,
    },
    io,
    io::{event::batch::Flow, fd::handles, socket::option, transfer},
};

type Done<N> = <N as entry::Source>::Event<events::Context<N>, Received<N>>;
type ReadyPoll<N> = <N as ready::Source>::Poll<files::Slots, poll::Failure>;
type PollState<N> = <N as ready::Source>::State<poll::raw::Context<N>>;
type DoneQueue<N> = <N as complete::Source>::Queue<Done<N>, settings::Completion>;
type Vacancy<'a, N> = <DoneQueue<N> as complete::Queue>::Vacancy<'a>;
type Trace<N> = <N as complete::Observe<fault::Echo, handles::PhysicalSlot, Flow>>::Observation;
type Files<N> = <N as table::Source>::Files<files::Slots>;
type Reader<'a, N> = <N as read::Source>::Arm<'a, files::Slots, Done<N>, ReadyPoll<N>>;
type ReadSlot<N> = <N as read::Source>::Entry<files::Slots>;
type Registry<N> = <N as read::Source>::Registry<files::Slots>;
type Data<N> = <N as retry::Source>::Send<files::Slots>;
type Pending<N> = <N as retry::Source>::Pending<files::Slots>;
type Writes<N> = <N as retry::Source>::State<Pending<N>>;
type Retry<'a, N> = <N as retry::Source>::Retry<'a, files::Slots, Done<N>, ReadyPoll<N>>;
type Pool<N> = <N as buffer::Source>::Pool<Receive>;
type Received<N> = <Pool<N> as buffer::Pool>::Buffer;
type Permits<N> = <N as permit::Source>::Queue<Stored, receive::Index>;

/// Binds a selected native owner to this driver's typed completion contracts.
#[derive(Debug)]
pub(crate) struct Host<N>(marker::PhantomData<fn() -> N>);

impl<N> admit::Context for Host<N>
where
    N: File + Source<request::Context> + admit::Native,
{
    type Native = N;
    type Inputs = request::Context;
    type Key = flight::raw::Echo;
    type Failure = Cause;
}

impl<'a, 'd> delivery::Context<driver::Context<'a, 'd>> for Host<Backend> {
    type Backend = crate::Engine;
    type Event = io::Event<'d>;
    type Failure = fault::Fault;
    type Dispatch = Dispatch<'d>;

    fn with<R>(host: &mut driver::Context<'a, 'd>, run: impl FnOnce(&mut Self::Backend) -> R) -> R {
        let mut native = host.native();
        run(native.as_mut())
    }

    fn resolve(
        host: &mut driver::Context<'a, 'd>,
    ) -> Option<Result<Option<Self::Event>, Self::Failure>> {
        host.native().resolve().resolve()
    }
}

impl<N> submit::Context for Host<N>
where
    N: buffer::Source + entry::Source + Prepare,
    Self: ready::Driver<Context: Exec<Key = flight::raw::Echo, Event = Done<N>>>,
{
    type Outbound = handles::OutboundSlot;
    type Handle = N::Handle;
    type Created = Done<N>;
    type File = file::File<N>;

    fn file(key: flight::raw::Echo) -> Self::File {
        file::File::<N>::new(key)
    }
}

impl<N> ready::Driver for Host<N>
where
    N: ready::Source
        + read::Source
        + retry::Source
        + entry::Source
        + complete::Source
        + permit::Source
        + Source<request::Context>
        + Prepare
        + complete::Observe<fault::Echo, handles::PhysicalSlot, Flow>,
{
    type Map = Done<N>;
    type Observation = Trace<N>;
    type Context = poll::raw::Context<N>;
    type State = PollState<N>;
    type Queue = DoneQueue<N>;

    fn shutdown() -> events::Udata {
        let () = Self::LAYOUT;
        events::Udata::Shutdown
    }
}

impl<N> config::Config<settings::Config> for Host<N>
where
    Self: ready::Driver<
            Context: store::Context<
                Pool: buffer::Pool<Layout = Receive>,
                Slots: table::raw::Slots<Layout = settings::FileSlots>,
            >,
            Queue: complete::Queue<Capacity = settings::Completion>,
        >,
{
    type Key = Stored;
    type Index = receive::Index;

    fn config(config: &settings::Config) -> config::Build<Self> {
        let slots = config.file_slots();
        config::Build {
            receive: Receive(config.receive()),
            slots,
            plan: Plan::from_bounded(slots.bound()),
            capacity: config.queue_layout().completion(),
        }
    }
}

impl<N> Context for Host<N>
where
    N: tuning::Source + provided::Source + ring::Source + token::Source,
    N: complete::Observe<<N as token::Source>::Token<context::Context>>,
{
    type Map =
        <N as ring::Source>::Decoded<<N as token::Source>::Token<context::Context>, Selection>;
    type Observation = <N as complete::Observe<Self::Token>>::Observation;
    type Selection = (N::Index, settings::Buffers);
    type Token = <N as token::Source>::Token<context::Context>;
    type Layout = Receive;
    type Tuning = N::Table<flight::raw::Echo, { option::MAX_STREAM_OPTIONS }>;
}

impl<N> Config<settings::Config> for Host<N>
where
    N: probe::Source
        + provided::Source
        + permit::Source
        + token::Source
        + engine::Source<Self, Backend: engine::Buffer>,
    Self: Context<Layout = Receive>,
{
    type Key = Stored;
    type Index = receive::Index;

    fn config(config: &settings::Config) -> Build<Self::Layout> {
        let () = Self::RING;
        let queues = config.queue_layout();
        let files = config.file_slots();
        Build {
            submit: num::NonZero::<u32>::MIN.saturating_add(queues.submission().get() - 1),
            complete: num::NonZero::<u32>::MIN.saturating_add(queues.completion().get() - 1),
            defer: matches!(config.progress(), settings::Progress::BatchedWhenSupported),
            files: files.bound(),
            range: files.accept_slot().map(SlotIndex::position),
            receive: Receive(config.receive()),
        }
    }
}

impl<N> Host<N>
where
    N: probe::Source
        + provided::Source
        + permit::Source
        + token::Source
        + engine::Source<Self, Backend: engine::Buffer>,
{
    const RING: () = {
        use dope_sys::io::reactor::complete::packet::record::Selection;

        type Received<N> = <N as provided::Source>::Context<Receive>;
        type Producer<N> = <N as provided::Source>::Producer<Receive>;
        type Canary<N> = <N as provided::Source>::Canary<Receive>;
        type Selected<N> = <N as provided::Source>::Pending<transfer::Extent>;
        type Completed<N> = <N as provided::Source>::Complete<transfer::Extent>;
        type Fault<N> = self::Selection<(<N as provided::Source>::Index, settings::Buffers)>;
        /// Registered close retains one fd, authority, and two mappings through unregister.
        /// Its exclusive phases preserve the exact unregister or unmap failure.
        /// Producer storage, tuning, and receive context remain owned until release.
        type Closing<N> = runtime::Closing<<N as engine::Source<Host<N>>>::Backend>;
        let word = mem::size_of::<usize>();
        assert!(usize::BITS >= u32::BITS);
        assert!(mem::size_of::<Received<N>>() == 2 * word + mem::size_of::<Receive>());
        assert!(mem::size_of::<Producer<N>>() == 5 * word);
        assert!(mem::size_of::<Selected<N>>() == 2 * mem::size_of::<u32>());
        assert!(mem::align_of::<Selected<N>>() == mem::align_of::<u32>());
        assert!(mem::size_of::<Completed<N>>() == 2 * mem::size_of::<u32>());
        assert!(mem::size_of::<(N::Index, settings::Buffers)>() == mem::size_of::<u32>());
        assert!(mem::align_of::<(N::Index, settings::Buffers)>() == mem::align_of::<u16>());
        assert!(mem::size_of::<Selection<Selected<N>, Fault<N>>>() == 3 * mem::size_of::<u32>());
        assert!(
            mem::size_of::<Canary<N>>()
                == mem::size_of::<Received<N>>() + mem::size_of::<Producer<N>>()
        );
        assert!(mem::size_of::<Permits<N>>() == 4 * word);
        assert!(mem::align_of::<Permits<N>>() == mem::align_of::<usize>());
        assert!(mem::size_of::<Closing<N>>() == 18 * word);
        assert!(mem::align_of::<Closing<N>>() == mem::align_of::<usize>());
        assert!(mem::size_of::<Result<(), Closing<N>>>() == mem::size_of::<Closing<N>>());
    };
}

impl<N> Host<N>
where
    N: ready::Source
        + read::Source
        + retry::Source
        + entry::Source
        + complete::Source
        + permit::Source
        + Source<request::Context>
        + Prepare
        + complete::Observe<fault::Echo, handles::PhysicalSlot, Flow>,
{
    const LAYOUT: () = {
        type Changes<N> = <Files<N> as table::Files<files::Slots>>::Changes;
        let word = mem::size_of::<usize>();
        assert!(mem::size_of::<Permits<N>>() == 0);
        assert!(mem::align_of::<Permits<N>>() == 1);
        assert!(mem::size_of::<DoneQueue<N>>() == 3 * word);
        assert!(mem::size_of::<Vacancy<'_, N>>() == word);
        assert!(mem::size_of::<Done<N>>() == 4 * word);
        assert!(mem::size_of::<complete::Dequeued<Done<N>>>() == 5 * word);
        assert!(mem::size_of::<Option<complete::Dequeued<Done<N>>>>() == 5 * word);
        assert!(mem::size_of::<<N::Raw as Lower>::Borrowed<'_>>() == 3 * word);
        assert!(mem::align_of::<<N::Raw as Lower>::Borrowed<'_>>() == mem::align_of::<usize>());
        assert!(
            mem::size_of::<PollState<N>>()
                == mem::size_of::<(Writes<N>, Files<N>, ReadyPoll<N>, Registry<N>, Pool<N>)>()
        );
        assert!(mem::size_of::<Data<N>>() == 2 * word);
        assert!(mem::size_of::<<Changes<N> as change::Changes>::Id>() == mem::size_of::<u64>());
        assert!(mem::size_of::<<Changes<N> as change::Changes>::Tail<'_>>() == word);
        assert!(
            mem::size_of::<Files<N>>()
                == mem::size_of::<(
                    Box<[Option<<Files<N> as table::Files<files::Slots>>::Handle>]>,
                    Capacity,
                    o3::depot::index::set::State,
                    Capacity,
                    Changes<N>
                )>()
        );
        assert!(mem::size_of::<Trace<N>>() == 3 * word);
    };
}

const _: () = {
    assert!(
        mem::size_of::<Option<std::os::fd::OwnedFd>>()
            <= mem::size_of::<Option<std::os::fd::RawFd>>()
    );
    assert!(mem::size_of::<usize>() == mem::size_of::<u64>());
};

/// Immutable validated receive geometry bound to the native backend.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Receive(pub(crate) settings::Receive);

// SAFETY: Receive construction validates a positive bounded slot length and a
// power-of-two entry count whose product fits isize. Copies retain both values.
unsafe impl memory::raw::Layout for Receive {
    fn entries(self) -> num::NonZero<u16> {
        self.0.count()
    }

    fn capacity(self) -> num::NonZero<u32> {
        self.0.buffer_len().into_nonzero()
    }
}

// SAFETY: the typed slot limit and raw capacity come from the same Receive value.
unsafe impl memory::raw::Receive for Receive {
    type Extent = transfer::Extent;

    fn limit(self) -> transfer::NonZero {
        self.0.buffer_len()
    }
}

// SAFETY: the fixed two-slot canary has the original datagram capacity;
// its first slot has room for the scalar used before any receive is submitted.
unsafe impl memory::raw::Canary for Receive {
    const LAYOUT: Self = {
        const {
            assert!(
                std::mem::size_of::<i32>()
                    <= settings::Receive::MIN_DATAGRAM_SLOT.into_nonzero().get() as usize
            )
        };
        Self(settings::Receive::fixed::<
            2,
            { settings::Receive::MIN_DATAGRAM_SLOT.into_nonzero().get() },
        >())
    };
}

/// Private decoder carrier keeps native uninhabited errors and their layout.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub(crate) struct Selection<T = <Backend as buffer::Selection<settings::Buffers>>::Error>(T);

impl<T> From<T> for Selection<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<I> From<(I, Receive)> for Selection<(I, settings::Buffers)> {
    fn from((index, layout): (I, Receive)) -> Self {
        Self((index, layout.0.buffers()))
    }
}

impl<E> Selection<E>
where
    Backend: buffer::Selection<settings::Buffers, Error = E>,
{
    pub(crate) fn into_public(self) -> receive::Selection {
        receive::Selection::from_native(self.0)
    }
}
