type Invariant<T> = marker::PhantomData<(ThreadBound, fn(T) -> T)>;

use std::{marker, mem, num};

use dope_sys::{
    engine::ring::create::Failure,
    io,
    io::reactor::complete,
    kernel, memory, probe,
    probe::{accept, cleanup::Rejection},
    ring,
    ring::{Access as _, create, inspect, prepare, prepare::Prepare as _, register::Registry as _},
    table::token::{Codec, host::Context},
};
pub(crate) use enter::{Enter, Features};
use map::setups::{Mapped, Setup};
pub(crate) use map::{
    cleanup::Cleanup,
    closing::Closing,
    setups::{Axis, Complete, Offset, Submit},
};
use o3::base::{marker::ThreadBound, num::bounded};
pub(crate) use outcome::Outcome;
pub(crate) use ready::Ready;
pub(crate) use session::Session;
pub(crate) use states::Resources;
pub(crate) use wait::{Accepted, Range, Recover, Released, Waiting};

use crate::{
    Admission, Observed, Primary, Register, error,
    reactor::{cq, op, register, sq},
};

mod wait;

mod outcome;

mod enter;

pub(crate) mod ffi;
mod map;

type Checked<T> = Outcome<T, Identity<T>>;
type Identity<T> = <<T as Codec>::Context as Context>::Probe;
type Update = kernel::Errno<error::Register<6>>;

impl<T, L, P> create::Acquire<L, P> for Ring<T>
where
    T: Codec,
    L: memory::raw::Canary,
    P: Copy + From<Primary> + From<memory::Error> + From<Admission>,
{
    type Range = Range;
    type Resources = Resources<T, L, P>;

    fn acquire<const FILES: u32, R>(
        submit: num::NonZero<u32>,
        complete: num::NonZero<u32>,
        defer: bool,
        files: bounded::U32<1, FILES>,
        range: Option<bounded::U32<1, { 1 << 24 }>>,
    ) -> Result<(Self, Option<Range>), Failure<Self::Setup, Self::Map, P, Self::Resources, R>> {
        use create::Retain as _;
        use probe::cleanup::Cleanup as _;

        let submit = bounded::U32::<1, { u32::MAX }>::clamp(submit.get() as usize);
        let complete = bounded::U32::<1, { u32::MAX }>::clamp(complete.get() as usize);
        let ring = <Self as create::Create>::new(submit, complete, defer)?;
        let (ring, error) = match ring.prepare(files, range) {
            Ok(ring) => return Ok((ring, range.map(Range::new))),
            Err(failure) => failure,
        };
        Err(Failure::from(
            Recover::raw(ring, error.into()).map_err(Resources::probe),
        ))
    }
}

impl<T, L, P, R> accept::Validate<L, P, R> for Ring<T>
where
    T: Codec,
    T::Context: probe::Context,
    L: memory::raw::Canary,
    P: Copy
        + From<ProbeFault<T>>
        + From<probe::Syscall<Identity<T>, probe::Creation>>
        + From<probe::Syscall<Identity<T>, dope_sys::Errno>>
        + From<probe::Syscall<Identity<T>, Update>>
        + From<accept::Check<Identity<T>>>,
    R: Copy,
{
    type Range = Range;
    type Probe = Identity<T>;
    type Update = Update;
    type Cleanup = Result<P, Recover<T, L, P>>;
    type Release = Result<(R, sq::Fixed), Released<T, L, R>>;

    fn accept(
        mut self,
        range: Range,
        release: impl FnOnce(Identity<T>, Update) -> R,
    ) -> Result<Self, Rejection<Self::Cleanup, Self::Release>> {
        use std::os::fd::AsFd as _;

        use probe::{
            Context as _,
            accept::Range as _,
            cleanup::{Cleanup as _, Rejection, Release as _},
        };

        use crate::{reactor::ring::wait::Listener, sockets::probe::Loopback};

        let probe = T::Context::LISTENER;
        let link = match Loopback::new() {
            Ok(link) => link,
            Err(error) => {
                return Err(Rejection::Cleanup(Recover::raw(
                    self,
                    probe::Syscall { probe, error }.into(),
                )));
            }
        };
        let file = match self.registry().occupy(range.fixed(), link.as_fd()) {
            Ok(file) => file,
            Err(error) => {
                return Err(Rejection::Cleanup(Recover::raw(
                    self,
                    probe::Syscall { probe, error }.into(),
                )));
            }
        };
        let state = Listener {
            ring: self,
            link,
            range,
            file,
        };
        let state = state.direct::<L, P>().map_err(Rejection::Cleanup)?;
        let mut state = state.multishot::<L, P>().map_err(Rejection::Cleanup)?;
        let removed = match state.ring.registry().try_remove(state.file) {
            Ok(()) => return Ok(state.ring),
            Err(removed) => removed,
        };
        let primary = release(T::Context::RELEASE, removed.failure);
        Err(Rejection::Release(Released::raw(
            state.ring,
            removed.file,
            primary,
        )))
    }
}

type ProbeFault<T> = probe::Cause<
    <<T as Codec>::Context as Context>::Probe,
    Observed<T>,
    kernel::Errno<error::Enter>,
>;

impl<T, L, P> probe::Validate<L, P> for Ring<T>
where
    T: Codec,
    L: memory::raw::Canary,
    P: Copy
        + From<inspect::Unsupported<op::Code, kernel::Errno<error::Register<8>>>>
        + From<ProbeFault<T>>,
{
    type Probe = <T::Context as Context>::Probe;
    type Cleanup = Result<P, Recover<T, L, P>>;

    fn support(mut self) -> Result<Self, Self::Cleanup> {
        use probe::cleanup::Cleanup as _;
        let result = inspect::Inspect::inspect(&mut self);
        let stopped = match probe::Stopped::attach(self, result) {
            Ok((ring, ())) => return Ok(ring),
            Err(stopped) => stopped,
        };
        Err(Recover::raw(stopped.state, stopped.cause.into()))
    }

    fn empty(mut self, probe: Self::Probe) -> Result<Self, Self::Cleanup> {
        use complete::Drain as _;
        use dope_sys::io::reactor::submit::Queue as _;
        use probe::cleanup::Cleanup as _;
        use ring::Complete as _;

        let submit = {
            let mut queue = self.sq();
            queue.refresh();
            queue.len()
        };
        let complete: bounded::U32<0, { u32::MAX }> = {
            let mut queue = self.completion();
            queue.acquire();
            queue.len()
        };
        if submit.get() != 0 || complete.get() != 0 {
            let cause = ProbeFault::<T>::Residual {
                probe,
                submit,
                complete,
            };
            return Err(Recover::raw(self, cause.into()));
        }
        Ok(self)
    }
}

type Error = create::Error<kernel::Errno<error::Setup>, kernel::Errno<error::Map>, Cleanup>;

#[derive(Debug)]
pub struct Ring<T> {
    pub(crate) fd: ffi::Fd,
    mapped: Mapped,
    features: Features,
    local: Invariant<T>,
}

/// Terminal ownership after queue publication, before registration release.
pub(crate) struct Release<T> {
    fd: ffi::Fd,
    maps: map::Set,
    auth: register::Authority,
    local: Invariant<T>,
}

impl<T> ring::Complete for Ring<T> {
    type Entry = cq::Entry<T>;
    type Queue<'a>
        = cq::Queue<'a, T>
    where
        Self: 'a;

    fn completion(&mut self) -> Self::Queue<'_> {
        cq::Queue::new(self.mapped.completion())
    }
}

impl<T> Ring<T> {
    pub(crate) fn into_release(self, auth: register::Authority) -> Release<T> {
        Release {
            fd: self.fd,
            maps: self.mapped.into_maps(),
            auth,
            local: self.local,
        }
    }

    pub(crate) fn new(setup: Setup) -> Self {
        let Setup {
            fd,
            mapped,
            parameters: params,
        } = setup;
        let features = Features::new(params.features);
        Self {
            fd,
            mapped,
            features,
            local: marker::PhantomData,
        }
    }

    /// Publishes the current SQ tail, refreshes the kernel-consumed head, and
    /// returns a one-shot snapshot for exactly one enter operation.
    pub(crate) fn enter_for<'a>(&'a mut self, auth: &'a register::Authority) -> Enter<'a, T> {
        Enter::new(self.mapped.sq(), ffi::Binding::registered(auth))
    }
}

impl<T: complete::raw::Token> create::Create for Ring<T> {
    type Setup = kernel::Errno<error::Setup>;
    type Map = kernel::Errno<error::Map>;
    type Unmap = kernel::Errno<error::Unmap>;
    type Cleanup = Cleanup;

    fn new<const MIN: u32, const MAX: u32, const CQ: u32>(
        submit: bounded::U32<MIN, MAX>,
        complete: bounded::U32<1, CQ>,
        defer: bool,
    ) -> Result<Self, Error> {
        const { assert!(MIN != 0) };
        let submit = num::NonZero::<u32>::MIN.saturating_add(submit.get() - 1);
        let complete = num::NonZero::<u32>::MIN.saturating_add(complete.get() - 1);
        Ok(Self::new(Setup::new(submit, complete, defer)?))
    }
}

impl<T> prepare::Prepare for Ring<T> {
    type Cancel = kernel::Errno<error::Register<24>>;
    type Files = kernel::Errno<error::Register<13>>;
    type Range = kernel::Errno<error::Register<25>>;

    fn prepare<const FILES: u32, const RANGE: u32>(
        mut self,
        files: bounded::U32<1, FILES>,
        range: Option<bounded::U32<1, RANGE>>,
    ) -> Result<
        Self,
        (
            Self,
            prepare::Rejection<Self::Cancel, Self::Files, Self::Range>,
        ),
    > {
        let mut admit = || {
            self.features
                .require()
                .map_err(prepare::Rejection::Feature)?;
            let mut registry = self.registry();
            registry.cancel_all().map_err(prepare::Rejection::Cancel)?;
            registry
                .sparse_files(files)
                .map_err(prepare::Rejection::Files)?;
            if let Some(range) = range {
                registry
                    .file_range(range)
                    .map_err(prepare::Rejection::Range)?;
            }
            Ok(())
        };
        match admit() {
            Ok(()) => Ok(self),
            Err(cause) => Err((self, cause)),
        }
    }
}

impl<T> inspect::Inspect for Ring<T> {
    type Code = op::Code;
    type Register = kernel::Errno<error::Register<8>>;

    fn inspect(&mut self) -> Result<(), inspect::Unsupported<Self::Code, Self::Register>> {
        use inspect::Unsupported;

        use crate::reactor::op::{buffers, controls, files, messages, sockets};

        let probe =
            register::Probe::acquire(&mut self.registry()).map_err(Unsupported::Register)?;
        for required in &[
            controls::POLL,
            messages::SEND,
            messages::RECEIVE,
            sockets::ACCEPT,
            controls::CANCEL,
            sockets::CONNECT,
            files::CLOSE,
            files::WRITE,
            files::SYNC,
            buffers::SEND,
            buffers::RECEIVE,
            files::OPEN,
            files::RENAME,
            files::UNLINK,
            sockets::SHUTDOWN,
            sockets::CREATE,
            sockets::SET_OPTION,
        ] {
            probe.require(*required).map_err(Unsupported::Opcode)?;
        }
        Ok(())
    }
}

impl<T> io::Close for Ring<T> {
    type State = Closing<T>;

    fn try_close(self) -> Result<(), Self::State> {
        Closing::close(self.fd, self.mapped.into_maps(), self.local)
    }
}

impl<T> io::Close for Release<T> {
    type State = register::raw::Retained<T>;

    fn try_close(self) -> Result<(), Self::State> {
        match self.fd.try_release(&self.auth) {
            Ok(()) => Closing::close(self.fd, self.maps, self.local)
                .map_err(register::raw::Retained::mapping),
            Err(error) => Err(register::raw::Retained::registered(self, error)),
        }
    }
}

impl<T> ring::Access for Ring<T> {
    type Token = T;
    type Target = sq::Fixed;
    type File = register::raw::Occupied;
    type Queue<'a>
        = sq::raw::Queue<'a, T>
    where
        Self: 'a,
        T: complete::raw::Token;
    type Enter<'a>
        = Enter<'a, T>
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
        sq::raw::Queue::new(self.mapped.sq())
    }
    fn enter(&mut self) -> Enter<'_, T> {
        Enter::new(self.mapped.sq(), self.fd.target())
    }
    fn registry(&mut self) -> Register<'_, T> {
        Register::new(&self.fd)
    }
}

impl<T> ring::Owner for Ring<T> {
    type Registered = register::raw::Registered<T>;
    type Registration = register::Attempt<T>;
    type Closing = register::Closing<T>;
    fn try_register(mut self) -> Result<register::raw::Registered<T>, register::Attempt<T>> {
        let auth = match self.registry().register() {
            Ok(auth) => auth,
            Err(error) => return Err(register::Attempt::new(self, error)),
        };
        Ok(register::raw::Registered::new(self, auth))
    }
    fn close_file(self, file: Self::File) -> Result<(), Self::Closing> {
        use io::Close as _;
        match self.try_close() {
            Ok(()) => Ok(()),
            Err(ring) => Err(register::Closing::new(ring, file)),
        }
    }
}

mod session;
mod states;

mod ready;

const _: () = {
    assert!(mem::size_of::<Release<()>>() == 5 * mem::size_of::<usize>());
    assert!(mem::align_of::<Release<()>>() == mem::align_of::<usize>());
};
