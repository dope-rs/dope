use dope_sys::{
    io::{Close as _, Retry as _},
    kernel, memory,
    memory::{
        Failure as _,
        buffer::{provided, provided::Cleanup as _},
    },
    probe::{cleanup, cleanup::Cleanup as _},
    ring::{create, create::Build as _, map, map::Unmap as _, register::Registration as _},
    table::token,
};

use crate::{
    BufferBuild, Fault, Primary, Ring, error,
    reactor::{
        buffers::{self, Canary, Cleanup, Producer},
        register, ring,
        ring::Recover,
    },
};

pub struct Resources<T: token::Codec, L: memory::raw::Canary, P>(State<T, L, P>);

pub struct Build<T: token::Codec, L: memory::raw::Canary, P>(Result<P, Pending<T, L, P>>);

enum State<T: token::Codec, L: memory::raw::Canary, P> {
    Ring(ring::Cleanup),
    Probe(Recover<T, L, P>),
    Unbound(Pending<T, L, P>),
    Bound {
        pending: Pending<T, L, P>,
        context: buffers::raw::Context<L>,
    },
}

enum Pending<T: token::Codec, L: memory::raw::Canary, P> {
    Buffers(BufferBuild<L, P>),
    Open(Open<T, L, P>),
    Raw(Closed<T, P>),
    Provided(Provided<T, L, P>),
    Registration(Registration<T, L>),
}

struct Open<T, L: memory::raw::Canary, P> {
    ring: Ring<T>,
    buffers: BufferBuild<L, P>,
}

struct Closed<T, P> {
    primary: P,
    current: ring::Closing<T>,
}

struct Provided<T, L: memory::raw::Canary, P> {
    primary: P,
    current: register::raw::Retained<T>,
    buffers: Producer<L>,
}

struct Registration<T, L: memory::raw::Canary> {
    current: register::Cleanup<T>,
    buffers: Producer<L>,
}

impl<T: token::Codec, L: memory::raw::Canary, P> From<ring::Cleanup> for Resources<T, L, P> {
    fn from(state: ring::Cleanup) -> Self {
        Self(State::Ring(state))
    }
}

impl<T: token::Codec, L: memory::raw::Canary, P: Copy + From<Primary> + From<memory::Error>>
    create::Retain<P> for Resources<T, L, P>
{
    type Build = Build<T, L, P>;
    type Ring = Ring<T>;
    type Probe = Recover<T, L, P>;
    type Canary = Canary<L>;
    type Context = buffers::raw::Context<L>;
    type Buffers = Cleanup<L>;
    type Producer = Producer<L>;
    type Registration = register::Attempt<T>;
    type Registered = register::raw::Registered<T>;
    type BufferError = kernel::Errno<error::Register<22>>;

    fn probe(state: Recover<T, L, P>) -> Self {
        Self(State::Probe(state))
    }

    fn open<F: provided::Failure<Cleanup = Cleanup<L>>>(ring: Ring<T>, fault: F) -> Self::Build {
        match BufferBuild::open(fault, P::from) {
            Ok(primary) => Pending::close_raw(ring, primary),
            Err(buffers) => Build(Err(Pending::Open(Open { ring, buffers }))),
        }
    }

    fn canary(ring: Ring<T>, primary: P, buffers: Canary<L>) -> Result<P, Self> {
        let cleanup = match buffers.try_close() {
            Ok(()) => return Pending::close_raw(ring, primary).into_result(),
            Err(cleanup) => cleanup,
        };
        Err(Self::unmap(ring, primary, cleanup))
    }

    fn unmap(ring: Ring<T>, primary: P, cleanup: Cleanup<L>) -> Self {
        Self(State::Unbound(Pending::Open(Open {
            ring,
            buffers: BufferBuild::new(primary, cleanup),
        })))
    }

    fn registration(state: register::Attempt<T>, buffers: Producer<L>) -> Self::Build {
        let result = match state.try_close() {
            Ok(error) => Pending::<T, L, P>::close(Primary::Register(error).into(), buffers)
                .map_err(Pending::Buffers),
            Err(current) => Err(Pending::Registration(Registration { current, buffers })),
        };
        Build(result)
    }

    fn provided(
        cause: Self::BufferError,
        ring: Self::Registered,
        buffers: Producer<L>,
    ) -> Self::Build {
        let primary = Primary::Provided(cause).into();
        let result = match ring.try_close() {
            Ok(()) => Pending::<T, L, P>::close(primary, buffers).map_err(Pending::Buffers),
            Err(current) => Err(Pending::Provided(Provided {
                primary,
                current,
                buffers,
            })),
        };
        Build(result)
    }
}

impl<T: token::Codec, L: memory::raw::Canary, P: Copy + From<Primary> + From<memory::Error>>
    create::Build<P> for Build<T, L, P>
{
    type Context = buffers::raw::Context<L>;
    type Cleanup = Resources<T, L, P>;

    fn into_result(self) -> Result<P, Self::Cleanup> {
        self.0.map_err(|pending| Resources(State::Unbound(pending)))
    }

    fn close(self, context: buffers::raw::Context<L>) -> Result<P, Self::Cleanup> {
        match self.0 {
            Ok(primary) => Pending::<T, L, P>::context(primary, context)
                .map_err(|pending| Resources(State::Unbound(Pending::Buffers(pending)))),
            Err(pending) => Err(Resources(State::Bound { pending, context })),
        }
    }
}

impl<T: token::Codec, L: memory::raw::Canary, P: Copy + From<Primary> + From<memory::Error>>
    memory::Cleanup<P> for Resources<T, L, P>
{
    type Error = Fault<T>;

    fn try_close(self, mut report: impl FnMut(Fault<T>)) -> Result<P, Self> {
        match self.0 {
            State::Ring(state) => state
                .try_close(|cause| report(create::Failure::Storage(map::Storage::Ring(cause))))
                .map(|cause| Primary::Ring(cause).into())
                .map_err(|state| Self(State::Ring(state))),
            State::Probe(state) => state
                .try_close(|cause| report(probe_error(cause)))
                .map_err(|state| Self(State::Probe(state))),
            State::Unbound(pending) => pending
                .try_close(&mut report)
                .map_err(|pending| Self(State::Unbound(pending))),
            State::Bound { pending, context } => match pending.try_close(&mut report) {
                Ok(primary) => Pending::<T, L, P>::context(primary, context)
                    .map_err(|pending| Self(State::Unbound(Pending::Buffers(pending)))),
                Err(pending) => Err(Self(State::Bound { pending, context })),
            },
        }
    }

    fn primary(&self) -> P {
        match &self.0 {
            State::Ring(state) => Primary::Ring(state.primary()).into(),
            State::Probe(state) => state.primary(),
            State::Unbound(pending) | State::Bound { pending, .. } => pending.primary(),
        }
    }

    fn failure(&self) -> Fault<T> {
        match &self.0 {
            State::Ring(state) => create::Failure::Storage(map::Storage::Ring(state.failure())),
            State::Probe(state) => probe_error(state.failure()),
            State::Unbound(pending) | State::Bound { pending, .. } => pending.failure(),
        }
    }
}

impl<T: token::Codec, L: memory::raw::Canary, P: Copy + From<Primary> + From<memory::Error>>
    memory::Cleanup<P> for Pending<T, L, P>
{
    type Error = Fault<T>;

    fn try_close(self, mut report: impl FnMut(Fault<T>)) -> Result<P, Self> {
        match self {
            Self::Buffers(state) => Self::retry_buffer(state, &mut report).map_err(Self::Buffers),
            Self::Open(Open { ring, buffers }) => match Self::retry_buffer(buffers, &mut report) {
                Ok(primary) => Self::close_raw(ring, primary).0,
                Err(buffers) => Err(Self::Open(Open { ring, buffers })),
            },
            Self::Raw(Closed { primary, current }) => {
                let (owner, cause) = current.recover().into_parts();
                report(create::Failure::Storage(cause.into()));
                match owner.try_close() {
                    Ok(()) => Ok(primary),
                    Err(current) => Err(Self::Raw(Closed { primary, current })),
                }
            }
            Self::Provided(Provided {
                primary,
                current,
                buffers,
            }) => {
                let (owner, cause) = current.recover().into_parts();
                report(create::Failure::Storage(cause));
                match owner.try_close() {
                    Ok(()) => Self::close(primary, buffers).map_err(Self::Buffers),
                    Err(current) => Err(Self::Provided(Provided {
                        primary,
                        current,
                        buffers,
                    })),
                }
            }
            Self::Registration(Registration { current, buffers }) => match current
                .try_close(|cause| report(create::Failure::Storage(map::Storage::Ring(cause))))
            {
                Ok(cause) => {
                    Self::close(Primary::Register(cause).into(), buffers).map_err(Self::Buffers)
                }
                Err(current) => Err(Self::Registration(Registration { current, buffers })),
            },
        }
    }

    fn primary(&self) -> P {
        match self {
            Self::Buffers(state) => state.primary(),
            Self::Open(Open { buffers, .. }) => buffers.primary(),
            Self::Raw(Closed { primary, .. }) | Self::Provided(Provided { primary, .. }) => {
                *primary
            }
            Self::Registration(Registration { current, .. }) => {
                Primary::Register(current.primary()).into()
            }
        }
    }

    fn failure(&self) -> Fault<T> {
        match self {
            Self::Buffers(state) => create::Failure::Memory(state.failure()),
            Self::Open(Open { buffers, .. }) => create::Failure::Memory(buffers.failure()),
            Self::Raw(Closed { current, .. }) => create::Failure::Storage(current.failure()),
            Self::Provided(Provided { current, .. }) => create::Failure::Storage(current.failure()),
            Self::Registration(Registration { current, .. }) => {
                create::Failure::Storage(map::Storage::Ring(current.failure()))
            }
        }
    }
}

impl<T: token::Codec, L: memory::raw::Canary, P: Copy + From<Primary> + From<memory::Error>>
    Pending<T, L, P>
{
    fn retry_buffer(
        state: BufferBuild<L, P>,
        report: &mut impl FnMut(Fault<T>),
    ) -> Result<P, BufferBuild<L, P>> {
        let (primary, cleanup) = state.recover();
        let (owner, cause) = cleanup.recover().into_parts();
        report(create::Failure::Memory(cause));
        let cleanup = match owner.try_close() {
            Ok(()) => return Ok(primary),
            Err(cleanup) => cleanup,
        };
        Err(BufferBuild::new(primary, cleanup))
    }

    fn close_raw(ring: Ring<T>, primary: P) -> Build<T, L, P> {
        match ring.try_close() {
            Ok(()) => Build(Ok(primary)),
            Err(current) => Build(Err(Self::Raw(Closed { primary, current }))),
        }
    }

    fn close(primary: P, buffers: Producer<L>) -> Result<P, BufferBuild<L, P>> {
        Self::buffers(primary, buffers.try_close().map_err(Cleanup::producer))
    }

    fn context(primary: P, context: buffers::raw::Context<L>) -> Result<P, BufferBuild<L, P>> {
        Self::buffers(primary, context.try_close().map_err(Cleanup::payload))
    }

    fn buffers(primary: P, closed: Result<(), Cleanup<L>>) -> Result<P, BufferBuild<L, P>> {
        let cleanup = match closed {
            Ok(()) => return Ok(primary),
            Err(cleanup) => cleanup,
        };
        Err(BufferBuild::new(primary, cleanup))
    }
}

fn probe_error<S, W>(cause: cleanup::Failure<S, W>) -> create::Failure<S, W> {
    match cause {
        cleanup::Failure::Storage(error) => create::Failure::Storage(error),
        cleanup::Failure::Wait(error) => create::Failure::Wait(error),
    }
}
