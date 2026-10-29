use std::os::fd;

pub(super) use closing::Closing;
pub(super) use direct::Direct;
use dope_sys::{
    io::{
        Close,
        reactor::{
            complete::flags::Meta as _,
            submit::{Control as _, Target as _},
        },
        request::Lower as _,
        socket::Domain,
    },
    kernel, memory,
    memory::{Failure as _, buffer::provided},
    probe,
    probe::{
        Cause, Context as _, Outcome as _, State, Stopped,
        cleanup::{Cleanup as _, Rejection},
        receive, session,
    },
    ring::{
        Access as _,
        create::{Build as _, Retain as _},
        register::{Occupied as _, Registry as _},
    },
    table::{token, token::host},
};
use o3::base::num::bounded;
pub(super) use pending::Pending;
use receiver::Receiver;

use crate::{
    Observed, Ring, Uring, error,
    reactor::{
        buffers::{Canary, Payload},
        cq, register,
        ring::{Outcome, Recover, Released, Resources, Waiting},
        sq,
    },
};

mod closing;
mod direct;
mod pending;
mod receiver;

const GROUP: u16 = <Uring as provided::Source>::CANARY;
const SLOT: sq::Fixed = sq::Fixed::ZERO;

type Identity<T> = <<T as token::Codec>::Context as host::Context>::Probe;
type Fault<T> = Cause<Identity<T>, Observed<T>, kernel::Errno<error::Enter>>;
type Checked<T> = Outcome<T, Identity<T>>;
type Stage<T, L, F> = State<Session<T, L, F>, Pending<T, L, F>>;
type Setup =
    session::Primary<kernel::Errno<error::Register<22>>, kernel::Errno<error::Register<23>>>;
type Inspected<T, L, E> = session::Checked<
    Identity<T>,
    Observed<T>,
    kernel::Errno<error::Enter>,
    cq::Buffer,
    <<L as memory::raw::Receive>::Extent as memory::raw::Extent>::Length,
    <<L as memory::raw::Receive>::Extent as memory::raw::Extent>::Capacity,
    E,
>;
type Inspect<T> = <<T as token::Codec>::Context as receive::Context<Payload>>::Check;
type Update = kernel::Errno<error::Register<6>>;

pub struct Session<T, L: memory::raw::Canary, F = ()> {
    owner: register::raw::Buffered<Canary<L>, GROUP, Ring<T>>,
    file: F,
}

impl<T, L: memory::raw::Canary, F> Session<T, L, F> {
    fn ring(&mut self) -> &mut Ring<T> {
        &mut self.owner
    }
}

impl<T, L, P, R> receive::Validate<P, R> for Session<T, L>
where
    T: token::Codec<Context: receive::Context<Payload>>,
    L: memory::raw::Canary,
    P: Copy
        + From<Fault<T>>
        + From<Inspected<T, L, Inspect<T>>>
        + From<probe::Syscall<Identity<T>, dope_sys::Errno>>
        + From<probe::Syscall<Identity<T>, probe::Creation>>
        + From<receive::Install<Update>>
        + From<(Identity<T>, receive::Send)>,
    R: Copy,
{
    type Update = Update;
    type Cleanup = Result<P, Recover<T, L, P>>;
    type Release = Result<(R, sq::Fixed), Released<T, L, R>>;

    fn receive(
        mut self,
        release: impl Fn(receive::Mode, Update) -> R,
    ) -> Result<Self, Rejection<Self::Cleanup, Self::Release>> {
        for mode in [receive::Mode::Data, receive::Mode::Message] {
            let receiver = Receiver::<T, L, P>::new(self, mode).map_err(Rejection::Cleanup)?;
            self = receiver.run(&release)?;
        }
        Ok(self)
    }
}

impl<T: token::Codec, L: memory::raw::Canary, F> session::Session for Session<T, L, F> {
    type Ring = Ring<T>;
    type Layout = L;
    type Owned = sq::Owned<T>;
    type Probe = Identity<T>;
    type Outcome = Checked<T>;
    type Error = kernel::Errno<error::Enter>;
    type Pending = Pending<T, L, F>;

    fn submit(
        self,
        entry: &sq::Owned<T>,
        probe: Identity<T>,
    ) -> Result<Pending<T, L, F>, Stopped<Self, Fault<T>>> {
        use dope_sys::probe::raw::Wait as _;
        Waiting::submit(self, Self::ring, entry, probe).map(Pending::new)
    }
}

impl<T: token::Codec, L: memory::raw::Canary> session::Empty for Session<T, L> {
    type Install = kernel::Errno<error::Register<6>>;
    type Direct = Session<T, L, Direct>;
    type Occupied = Session<T, L, register::raw::Occupied>;

    fn create(self, probe: Identity<T>) -> Result<Self::Direct, Stopped<Self, Fault<T>>> {
        use dope_sys::probe::raw::Wait as _;
        let entry = sq::Entry::socket(SLOT, Domain::Inet4).user_data(T::probe(probe));
        let (state, done) = Waiting::submit(self, Self::ring, &entry, probe)?.finish(Self::ring)?;
        let (state, ()) = Stopped::attach(state, done.zero().map_err(Cause::Check))?;
        Ok(Session {
            owner: state.owner,
            file: Direct(()),
        })
    }

    fn install(
        mut self,
        file: fd::BorrowedFd<'_>,
    ) -> Result<Self::Occupied, Stopped<Self, Self::Install>> {
        let result = self.owner.registry().occupy(SLOT, file);
        let (state, file) = Stopped::attach(self, result)?;
        Ok(Session {
            owner: state.owner,
            file,
        })
    }
}

impl<T, L, P> session::Create<P> for Session<T, L>
where
    T: token::Codec<Context: probe::Context>,
    L: memory::raw::Canary,
    P: Copy + From<crate::Primary> + From<memory::Error> + From<Setup> + From<Fault<T>>,
{
    type Cleanup = Resources<T, L, P>;

    fn try_new(mut ring: Ring<T>) -> Result<Self, Result<P, Self::Cleanup>> {
        use dope_sys::memory::buffer::canary::raw::Storage as _;
        const {
            assert!(size_of::<Session<T, L, Direct>>() == size_of::<Self>());
            assert!(
                size_of::<Pending<T, L, register::raw::Occupied>>()
                    == size_of::<Session<T, L, register::raw::Occupied>>()
                        + align_of::<Session<T, L, register::raw::Occupied>>()
            );
            assert!(
                size_of::<Stage<T, L, register::raw::Occupied>>()
                    == size_of::<Pending<T, L, register::raw::Occupied>>()
            );
        }
        let canary = match Canary::new() {
            Ok(canary) => canary,
            Err(error) => return Err(Self::Cleanup::open(ring, error).into_result()),
        };
        let buffer = canary.into_buffer::<GROUP>();
        let cause = match ring
            .registry()
            .buffer(buffer.address, buffer.entries, GROUP)
        {
            Ok(()) => {
                return Ok(Self {
                    owner: buffer.bind(ring),
                    file: (),
                });
            }
            Err(cause) => cause,
        };
        Err(Self::Cleanup::canary(
            ring,
            Setup::Register(cause).into(),
            buffer.into_storage(),
        ))
    }

    fn socket(self) -> Result<Self, Result<P, Self::Cleanup>> {
        use session::{Direct as _, Empty as _};

        let state = match self.create(T::Context::SOCKET) {
            Ok(state) => state,
            Err(stopped) => {
                return Err(Recover::session(stopped.state, stopped.cause.into())
                    .map_err(Self::Cleanup::probe));
            }
        };
        let state = match state.option(T::Context::OPTION) {
            Ok(state) => state,
            Err(stopped) => {
                return Err(Recover::option(stopped.state, stopped.cause.into())
                    .map_err(Self::Cleanup::probe));
            }
        };
        let stopped = match state.close(T::Context::CLOSE) {
            Ok(state) => return Ok(state),
            Err(stopped) => stopped,
        };
        Err(Recover::socket(stopped.state, stopped.cause.into()).map_err(Self::Cleanup::probe))
    }

    fn finish(self) -> Result<Ring<T>, Result<P, Self::Cleanup>> {
        let Self {
            mut owner,
            file: (),
        } = self;
        if let Err(cause) = owner.owner.registry().unbuffer(GROUP) {
            return Err(Recover::session(
                Self { owner, file: () },
                Setup::Unregister(cause).into(),
            )
            .map_err(Self::Cleanup::probe));
        }
        let register::raw::Buffered {
            owner: ring,
            storage: canary,
        } = owner;
        let state = match canary.try_close() {
            Ok(()) => return Ok(ring),
            Err(state) => state,
        };
        let primary = state.failure().into();
        Err(Err(Self::Cleanup::unmap(ring, primary, state)))
    }
}

impl<T: token::Codec, L: memory::raw::Canary> session::Direct for Session<T, L, Direct> {
    type Empty = Session<T, L>;

    fn option(self, probe: Identity<T>) -> Result<Self, Stopped<Stage<T, L, Direct>, Fault<T>>> {
        use dope_sys::{memory::buffer::canary::raw::Storage as _, probe::raw::Wait as _};
        // SAFETY: this idle session owns the stable scalar mapping; the returned
        // Pending only permits native progress and retains the exact session.
        let waiting = unsafe {
            Waiting::option(
                self,
                |state| {
                    let (canary, ring) = state.owner.split();
                    (ring, SLOT, canary.option())
                },
                probe,
            )
        }
        .map_err(Stopped::idle)?;
        let (state, done) = Pending::new(waiting).next();
        let (state, done) = Stopped::attach(state, done)?;
        match state {
            State::Idle(state) => {
                let (state, ()) = Stopped::attach(state, done.zero().map_err(Cause::Check))
                    .map_err(Stopped::idle)?;
                Ok(state)
            }
            State::Active(waiting) => Err(Stopped {
                state: State::Active(waiting),
                cause: Cause::Check(done.unexpected()),
            }),
        }
    }

    fn close(self, probe: Identity<T>) -> Result<Self::Empty, Stopped<Self, Fault<T>>> {
        use dope_sys::probe::raw::Wait as _;
        let entry = Uring::close(SLOT, T::probe(probe));
        let (state, done) = Waiting::submit(self, Self::ring, &entry, probe)?.finish(Self::ring)?;
        let (state, ()) = Stopped::attach(state, done.zero().map_err(Cause::Check))?;
        Ok(Session {
            owner: state.owner,
            file: (),
        })
    }
}

impl<T: token::Codec, L: memory::raw::Canary> session::Occupied
    for Session<T, L, register::raw::Occupied>
{
    type Empty = Session<T, L>;
    type Target = sq::Fixed;
    type Remove = kernel::Errno<error::Register<6>>;
    type Index = cq::Buffer;
    type Length = <L::Extent as memory::raw::Extent>::Length;
    type Capacity = <L::Extent as memory::raw::Extent>::Capacity;

    fn slot(&self) -> sq::Fixed {
        self.file.slot()
    }

    fn try_remove(self) -> Result<Self::Empty, Stopped<Self, Self::Remove>> {
        let Self { mut owner, file } = self;
        let failed = match owner.registry().try_remove(file) {
            Ok(()) => return Ok(Session { owner, file: () }),
            Err(failed) => failed,
        };
        Err(Stopped {
            state: Self {
                owner,
                file: failed.file,
            },
            cause: failed.failure,
        })
    }

    fn next<E>(
        waiting: Pending<T, L, register::raw::Occupied>,
        check: impl FnOnce(&[u8]) -> Result<(), E>,
    ) -> Result<
        Pending<T, L, register::raw::Occupied>,
        Stopped<Stage<T, L, register::raw::Occupied>, Inspected<T, L, E>>,
    > {
        use dope_sys::memory::buffer::canary::raw::Storage as _;
        use session::Checked;

        let (state, done) = waiting.next();
        let (state, done) = Stopped::attach(state, done).map_err(Stopped::convert)?;
        let probe = done.probe();
        let len: <L::Extent as memory::raw::Extent>::Length = match done.success() {
            Ok(value) => {
                let count: bounded::U32<0, { i32::MAX as u32 }> = value.into();
                count.into()
            }
            Err(error) => {
                return Err(Stopped {
                    state,
                    cause: Checked::Wait(Cause::Check(error)),
                });
            }
        };
        let Some(buffer) = done.meta().buffer() else {
            return Err(Stopped {
                state,
                cause: Checked::MissingBuffer(probe),
            });
        };
        match state {
            State::Active(waiting) => {
                let (waiting, checked) = waiting.inspect(buffer, len, check);
                match checked {
                    Ok(()) => Ok(waiting),
                    Err(failure) => Err(Stopped {
                        state: State::Active(waiting),
                        cause: Checked::Buffer { probe, failure },
                    }),
                }
            }
            State::Idle(mut state) => {
                // SAFETY: next consumed the unique terminal CQE for this session.
                let error = match unsafe { state.owner.storage_mut().inspect(buffer, len, check) } {
                    Ok(()) => Checked::Terminated(probe),
                    Err(failure) => Checked::Buffer { probe, failure },
                };
                Err(Stopped {
                    state: State::Idle(state),
                    cause: error,
                })
            }
        }
    }
}

impl<T, L: memory::raw::Canary, F> Close for Session<T, L, F> {
    type State = Closing<T, L, F>;

    fn try_close(self) -> Result<(), Self::State> {
        let Self { owner, file } = self;
        match owner.try_close() {
            Ok(()) => Ok(()),
            Err(state) => Err(Closing::new(file, state)),
        }
    }
}
