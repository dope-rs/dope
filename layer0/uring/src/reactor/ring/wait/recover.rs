use dope_sys::{
    io::Close,
    kernel, memory,
    probe::{
        Cause, State, Stopped, accept::Accept as _, cleanup, cleanup::Failure, session,
        session::Pending as _,
    },
    ring::{
        Access, Owner as _,
        map::{Storage, Unmap},
    },
    table::{token, token::host},
};

use crate::{
    Observed, Ring, error,
    reactor::{
        register, ring,
        ring::{Accepted, Outcome, Session},
    },
};

type Identity<T> = <<T as token::Codec>::Context as host::Context>::Probe;
type Fault<T> = Cause<Identity<T>, Observed<T>, kernel::Errno<error::Enter>>;
type Checked<T> = Outcome<T, Identity<T>>;
type Direct<T, L> = <Session<T, L> as session::Empty>::Direct;
type Occupied<T, L> = <Session<T, L> as session::Empty>::Occupied;
type Stage<S> = State<S, <S as session::Session>::Pending>;
type Closed<S> = <S as Close>::State;
type Error<T> = Failure<Storage<kernel::Errno<error::Unmap>>, Fault<T>>;

pub struct Recover<T: token::Codec, L: memory::raw::Canary, P> {
    primary: P,
    state: Pending<T, L>,
}

enum Pending<T: token::Codec, L: memory::raw::Canary> {
    Raw(ring::Closing<T>),
    File(register::Closing<T>),
    Session(Closed<Session<T, L>>),
    Receive(Closed<Occupied<T, L>>),
    Socket(Closed<Direct<T, L>>),
    Option(Stopped<Stage<Direct<T, L>>, Fault<T>>),
    Cancel(Stopped<Stage<Occupied<T, L>>, Fault<T>>),
}

impl<T: token::Codec, L: memory::raw::Canary, P: Copy> cleanup::Cleanup for Recover<T, L, P> {
    type Primary = P;
    type Ring = Ring<T>;
    type File = <Ring<T> as Access>::File;
    type Accept = Accepted<T>;
    type Session = Session<T, L>;
    type Error = Error<T>;

    fn try_close(self, mut report: impl FnMut(Error<T>)) -> Result<P, Self> {
        let Self { primary, state } = self;
        match state {
            Pending::Raw(state) => Self::retry(state, primary, Pending::Raw, &mut report),
            Pending::File(state) => Self::retry(state, primary, Pending::File, &mut report),
            Pending::Session(state) => Self::retry(state, primary, Pending::Session, &mut report),
            Pending::Receive(state) => Self::retry(state, primary, Pending::Receive, &mut report),
            Pending::Socket(state) => Self::retry(state, primary, Pending::Socket, &mut report),
            Pending::Option(stopped) => Self::resume(
                stopped,
                primary,
                Pending::Option,
                Pending::Socket,
                &mut report,
            ),
            Pending::Cancel(stopped) => Self::resume(
                stopped,
                primary,
                Pending::Cancel,
                Pending::Receive,
                &mut report,
            ),
        }
    }

    fn raw(ring: Ring<T>, primary: P) -> Result<P, Self> {
        Self::closed(ring.try_close(), primary, Pending::Raw)
    }

    fn file(ring: Ring<T>, primary: P, file: Self::File) -> Result<P, Self> {
        Self::closed(ring.close_file(file), primary, Pending::File)
    }

    fn accept(state: Accepted<T>, primary: P, file: Self::File) -> Result<P, Self> {
        Self::closed(state.try_close(file), primary, Pending::File)
    }

    fn session(state: Session<T, L>, primary: P) -> Result<P, Self> {
        Self::closed(state.try_close(), primary, Pending::Session)
    }

    fn receive(state: Stage<Occupied<T, L>>, primary: P) -> Result<P, Self> {
        Self::pending(state, primary, Pending::Cancel, Pending::Receive)
    }

    fn socket(state: Direct<T, L>, primary: P) -> Result<P, Self> {
        Self::closed(state.try_close(), primary, Pending::Socket)
    }

    fn option(state: Stage<Direct<T, L>>, primary: P) -> Result<P, Self> {
        Self::pending(state, primary, Pending::Option, Pending::Socket)
    }

    fn primary(&self) -> P {
        self.primary
    }

    fn failure(&self) -> Error<T> {
        match &self.state {
            Pending::Raw(state) => Failure::Storage(state.failure()),
            Pending::File(state) => Failure::Storage(state.failure()),
            Pending::Session(state) => Failure::Storage(state.failure()),
            Pending::Receive(state) => Failure::Storage(state.failure()),
            Pending::Socket(state) => Failure::Storage(state.failure()),
            Pending::Option(stopped) => Failure::Wait(stopped.cause),
            Pending::Cancel(stopped) => Failure::Wait(stopped.cause),
        }
    }
}

impl<T: token::Codec, L: memory::raw::Canary, P: Copy> Recover<T, L, P> {
    fn closed<S>(
        result: Result<(), S>,
        primary: P,
        retain: impl FnOnce(S) -> Pending<T, L>,
    ) -> Result<P, Self> {
        let state = match result {
            Ok(()) => return Ok(primary),
            Err(state) => state,
        };
        Err(Self {
            primary,
            state: retain(state),
        })
    }

    fn retry<S: Unmap<Error = kernel::Errno<error::Unmap>>>(
        state: S,
        primary: P,
        retain: impl FnOnce(S) -> Pending<T, L>,
        report: &mut impl FnMut(Error<T>),
    ) -> Result<P, Self> {
        let (owner, cause) = state.recover().into_parts();
        report(Failure::Storage(cause.into()));
        Self::closed(owner.try_close(), primary, retain)
    }

    fn resume<S>(
        stopped: Stopped<Stage<S>, Fault<T>>,
        primary: P,
        active: impl FnOnce(Stopped<Stage<S>, Fault<T>>) -> Pending<T, L>,
        closed: impl FnOnce(Closed<S>) -> Pending<T, L>,
        report: &mut impl FnMut(Error<T>),
    ) -> Result<P, Self>
    where
        S: session::Session<
                Probe = Identity<T>,
                Outcome = Checked<T>,
                Error = kernel::Errno<error::Enter>,
            >,
    {
        let state = match stopped.state {
            State::Idle(state) => {
                report(Failure::Wait(stopped.cause));
                State::Idle(state)
            }
            State::Active(state) => {
                let result = S::Pending::resume(
                    Stopped {
                        state,
                        cause: stopped.cause,
                    },
                    |cause| report(Failure::Wait(cause)),
                );
                let state = match result {
                    Ok(state) => state,
                    Err(failed) => {
                        return Err(Self {
                            primary,
                            state: active(failed.active()),
                        });
                    }
                };
                State::Active(state)
            }
        };
        Self::pending(state, primary, active, closed)
    }

    fn pending<S>(
        state: Stage<S>,
        primary: P,
        active: impl FnOnce(Stopped<Stage<S>, Fault<T>>) -> Pending<T, L>,
        closed: impl FnOnce(Closed<S>) -> Pending<T, L>,
    ) -> Result<P, Self>
    where
        S: session::Session<
                Probe = Identity<T>,
                Outcome = Checked<T>,
                Error = kernel::Errno<error::Enter>,
            >,
    {
        let state = match state {
            State::Idle(state) => state,
            State::Active(state) => match state.cancel() {
                Ok(state) => state,
                Err(failure) => {
                    return Err(Self {
                        primary,
                        state: active(failure),
                    });
                }
            },
        };
        Self::closed(state.try_close(), primary, closed)
    }
}
