use crate::{io::socket::addr, memory};
pub mod accept;
mod check;
pub mod cleanup;
mod failure;
mod outcome;
pub mod raw;
pub mod receive;
pub mod session;
pub use check::Check;
pub use failure::Failure;
pub use outcome::Outcome;

/// Exact syscall or local-address failure while constructing probe sockets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Creation {
    Syscall(crate::Errno),
    Local(addr::Local<crate::Errno>),
}

impl From<crate::Errno> for Creation {
    fn from(error: crate::Errno) -> Self {
        Self::Syscall(error)
    }
}

impl From<addr::Local<crate::Errno>> for Creation {
    fn from(error: addr::Local<crate::Errno>) -> Self {
        Self::Local(error)
    }
}

/// A probe operation and its exact syscall failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Syscall<P, E> {
    pub probe: P,
    pub error: E,
}
use crate::{
    io::{
        Close,
        reactor::{complete, submit},
    },
    ring,
    ring::{Access, map},
    table::{token, token::host},
};

/// Stable host identities used by native capability checks.
pub trait Context: host::Context {
    const RING: Self::Probe;
    const SOCKET: Self::Probe;
    const OPTION: Self::Probe;
    const CLOSE: Self::Probe;
    const RECEIVE: Self::Probe;
    const MESSAGE: Self::Probe;
    const LISTENER: Self::Probe;
    const ACCEPT: Self::Probe;
    const MULTISHOT: Self::Probe;
    const ACCEPT_CLOSE: Self::Probe;
    const MULTI_CLOSE: Self::Probe;
    const RELEASE: Self::Probe;
}

/// Classifies native fixed-file removal failures without erasing their phase.
pub trait Build<E>: Context {
    type Primary: Copy;
    type Removed: Copy;

    fn remove(mode: receive::Mode, error: E) -> Self::Removed;
    fn release(probe: Self::Probe, error: E) -> Self::Removed;
}

/// Selects native canary checks without exposing native completion layouts.
pub trait Source: ring::Source + submit::Control {
    type Range: accept::Range<Target = Self::Target>;
    type Payload: receive::Receive;

    type Release<T: token::Codec, L: memory::raw::Canary, P: Copy>: cleanup::Release<
            P,
            Ring = Self::Ring<T>,
            File = <Self::Ring<T> as Access>::File,
            Session = <Self::Session<T, L> as session::Empty>::Occupied,
            Target = <Self::Ring<T> as Access>::Target,
            Error = map::Storage<<<Self::Ring<T> as Close>::State as map::Unmap>::Error>,
        >
    where
        Self: complete::Observe<T>;

    type Cleanup<T: token::Codec, L: memory::raw::Canary, P: Copy>: cleanup::Cleanup<
            Primary = P,
            Ring = Self::Ring<T>,
            File = <Self::Ring<T> as Access>::File,
            Accept = Self::Accept<T>,
            Session = Self::Session<T, L>,
            Error = cleanup::Failure<
                map::Storage<<<Self::Ring<T> as Close>::State as map::Unmap>::Error>,
                Cause<
                    <T::Context as host::Context>::Probe,
                    <Self as complete::Observe<T>>::Observation,
                    <Self::Accept<T> as accept::Accept>::Error,
                >,
            >,
        >
    where
        Self: complete::Observe<T>;

    type Accept<T: token::Codec>: accept::Accept<
            Ring = Self::Ring<T>,
            Owned = Self::Owned<T>,
            Probe = <T::Context as host::Context>::Probe,
            Outcome = Self::Outcome<T, <T::Context as host::Context>::Probe>,
        >
    where
        Self: complete::Observe<T>;

    type Session<T: token::Codec, L: memory::raw::Canary>: session::Empty<
            Ring = Self::Ring<T>,
            Layout = L,
            Owned = Self::Owned<T>,
            Probe = <T::Context as host::Context>::Probe,
            Outcome = Self::Outcome<T, <T::Context as host::Context>::Probe>,
        >
    where
        Self: complete::Observe<T>;

    type Outcome<T: complete::raw::Token, P: Copy>: Outcome<
            Probe = P,
            Observation = <Self as complete::Observe<T>>::Observation,
            Value = <Self::Range as accept::Range>::Value,
        >
    where
        Self: complete::Observe<T>;

    type Waiting<T: token::Codec, S>: raw::Wait<
            S,
            Ring = Self::Ring<T>,
            Owned = Self::Owned<T>,
            Probe = <T::Context as host::Context>::Probe,
            Outcome = Self::Outcome<T, <T::Context as host::Context>::Probe>,
        >
    where
        Self: complete::Observe<T>;
}

/// Validates an owned ring before its resources are exposed to a driver.
pub trait Validate<L: memory::raw::Canary, P: Copy>: Sized {
    type Probe: Copy;
    type Cleanup;

    fn support(self) -> Result<Self, Self::Cleanup>;
    fn empty(self, probe: Self::Probe) -> Result<Self, Self::Cleanup>;
}
use o3::base::num::bounded;

/// Exact kernel or completion evidence that stopped a capability canary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cause<P, O, E> {
    Enter {
        probe: P,
        error: E,
    },
    Missing(P),
    Unexpected {
        probe: P,
        observed: O,
    },
    Residual {
        probe: P,
        submit: bounded::U32<0, { u32::MAX }>,
        complete: bounded::U32<0, { u32::MAX }>,
    },
    Check(Failure<P, O>),
}

impl<P, O: Copy, E> Cause<P, O, E> {
    /// Returns only evidence whose completion remains at the queue head.
    pub fn unread(&self) -> Option<O> {
        match self {
            Self::Unexpected { observed, .. } => Some(*observed),
            Self::Check(failure) => failure.unread(),
            Self::Enter { .. } | Self::Missing(_) | Self::Residual { .. } => None,
        }
    }
}

/// The owner is returned only after the kernel reports terminal completion.
#[derive(Debug)]
#[must_use = "a submitted operation must be completed or canceled"]
pub enum State<S, W> {
    Idle(S),
    Active(W),
}

/// A stopped transition retains its current owner and the exact cause.
/// Unlike a rejected input, the owner may already contain committed progress.
#[derive(Debug)]
#[must_use = "resume or close the retained owner and report its cause"]
pub struct Stopped<S, E> {
    pub state: S,
    pub cause: E,
}

impl<S, E> Stopped<S, E> {
    pub fn attach<R>(state: S, result: Result<R, E>) -> Result<(S, R), Self> {
        let cause = match result {
            Ok(value) => return Ok((state, value)),
            Err(cause) => cause,
        };
        Err(Self { state, cause })
    }

    pub fn map<T>(self, map: impl FnOnce(S) -> T) -> Stopped<T, E> {
        Stopped {
            state: map(self.state),
            cause: self.cause,
        }
    }

    pub fn idle<W>(self) -> Stopped<State<S, W>, E> {
        self.map(State::Idle)
    }

    pub fn active<I>(self) -> Stopped<State<I, S>, E> {
        self.map(State::Active)
    }

    pub fn convert<F: From<E>>(self) -> Stopped<S, F> {
        Stopped {
            state: self.state,
            cause: F::from(self.cause),
        }
    }

    pub fn into_parts(self) -> (S, E) {
        (self.state, self.cause)
    }
}
