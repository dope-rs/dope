use std::{marker, mem};

use o3::base::marker::ThreadBound;

use crate::wire::{self, failure};

type Cause<'d, W> =
    <<W as wire::Spec>::TerminalFailure as failure::Class<W, failure::Terminal>>::Error<'d>;

/// The first wire failure and any failure while draining its terminal output.
pub struct Error<'d, W: wire::Spec + 'd> {
    cause: Cause<'d, W>,
    closing: Option<Cause<'d, W>>,
}

impl<'d, W: wire::Spec + 'd> Error<'d, W> {
    pub const fn from_cause(cause: Cause<'d, W>) -> Self {
        Self {
            cause,
            closing: None,
        }
    }

    pub const fn causes(&self) -> (&Cause<'d, W>, Option<&Cause<'d, W>>) {
        (&self.cause, self.closing.as_ref())
    }

    pub fn into_parts(self) -> (Cause<'d, W>, Option<Cause<'d, W>>) {
        (self.cause, self.closing)
    }
}

impl<'d, W> Error<'d, W>
where
    W: wire::Spec<TerminalFailure = failure::Never> + 'd,
{
    /// Eliminates a terminal error whose wire failure class is uninhabited.
    pub fn eliminate<T>(self) -> T {
        match self.cause {}
    }
}

/// Affine permission to record one failure while draining terminal output.
#[repr(transparent)]
pub struct Drain<'a, 'd, W: wire::Spec + 'd>(
    &'a mut Option<Cause<'d, W>>,
    marker::PhantomData<ThreadBound>,
);

impl<'d, W: wire::Spec + 'd> Drain<'_, 'd, W> {
    pub fn fail(self, cause: Cause<'d, W>) {
        *self.0 = Some(cause);
    }
}

/// Output produced after an owner-bound terminal admission was resolved.
#[must_use]
pub struct Processed<T> {
    output: T,
    captured: Captured,
}

impl<T> Processed<T> {
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> Processed<U> {
        Processed {
            output: map(self.output),
            captured: self.captured,
        }
    }

    pub(crate) fn into_parts(self) -> (T, Captured) {
        (self.output, self.captured)
    }
}

#[must_use]
#[derive(Clone, Copy)]
pub(crate) enum Captured {
    None,
    Eof,
    Terminal,
}

impl Captured {
    pub(crate) fn resolve<T>(self, output: T, terminal: impl FnOnce(T) -> T) -> T {
        match self {
            Self::None | Self::Eof => output,
            Self::Terminal => terminal(output),
        }
    }
}

/// The terminal state observed by a wire transition.
#[must_use]
pub enum Terminal<E> {
    Clean,
    Failed(E),
}

/// Allocation-free terminal discriminator used before the owned cause is moved.
pub enum Kind {
    /// The wire reached a clean terminal state.
    Clean,
    /// The wire retained an exact terminal failure.
    Failed,
}

impl<E> Terminal<E> {
    pub const fn kind(&self) -> Kind {
        match self {
            Self::Clean => Kind::Clean,
            Self::Failed(_) => Kind::Failed,
        }
    }

    pub fn map<F>(self, map: impl FnOnce(E) -> F) -> Terminal<F> {
        match self {
            Self::Clean => Terminal::Clean,
            Self::Failed(cause) => Terminal::Failed(map(cause)),
        }
    }
}

enum Phase<E> {
    Open,
    Eof,
    ReadClosed,
    BothClean,
    BothFailed(E),
    RecvClean,
    RecvFailed(E),
    SendClean,
    SendFailed(E),
    Posted,
    ReadyClean,
    ReadyFailed(E),
    Done,
}

/// Connection-owned convergence state for receive and send termination.
pub struct State<E>(Phase<E>);

/// Affine permission to commit the first send-side terminal value.
#[repr(transparent)]
pub struct Send<'a, E>(&'a mut State<E>, marker::PhantomData<ThreadBound>);

/// Affine permission to end input or commit the first bidirectional terminal value.
#[repr(transparent)]
pub struct Both<'a, E>(&'a mut State<E>, marker::PhantomData<ThreadBound>);

#[must_use]
pub(crate) enum Admission<T> {
    Open(T),
    Closed,
}

#[must_use]
pub(crate) enum Wake {
    Idle,
    Ready,
}

/// Receive-side observation of terminal convergence.
#[must_use]
pub enum Receive<E> {
    /// No terminal convergence is active.
    Idle,
    /// Authenticated input ended without this transition ending output.
    /// Output may already have been closed locally.
    Eof,
    /// The peer direction must complete before the terminal can be delivered.
    Pending,
    /// The exact terminal value is ready for its final consumer.
    Ready(Terminal<E>),
}

impl<E> Send<'_, E> {
    pub fn output<T>(self, output: T) -> Processed<T> {
        Processed {
            output,
            captured: Captured::None,
        }
    }

    pub fn terminal<T>(self, output: T, end: Terminal<E>) -> Processed<T> {
        (self.0).0 = match end {
            Terminal::Clean => Phase::SendClean,
            Terminal::Failed(cause) => Phase::SendFailed(cause),
        };
        Processed {
            output,
            captured: Captured::Terminal,
        }
    }
}

impl<E> Both<'_, E> {
    pub fn output<T>(self, output: T) -> Processed<T> {
        Processed {
            output,
            captured: Captured::None,
        }
    }

    /// Ends input after this transition's produced plaintext is settled.
    /// The connection retains its send admission and exact receive owner.
    pub fn eof<T>(self, output: T) -> Processed<T> {
        (self.0).0 = Phase::Eof;
        Processed {
            output,
            captured: Captured::Eof,
        }
    }

    pub fn terminal<T>(self, output: T, end: Terminal<E>) -> Processed<T> {
        (self.0).0 = match end {
            Terminal::Clean => Phase::BothClean,
            Terminal::Failed(cause) => Phase::BothFailed(cause),
        };
        Processed {
            output,
            captured: Captured::Terminal,
        }
    }
}

impl<'d, W: wire::Spec + 'd> State<Error<'d, W>> {
    pub(crate) fn drain(&mut self) -> Option<Drain<'_, 'd, W>> {
        match &mut self.0 {
            Phase::BothFailed(error) | Phase::SendFailed(error) if error.closing.is_none() => {
                Some(Drain(&mut error.closing, marker::PhantomData))
            }
            _ => None,
        }
    }
}

impl<E> State<E> {
    pub(crate) const fn open() -> Self {
        Self(Phase::Open)
    }

    pub(crate) fn send(&mut self) -> Admission<Send<'_, E>> {
        if matches!(&self.0, Phase::Open | Phase::Eof | Phase::ReadClosed) {
            Admission::Open(Send(self, marker::PhantomData))
        } else {
            Admission::Closed
        }
    }

    pub(crate) fn both(&mut self) -> Admission<Both<'_, E>> {
        if self.is_open() {
            Admission::Open(Both(self, marker::PhantomData))
        } else {
            Admission::Closed
        }
    }

    fn is_open(&self) -> bool {
        matches!(&self.0, Phase::Open)
    }

    /// Stops further wire input while preserving the send direction.
    pub(crate) const fn is_read_done(&self) -> bool {
        matches!(&self.0, Phase::Eof | Phase::ReadClosed)
    }

    /// Reports an input end which still follows the produced plaintext.
    pub const fn has_eof(&self) -> bool {
        matches!(&self.0, Phase::Eof)
    }

    /// Commits receive completion and polls any now-deliverable terminal state.
    pub fn finish_recv(&mut self) -> Receive<E> {
        match mem::replace(&mut self.0, Phase::Open) {
            Phase::Eof => {
                self.0 = Phase::ReadClosed;
                Receive::Eof
            }
            phase @ (Phase::ReadClosed | Phase::Done) => {
                self.0 = phase;
                Receive::Idle
            }
            Phase::BothClean | Phase::SendClean => {
                self.0 = Phase::Posted;
                Receive::Ready(Terminal::Clean)
            }
            Phase::RecvClean | Phase::ReadyClean => {
                self.0 = Phase::Done;
                Receive::Ready(Terminal::Clean)
            }
            Phase::RecvFailed(cause) | Phase::ReadyFailed(cause) => {
                self.0 = Phase::Done;
                Receive::Ready(Terminal::Failed(cause))
            }
            Phase::BothFailed(cause) | Phase::SendFailed(cause) => {
                self.0 = Phase::SendFailed(cause);
                Receive::Pending
            }
            Phase::Posted => {
                self.0 = Phase::Posted;
                Receive::Pending
            }
            Phase::Open => Receive::Idle,
        }
    }

    pub(crate) fn finish_send(&mut self) -> Wake {
        match mem::replace(&mut self.0, Phase::Open) {
            Phase::Open => Wake::Idle,
            phase @ (Phase::Eof | Phase::ReadClosed | Phase::Done) => {
                self.0 = phase;
                Wake::Idle
            }
            Phase::BothClean | Phase::RecvClean => {
                self.0 = Phase::RecvClean;
                Wake::Idle
            }
            Phase::BothFailed(cause) | Phase::RecvFailed(cause) => {
                self.0 = Phase::RecvFailed(cause);
                Wake::Idle
            }
            Phase::SendClean | Phase::ReadyClean => {
                self.0 = Phase::ReadyClean;
                Wake::Ready
            }
            Phase::SendFailed(cause) | Phase::ReadyFailed(cause) => {
                self.0 = Phase::ReadyFailed(cause);
                Wake::Ready
            }
            Phase::Posted => {
                self.0 = Phase::Done;
                Wake::Ready
            }
        }
    }

    /// Returns whether terminal send work remains owned by the connection.
    pub const fn has_send(&self) -> bool {
        matches!(
            &self.0,
            Phase::BothFailed(_) | Phase::SendFailed(_) | Phase::Posted
        )
    }

    /// Returns whether input notification or terminal convergence is still pending.
    pub const fn is_pending(&self) -> bool {
        !matches!(&self.0, Phase::Open | Phase::ReadClosed | Phase::Done)
    }

    /// Returns whether both directions completed terminal convergence.
    pub const fn is_done(&self) -> bool {
        matches!(&self.0, Phase::Done)
    }

    /// Returns the retained terminal class while convergence is active.
    pub const fn kind(&self) -> Option<Kind> {
        match &self.0 {
            Phase::Open | Phase::Eof | Phase::ReadClosed | Phase::Done => None,
            Phase::BothClean
            | Phase::RecvClean
            | Phase::SendClean
            | Phase::Posted
            | Phase::ReadyClean => Some(Kind::Clean),
            Phase::BothFailed(_)
            | Phase::RecvFailed(_)
            | Phase::SendFailed(_)
            | Phase::ReadyFailed(_) => Some(Kind::Failed),
        }
    }
}

const _: () = assert!(mem::size_of::<Wake>() == 1);
const _: () = assert!(mem::size_of::<Processed<()>>() == 1);
const _: () =
    assert!(mem::size_of::<Drain<'static, 'static, wire::Identity>>() == mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Send<'static, ()>>() == mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Both<'static, ()>>() == mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Admission<Send<'static, ()>>>() == mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<State<usize>>() <= 2 * mem::size_of::<usize>());
