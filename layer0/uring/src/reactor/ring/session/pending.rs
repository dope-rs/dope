use dope_sys::{
    memory,
    memory::buffer::canary,
    probe::{State, Stopped, session},
    table::token,
};

use crate::reactor::{
    cq, register,
    ring::{
        Session, Waiting,
        session::{Checked, Fault, Stage},
    },
};

type Progression<T, L, F> = (Stage<T, L, F>, Result<Checked<T>, Fault<T>>);

type Native<T, L, F> = Waiting<Session<T, L, F>, T>;
type Progress<T, L, F> = State<Session<T, L, F>, Native<T, L, F>>;
type Inspection<L, E> = canary::Failure<
    cq::Buffer,
    <<L as memory::raw::Receive>::Extent as memory::raw::Extent>::Length,
    <<L as memory::raw::Receive>::Extent as memory::raw::Extent>::Capacity,
    E,
>;

/// Pending sessions permit cancellation while retaining their exact native owner.
#[repr(transparent)]
pub struct Pending<T: token::Codec, L: memory::raw::Canary, F>(Waiting<Session<T, L, F>, T>);

impl<T: token::Codec, L: memory::raw::Canary, F> Pending<T, L, F> {
    pub(super) fn new(waiting: Waiting<Session<T, L, F>, T>) -> Self {
        Self(waiting)
    }

    fn retained<E>(failed: Stopped<Native<T, L, F>, E>) -> Stopped<Self, E> {
        failed.map(Self)
    }

    pub(super) fn stage(state: Progress<T, L, F>) -> Stage<T, L, F> {
        match state {
            State::Idle(state) => State::Idle(state),
            State::Active(waiting) => State::Active(Self(waiting)),
        }
    }

    fn stopped(failed: Stopped<Progress<T, L, F>, Fault<T>>) -> Stopped<Stage<T, L, F>, Fault<T>> {
        failed.map(Self::stage)
    }

    pub(super) fn next(self) -> Progression<T, L, F> {
        use dope_sys::probe::raw::Wait as _;
        match self.0.next(Session::ring) {
            Ok((state, done)) => (Self::stage(state), Ok(done)),
            Err(Stopped { state, cause }) => (Self::stage(state), Err(cause)),
        }
    }
}

impl<T: token::Codec, L: memory::raw::Canary> Pending<T, L, register::raw::Occupied> {
    pub(super) fn inspect<E>(
        self,
        buffer: cq::Buffer,
        len: <L::Extent as memory::raw::Extent>::Length,
        check: impl FnOnce(&[u8]) -> Result<(), E>,
    ) -> (Self, Result<(), Inspection<L, E>>) {
        use dope_sys::{memory::buffer::canary::raw::Storage as _, probe::raw::Wait as _};
        // SAFETY: only Occupied::next calls this after consuming its unique CQE;
        // inspection retains the exact owner and all outstanding kernel borrows.
        match unsafe {
            self.0
                .inspect(|state| state.owner.storage_mut().inspect(buffer, len, check))
        } {
            Ok(state) => (Self(state), Ok(())),
            Err(Stopped { state, cause }) => (Self(state), Err(cause)),
        }
    }
}

impl<T: token::Codec, L: memory::raw::Canary, F> session::Pending for Pending<T, L, F> {
    type Session = Session<T, L, F>;

    fn resume(
        stopped: Stopped<Self, Fault<T>>,
        report: impl FnMut(Fault<T>),
    ) -> Result<Self, Stopped<Self, Fault<T>>> {
        use dope_sys::probe::raw::Wait as _;
        Waiting::resume(stopped.map(|pending| pending.0), Session::ring, report)
            .map(Self)
            .map_err(Self::retained)
    }

    fn cancel(self) -> Result<Self::Session, Stopped<Stage<T, L, F>, Fault<T>>> {
        use dope_sys::probe::raw::Wait as _;
        self.0.cancel(Session::ring).map_err(Self::stopped)
    }
}
