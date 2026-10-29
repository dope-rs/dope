use dope_sys::{
    io::Close,
    kernel, memory,
    probe::{
        cleanup, session,
        session::{Closed as _, Occupied as _},
    },
    ring::{
        Owner as _,
        map::{Storage, Unmap as _},
        register::Occupied as _,
    },
    table::token,
};

use crate::{
    Ring, error,
    reactor::{register, ring::Session, sq},
};

type Occupied<T, L> = <Session<T, L> as session::Empty>::Occupied;
type Closed<T, L> = <Occupied<T, L> as Close>::State;

pub struct Released<T: token::Codec, L: memory::raw::Canary, P> {
    primary: P,
    state: State<T, L>,
}

enum State<T: token::Codec, L: memory::raw::Canary> {
    Raw(register::Closing<T>),
    Session(Closed<T, L>),
}

impl<T: token::Codec, L: memory::raw::Canary, P: Copy> cleanup::Release<P> for Released<T, L, P> {
    type Ring = Ring<T>;
    type File = register::raw::Occupied;
    type Session = Occupied<T, L>;
    type Target = sq::Fixed;

    fn raw(ring: Ring<T>, file: Self::File, primary: P) -> Result<(P, sq::Fixed), Self> {
        let slot = file.slot();
        match ring.close_file(file) {
            Ok(()) => Ok((primary, slot)),
            Err(state) => Err(Self {
                primary,
                state: State::Raw(state),
            }),
        }
    }

    fn session(session: Self::Session, primary: P) -> Result<(P, sq::Fixed), Self> {
        let slot = session.slot();
        match session.try_close() {
            Ok(()) => Ok((primary, slot)),
            Err(state) => Err(Self {
                primary,
                state: State::Session(state),
            }),
        }
    }
}

impl<T: token::Codec, L: memory::raw::Canary, P: Copy> memory::Cleanup<(P, sq::Fixed)>
    for Released<T, L, P>
{
    type Error = Storage<kernel::Errno<error::Unmap>>;

    fn primary(&self) -> (P, sq::Fixed) {
        let slot = match &self.state {
            State::Raw(state) => state.slot(),
            State::Session(state) => state.slot(),
        };
        (self.primary, slot)
    }

    fn failure(&self) -> Self::Error {
        match &self.state {
            State::Raw(state) => state.failure(),
            State::Session(state) => state.failure(),
        }
    }

    fn try_close(self, mut report: impl FnMut(Self::Error)) -> Result<(P, sq::Fixed), Self> {
        match self.state {
            State::Raw(state) => Self::finish(state, self.primary, State::Raw, &mut report),
            State::Session(state) => Self::finish(state, self.primary, State::Session, &mut report),
        }
    }
}

impl<T: token::Codec, L: memory::raw::Canary, P: Copy> Released<T, L, P> {
    fn finish<S: session::Closed<Target = sq::Fixed, Error = kernel::Errno<error::Unmap>>>(
        state: S,
        primary: P,
        retain: impl FnOnce(S) -> State<T, L>,
        report: &mut impl FnMut(Storage<kernel::Errno<error::Unmap>>),
    ) -> Result<(P, sq::Fixed), Self> {
        let slot = state.slot();
        let (owner, cause) = state.recover().into_parts();
        report(cause.into());
        let state = match owner.try_close() {
            Ok(()) => return Ok((primary, slot)),
            Err(state) => state,
        };
        Err(Self {
            primary,
            state: retain(state),
        })
    }
}
