use dope_sys::{
    io,
    memory::{self, Failure as _},
    ring::map,
};
use o3::mem::Rejected;

use crate::reactor::register;

#[repr(transparent)]
pub struct Closing<P: io::Close, const GROUP: u16, R: io::Retry>(State<P, GROUP, R>);

enum State<P: io::Close, const GROUP: u16, R: io::Retry> {
    Ring(register::raw::Buffered<P, GROUP, R>),
    Storage(P::State),
}

pub struct Pending<P: io::Close, const GROUP: u16, R: io::Retry>(Live<P, GROUP, R>);

enum Live<P: io::Close, const GROUP: u16, R: io::Retry> {
    Ring(register::raw::Buffered<P, GROUP, R::Owner>),
    Storage(<P::State as io::Retry>::Owner),
}

impl<P: io::Close, const GROUP: u16, R: io::Retry> Closing<P, GROUP, R> {
    pub(crate) fn ring(owner: register::raw::Buffered<P, GROUP, R>) -> Self {
        Self(State::Ring(owner))
    }

    pub(crate) fn storage(state: P::State) -> Self {
        Self(State::Storage(state))
    }
}

impl<P: io::Close, const GROUP: u16, R: io::Retry> io::Retry for Closing<P, GROUP, R> {
    type Failure = io::Cleanup<R::Failure, <P::State as io::Retry>::Failure>;
    type Owner = Pending<P, GROUP, R>;

    fn recover(self) -> Rejected<Self::Owner, Self::Failure> {
        match self.0 {
            State::Ring(register::raw::Buffered { owner, storage }) => {
                let (owner, error) = owner.recover().into_parts();
                Rejected::new(
                    Pending(Live::Ring(register::raw::Buffered { owner, storage })),
                    io::Cleanup::Owner(error),
                )
            }
            State::Storage(state) => {
                let (state, error) = state.recover().into_parts();
                Rejected::new(Pending(Live::Storage(state)), io::Cleanup::Storage(error))
            }
        }
    }
}

impl<P: io::Close, const GROUP: u16, R: io::Retry> io::Close for Pending<P, GROUP, R> {
    type State = Closing<P, GROUP, R>;

    fn try_close(self) -> Result<(), Self::State> {
        match self.0 {
            Live::Ring(owner) => owner.try_close(),
            Live::Storage(state) => state.try_close().map_err(Closing::storage),
        }
    }
}

impl<P, const GROUP: u16, R> map::Unmap for Closing<P, GROUP, R>
where
    P: io::Close<State: memory::Failure + io::Retry<Failure: Into<memory::Error>>>,
    R: map::Unmap,
{
    type Error = R::Error;

    fn failure(&self) -> map::Storage<Self::Error> {
        match &self.0 {
            State::Ring(buffer) => buffer.owner.failure(),
            State::Storage(state) => map::Storage::Buffer(state.failure()),
        }
    }
}
