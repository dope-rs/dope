use std::marker;

use dope_sys::{
    io::{Close, Retry},
    memory,
    memory::{Error, Failure, buffer::provided},
    probe::Stopped,
};
use o3::mem::Rejected;

use crate::reactor::{
    buffers::{self, Entry, Producer},
    mappings,
};

#[derive(Debug)]
pub struct Cleanup<L: memory::raw::Receive>(Rejected<Pending<L>, Error>);

#[derive(Debug)]
pub enum Create<L: memory::raw::Receive> {
    Failure(Error),
    Cleanup { primary: Error, cleanup: Cleanup<L> },
}

#[derive(Debug)]
pub struct Open<L> {
    state: mappings::Open,
    layout: marker::PhantomData<fn(L) -> L>,
}

/// Live mappings after a failure has been returned to their owning caller.
#[derive(Debug)]
pub struct Pending<L: memory::raw::Receive>(State<L>);

#[derive(Debug)]
enum State<L: memory::raw::Receive> {
    Entries {
        current: mappings::Mapping<Entry>,
        context: buffers::raw::Context<L>,
    },
    Region(mappings::Region),
    RegionContext {
        current: mappings::Region,
        context: buffers::raw::Context<L>,
    },
    Payload(mappings::Mapping<u8>),
    Producer(mappings::Mapping<Entry>),
}

impl<L: memory::raw::Receive> provided::Cleanup for Cleanup<L> {
    type Context = buffers::raw::Context<L>;
    type Producer = Producer<L>;

    fn entries(
        current: <mappings::Mapping<Entry> as Close>::State,
        context: buffers::raw::Context<L>,
    ) -> Self {
        Self(current.map(|current| Pending(State::Entries { current, context })))
    }

    fn payload(current: <mappings::Mapping<u8> as Close>::State) -> Self {
        Self(current.map(|current| Pending(State::Payload(current))))
    }

    fn producer(current: <mappings::Mapping<Entry> as Close>::State) -> Self {
        Self(current.map(|current| Pending(State::Producer(current))))
    }
}

impl<L: memory::raw::Receive>
    From<Stopped<buffers::raw::Context<L>, <mappings::Mapping<Entry> as Close>::State>>
    for Cleanup<L>
{
    fn from(
        stopped: Stopped<buffers::raw::Context<L>, <mappings::Mapping<Entry> as Close>::State>,
    ) -> Self {
        use provided::Cleanup as _;
        Self::entries(stopped.cause, stopped.state)
    }
}

impl<L: memory::raw::Receive>
    From<Stopped<buffers::raw::Context<L>, <mappings::Region as Close>::State>> for Cleanup<L>
{
    fn from(
        stopped: Stopped<buffers::raw::Context<L>, <mappings::Region as Close>::State>,
    ) -> Self {
        Self::with_region(stopped.cause, stopped.state)
    }
}

impl<L: memory::raw::Receive> Failure for Cleanup<L> {
    fn failure(&self) -> Error {
        *self.0.error()
    }
}

impl<L: memory::raw::Receive> Retry for Cleanup<L> {
    type Failure = Error;
    type Owner = Pending<L>;

    fn recover(self) -> Rejected<Self::Owner, Error> {
        self.0
    }
}

impl<L: memory::raw::Receive> Close for Pending<L> {
    type State = Cleanup<L>;

    fn try_close(self) -> Result<(), Self::State> {
        use provided::Cleanup as _;

        match self.0 {
            State::Entries { current, context } => {
                let (context, ()) = Stopped::attach(context, current.try_close())?;
                context.try_close().map_err(Cleanup::payload)
            }
            State::Region(current) => current.try_close().map_err(Cleanup::region),
            State::RegionContext { current, context } => {
                let (context, ()) = Stopped::attach(context, current.try_close())?;
                context.try_close().map_err(Cleanup::payload)
            }
            State::Payload(current) => current.try_close().map_err(Cleanup::payload),
            State::Producer(current) => current.try_close().map_err(Cleanup::producer),
        }
    }
}

impl<L: memory::raw::Receive> Cleanup<L> {
    fn region(current: <mappings::Region as Close>::State) -> Self {
        Self(current.map(|current| Pending(State::Region(current))))
    }

    fn with_region(
        current: <mappings::Region as Close>::State,
        context: buffers::raw::Context<L>,
    ) -> Self {
        Self(current.map(|current| Pending(State::RegionContext { current, context })))
    }

    pub(super) fn close(
        producer: Producer<L>,
        context: buffers::raw::Context<L>,
    ) -> Result<(), Self> {
        use provided::Cleanup as _;

        let (context, ()) = Stopped::attach(context, producer.try_close())?;
        context.try_close().map_err(Self::payload)
    }
}

impl<L> From<mappings::Open> for Open<L> {
    fn from(state: mappings::Open) -> Self {
        Self {
            state,
            layout: marker::PhantomData,
        }
    }
}

impl<L: memory::raw::Receive> provided::Failure for Open<L> {
    type Cleanup = Cleanup<L>;

    fn into_parts(self) -> (Error, Option<Self::Cleanup>) {
        match self.state {
            mappings::Open::Failure(primary) => (primary, None),
            mappings::Open::Cleanup { primary, current } => {
                (primary, Some(Cleanup::region(current)))
            }
        }
    }
}

impl<L: memory::raw::Receive> provided::Open for Open<L> {
    type Context = buffers::raw::Context<L>;

    fn close(self, context: buffers::raw::Context<L>) -> Create<L> {
        use provided::Cleanup as _;

        match self.state {
            mappings::Open::Failure(primary) => match context.try_close() {
                Ok(()) => Create::Failure(primary),
                Err(current) => Create::Cleanup {
                    primary,
                    cleanup: Cleanup::payload(current),
                },
            },
            mappings::Open::Cleanup { primary, current } => Create::Cleanup {
                primary,
                cleanup: Cleanup::with_region(current, context),
            },
        }
    }
}

impl<L: memory::raw::Receive> From<Open<L>> for Create<L> {
    fn from(state: Open<L>) -> Self {
        Self::from(state.state)
    }
}

impl<L: memory::raw::Receive> From<mappings::Open> for Create<L> {
    fn from(state: mappings::Open) -> Self {
        match state {
            mappings::Open::Failure(primary) => Self::Failure(primary),
            mappings::Open::Cleanup { primary, current } => Self::Cleanup {
                primary,
                cleanup: Cleanup::region(current),
            },
        }
    }
}

impl<L: memory::raw::Receive> provided::Failure for Create<L> {
    type Cleanup = Cleanup<L>;

    fn into_parts(self) -> (Error, Option<Cleanup<L>>) {
        match self {
            Self::Failure(primary) => (primary, None),
            Self::Cleanup { primary, cleanup } => (primary, Some(cleanup)),
        }
    }
}
