use dope_sys::{
    engine::{ring::Context, shutdown},
    io::{Close, Retry},
    memory::Failure as _,
    probe::Stopped,
    ring::map::Unmap,
};
use o3::mem::Rejected;

use crate::{driver, reactor::buffers};

type Buffer<C> = buffers::raw::Context<<C as Context>::Layout>;
type State<C> = <Buffer<C> as Close>::State;

/// A failed phase retains its receive storage until native ownership has ended.
pub enum Shutdown<C: Context> {
    Backend {
        state: driver::Closing<C>,
        buffer: Buffer<C>,
    },
    Context(State<C>),
}

/// Live shutdown owners after the previous typed failure reached the caller.
pub enum Pending<C: Context> {
    Backend {
        owner: <driver::Closing<C> as Retry>::Owner,
        buffer: Buffer<C>,
    },
    Context(<State<C> as Retry>::Owner),
}

impl<C: Context> From<Stopped<Buffer<C>, driver::Closing<C>>> for Shutdown<C> {
    fn from(stopped: Stopped<Buffer<C>, driver::Closing<C>>) -> Self {
        let Stopped {
            state: buffer,
            cause: state,
        } = stopped;
        Self::Backend { state, buffer }
    }
}

impl<C: Context> Retry for Shutdown<C> {
    type Failure = shutdown::Failure<<driver::Closing<C> as Unmap>::Error>;
    type Owner = Pending<C>;

    fn recover(self) -> Rejected<Self::Owner, Self::Failure> {
        match self {
            Self::Backend { state, buffer } => {
                let (owner, cause) = state.recover().into_parts();
                Rejected::new(
                    Pending::Backend { owner, buffer },
                    shutdown::Failure::Backend(cause.into()),
                )
            }
            Self::Context(state) => {
                let (owner, cause) = state.recover().into_parts();
                Rejected::new(Pending::Context(owner), shutdown::Failure::Context(cause))
            }
        }
    }
}

impl<C: Context> Close for Pending<C> {
    type State = Shutdown<C>;

    fn try_close(self) -> Result<(), Self::State> {
        match self {
            Self::Backend { owner, buffer } => {
                let (buffer, ()) = Stopped::attach(buffer, owner.try_close())?;
                buffer.try_close().map_err(Shutdown::Context)
            }
            Self::Context(owner) => owner.try_close().map_err(Shutdown::Context),
        }
    }
}

impl<C: Context> shutdown::Cleanup for Shutdown<C> {
    type Error = <driver::Closing<C> as Unmap>::Error;

    fn failure(&self) -> shutdown::Failure<Self::Error> {
        match self {
            Self::Backend { state, buffer: _ } => shutdown::Failure::Backend(state.failure()),
            Self::Context(state) => shutdown::Failure::Context(state.failure()),
        }
    }
}
