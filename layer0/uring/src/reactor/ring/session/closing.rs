use dope_sys::{
    io::{Close, Retry},
    kernel, memory,
    probe::session,
    ring::{map, register::Occupied as _},
};
use o3::mem::Rejected;

use crate::{
    error,
    reactor::{buffers::Canary, register, ring, ring::session::GROUP, sq},
};

type State<T, L> = register::raw::Closing<Canary<L>, GROUP, ring::Closing<T>>;

pub struct Closing<T, L: memory::raw::Canary, F> {
    file: F,
    state: State<T, L>,
}

impl<T, L: memory::raw::Canary, F> Closing<T, L, F> {
    pub(super) fn new(file: F, state: State<T, L>) -> Self {
        Self { file, state }
    }
}

pub struct Owner<T, L: memory::raw::Canary, F> {
    file: F,
    state: <State<T, L> as Retry>::Owner,
}

impl<T, L: memory::raw::Canary, F> Close for Owner<T, L, F> {
    type State = Closing<T, L, F>;

    fn try_close(self) -> Result<(), Self::State> {
        let Self { file, state } = self;
        match state.try_close() {
            Ok(()) => Ok(()),
            Err(state) => Err(Closing { file, state }),
        }
    }
}

impl<T, L: memory::raw::Canary, F> Retry for Closing<T, L, F> {
    type Failure = <State<T, L> as Retry>::Failure;
    type Owner = Owner<T, L, F>;

    fn recover(self) -> Rejected<Self::Owner, Self::Failure> {
        let Self { file, state } = self;
        state.recover().map(|state| Owner { file, state })
    }
}

impl<T, L: memory::raw::Canary, F> map::Unmap for Closing<T, L, F> {
    type Error = kernel::Errno<error::Unmap>;

    fn failure(&self) -> map::Storage<Self::Error> {
        self.state.failure()
    }
}

impl<T, L: memory::raw::Canary> session::Closed for Closing<T, L, register::raw::Occupied> {
    type Target = sq::Fixed;

    fn slot(&self) -> sq::Fixed {
        self.file.slot()
    }
}
