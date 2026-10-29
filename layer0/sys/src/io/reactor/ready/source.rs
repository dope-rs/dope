use std::num;

use crate::{
    engine,
    io::reactor::ready::{
        self, Driver, Failure, Poll,
        store::{Context, Quota, State},
    },
    table,
};

pub trait Source: table::Source {
    const LANES: num::NonZero<u8>;

    type State<C: Context>: State<C>;
    type Work<C, Q>: ready::raw::Work<C, Q, Backend = <Self as engine::Source<C>>::Backend>
    where
        C: Driver<Context: ready::raw::Decode>,
        Q: Quota,
        Self: engine::Source<C>;
    type Poll<S: table::raw::Slots, F: Failure<Index = S::Index, Missing = S::Missing>>: Poll<S, Failure = F, Files = Self::Files<S>>;
}
