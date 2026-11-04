use std::num;

use crate::{
    io::reactor::ready::{
        Failure, Poll,
        store::{Context, State},
    },
    table,
};

pub trait Source: table::Source {
    const LANES: num::NonZero<u8>;

    type State<C: Context>: State<C>;
    type Poll<S: table::raw::Slots, F: Failure<Index = S::Index, Missing = S::Missing>>: Poll<S, Failure = F, Files = Self::Files<S>>;
}
