use crate::{
    io::reactor::{
        ready,
        retry::{Complete, Context, Entry, State},
    },
    table::{self, change},
};

pub trait Source: table::Source {
    type State<E: Entry>: State<E>;
    type Send<C: Context>: super::raw::Send<C>;
    type Pending<C: Context>: Entry<
        File = <<Self::Files<C::Slots> as table::Files<C::Slots>>::Changes as change::Changes>::Id,
    >;
    type Retry<'a, C, E, P>: super::raw::Write<
            'a,
            C,
            E,
            P,
            Files = Self::Files<C::Slots>,
            Entry = Self::Pending<C>,
            State = Self::State<Self::Pending<C>>,
            Send = Self::Send<C>,
            Failure = P::Failure,
        >
    where
        C: Context + 'a,
        C::Slots: 'a,
        E: Complete<C>,
        P: ready::Poll<
                C::Slots,
                Files = Self::Files<C::Slots>,
                Failure: ready::Failure<Operation = C::Operation>,
            > + 'a,
        Self::Files<C::Slots>: 'a,
        Self::State<Self::Pending<C>>: 'a;
}
