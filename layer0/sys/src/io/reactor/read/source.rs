use crate::{
    io::reactor::{
        read::{Arm, Complete, Context},
        ready,
    },
    memory,
    memory::buffer,
    table,
    table::{change, registry},
};

pub trait Source: buffer::Source + table::Source {
    type Registry<C: Context>: registry::State<C::Slots, Self::Entry<C>, Files = Self::Files<C::Slots>>;
    type Entry<C: Context>: super::raw::Entry<C, File = <<Self::Files<C::Slots> as table::Files<C::Slots>>::Changes as change::Changes>::Id>;
    type Arm<'a, C, E, P>: Arm<
            'a,
            C,
            E,
            P,
            Files = Self::Files<C::Slots>,
            Entry = Self::Entry<C>,
            Registry = Self::Registry<C>,
            Failure = P::Failure,
        >
    where
        C: Context + 'a,
        C::Slots: 'a,
        E: Complete<Key = C::Key, Flow = C::Flow>,
        P: ready::Poll<
                C::Slots,
                Files = Self::Files<C::Slots>,
                Failure: ready::Failure<Operation = C::Operation>,
            > + 'a,
        Self::Files<C::Slots>: 'a,
        Self::Registry<C>: 'a;
    type Poll<'a, C, L, E, P>: super::raw::Poll<'a, C, L, E, P, Arm = Self::Arm<'a, C, E, P>, Pool = Self::Pool<L>>
    where
        C: Context + 'a,
        C::Slots: 'a,
        L: memory::raw::Layout + 'a,
        E: Complete<
                Key = C::Key,
                Flow = C::Flow,
                Accepted = <C::Slots as table::raw::Slots>::Accepted,
                Buffer = <Self::Pool<L> as buffer::Pool>::Buffer,
            >,
        P: ready::Poll<
                C::Slots,
                Files = Self::Files<C::Slots>,
                Failure: ready::Failure<Operation = C::Operation>,
            > + 'a,
        Self::Files<C::Slots>: 'a,
        Self::Registry<C>: 'a,
        Self::Pool<L>: 'a;
}
