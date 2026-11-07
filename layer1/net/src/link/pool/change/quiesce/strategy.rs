use dope_core::driver::{receive::credit, route::Identity};

use crate::{
    link::pool::{
        change::drain,
        recv::{self, storage, storage::tail},
    },
    wire::{
        self,
        receive::{Policy, mode, reserve},
    },
};

pub(in crate::link::pool::change) trait Strategy<W: wire::Spec>:
    Sized + storage::raw::Mode + drain::Strategy<W>
{
    fn credits<'a, 'd, ID: Identity, T, S, R>(
        _: &'a tail::Table<T, ID, S, recv::State<'d, ID, Self, R>>,
    ) -> Option<&'a credit::Storage<'d>> {
        None
    }
}

impl<W> Strategy<W> for mode::Disabled
where
    W: wire::Spec,
    <W::Receive as Policy<W>>::Reserve: reserve::Immediate<W>,
{
}

impl<W> Strategy<W> for mode::Parking
where
    W: wire::Spec,
    <W::Receive as Policy<W>>::Reserve: reserve::Immediate<W>,
{
}

impl<W> Strategy<W> for mode::Enabled
where
    W: wire::Spec,
    <W::Receive as Policy<W>>::Reserve: reserve::Immediate<W>,
{
    fn credits<'a, 'd, ID: Identity, T, S, R>(
        slab: &'a tail::Table<T, ID, S, recv::State<'d, ID, Self, R>>,
    ) -> Option<&'a credit::Storage<'d>> {
        Some(slab.credit())
    }
}

impl<W, R> Strategy<W> for mode::Waiting<R>
where
    W: wire::Spec,
    R: reserve::Try<W>,
    W::Receive: Policy<W, Reserve = reserve::Waiting<R>>,
{
    fn credits<'a, 'd, ID: Identity, T, S, C>(
        slab: &'a tail::Table<T, ID, S, recv::State<'d, ID, Self, C>>,
    ) -> Option<&'a credit::Storage<'d>> {
        Some(slab.credit())
    }
}
