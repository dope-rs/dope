use dope_net::{
    link::pool::recv::{delivery, input},
    wire::{
        self,
        receive::{self, Chunk, batch, mode::Disabled},
    },
};

use crate::app::receive::{
    Batch, GatedScoped, Mode, Owned, Scoped,
    ingress::{self, Strategy},
};

pub(in crate::app::receive::ingress) mod flow;

pub(crate) trait Dispatch<W: wire::Spec>: Mode<W> + Sized {
    /// Statically selected admission and delivery strategy for this mode.
    type Strategy: Strategy<W, Self>;

    fn from_value<'input, 'w, 'd>(
        value: <Self::Strategy as Strategy<W, Self>>::Value<'input, 'w, 'd>,
    ) -> Self::Value<'input, 'w, 'd>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd;

    fn into_value<'input, 'w, 'd>(
        value: Self::Value<'input, 'w, 'd>,
    ) -> <Self::Strategy as Strategy<W, Self>>::Value<'input, 'w, 'd>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd;
}

impl<W, I> Dispatch<W> for I
where
    W: wire::Spec,
    I: Batch + input::Mode<W>,
    <I as input::Mode<W>>::Kind: delivery::BorrowedData,
{
    type Strategy = ingress::Batched;

    fn from_value<'input, 'w, 'd>(
        value: batch::Items<'input, W>,
    ) -> <Self as Mode<W>>::Value<'input, 'w, 'd>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd,
    {
        value
    }

    fn into_value<'input, 'w, 'd>(
        value: <Self as Mode<W>>::Value<'input, 'w, 'd>,
    ) -> batch::Items<'input, W>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd,
    {
        value
    }
}

impl<W> Dispatch<W> for Scoped
where
    W: wire::Spec,
    Scoped: input::Mode<W>,
    <Scoped as input::Mode<W>>::Kind: delivery::ScopedData + input::Replay<W>,
{
    type Strategy = ingress::Scope;

    fn from_value<'input, 'w, 'd>(
        value: Chunk<'w, 'd, W, Disabled>,
    ) -> <Self as Mode<W>>::Value<'input, 'w, 'd>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd,
    {
        value
    }

    fn into_value<'input, 'w, 'd>(
        value: <Self as Mode<W>>::Value<'input, 'w, 'd>,
    ) -> Chunk<'w, 'd, W, Disabled>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd,
    {
        value
    }
}

impl<W> Dispatch<W> for GatedScoped
where
    W: wire::Spec,
    GatedScoped: input::Mode<W>,
    <GatedScoped as input::Mode<W>>::Kind: delivery::ScopedData + input::Replay<W>,
{
    type Strategy = ingress::Scope;

    fn from_value<'input, 'w, 'd>(
        value: Chunk<'w, 'd, W, Disabled>,
    ) -> <Self as Mode<W>>::Value<'input, 'w, 'd>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd,
    {
        value
    }

    fn into_value<'input, 'w, 'd>(
        value: <Self as Mode<W>>::Value<'input, 'w, 'd>,
    ) -> Chunk<'w, 'd, W, Disabled>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd,
    {
        value
    }
}

impl<W> Dispatch<W> for Owned
where
    W: input::Execution,
    Owned: input::Mode<W, Kind = input::Retained>,
    <W::Receive as receive::Policy<W>>::Credit: delivery::Ownership<W>,
{
    type Strategy = ingress::Retain;

    fn from_value<'input, 'w, 'd>(
        value: Chunk<'w, 'd, W>,
    ) -> <Self as Mode<W>>::Value<'input, 'w, 'd>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd,
    {
        value
    }

    fn into_value<'input, 'w, 'd>(
        value: <Self as Mode<W>>::Value<'input, 'w, 'd>,
    ) -> Chunk<'w, 'd, W>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd,
    {
        value
    }
}
