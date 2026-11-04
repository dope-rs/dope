use dope_net::{
    link::pool::recv::{delivery, input},
    wire::{self, receive},
};

use crate::app::receive::{
    Batch, GatedScoped, Mode, Owned, Scoped,
    ingress::{self, Strategy},
};

pub(in crate::app::receive::ingress) mod flow;

pub(crate) trait Dispatch<W: wire::Spec>: Mode<W> + Sized {
    /// Statically selected admission and delivery strategy for this mode.
    type Strategy: Strategy<W, Self>;
}

impl<W, I> Dispatch<W> for I
where
    W: wire::Spec,
    I: Batch + input::Mode<W>,
    <I as input::Mode<W>>::Kind: delivery::BorrowedData,
{
    type Strategy = ingress::Batched;
}

impl<W> Dispatch<W> for Scoped
where
    W: wire::Spec,
    Scoped: input::Mode<W>,
    <Scoped as input::Mode<W>>::Kind: delivery::ScopedData + input::Replay<W>,
{
    type Strategy = ingress::Scope;
}

impl<W> Dispatch<W> for GatedScoped
where
    W: wire::Spec,
    GatedScoped: input::Mode<W>,
    <GatedScoped as input::Mode<W>>::Kind: delivery::ScopedData + input::Replay<W>,
{
    type Strategy = ingress::Scope;
}

impl<W> Dispatch<W> for Owned
where
    W: input::Execution,
    Owned: input::Mode<W, Kind = input::Retained>,
    <W::Receive as receive::Policy<W>>::Credit: delivery::Ownership<W>,
{
    type Strategy = ingress::Retain;
}
