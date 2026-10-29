mod dispatch;
mod marker;
pub use dispatch::Dispatch;
pub(crate) use marker::Marker;
use receive::{mode, reserve};

use crate::{
    link::pool::recv::storage::tail,
    wire::{self, receive},
};

pub struct Borrowed;
pub struct Coupled;
pub struct Scoped;
pub struct Retained;

pub trait Execution: wire::Spec {
    type Borrowed: mode::Flag + Dispatch<Self>;
    type Parked: mode::Flag + Dispatch<Self>;
    type Retained: mode::Flag + Dispatch<Self>;
}

impl<W> Execution for W
where
    W: wire::Spec,
    <<W::Receive as receive::Policy<W>>::Reserve as reserve::Kind<W>>::Wait: Dispatch<W>,
    <<<W::Receive as receive::Policy<W>>::Reserve as reserve::Kind<W>>::Wait as mode::Flag>::Or<
        mode::Parking,
    >: Dispatch<W>,
    <<<<W::Receive as receive::Policy<W>>::Reserve as reserve::Kind<W>>::Wait as mode::Flag>::Or<
        <W::Receive as receive::Policy<W>>::Credit,
    > as mode::Flag>::WithParking: Dispatch<W>,
{
    type Borrowed = <<W::Receive as receive::Policy<W>>::Reserve as reserve::Kind<W>>::Wait;
    type Parked =
        <<<W::Receive as receive::Policy<W>>::Reserve as reserve::Kind<W>>::Wait as mode::Flag>::Or<
            mode::Parking,
        >;
    type Retained =
        <<<<W::Receive as receive::Policy<W>>::Reserve as reserve::Kind<W>>::Wait as mode::Flag>::Or<
            <W::Receive as receive::Policy<W>>::Credit,
        > as mode::Flag>::WithParking;
}

pub trait Mode<W: wire::Spec> {
    type Kind: Kind<W, Ingress = Self::Ingress>;

    type Ingress: mode::Flag + Dispatch<W>;
    type Reserve: reserve::Kind<W>;
}

/// Tail storage selected by one input mode.
pub(in crate::link::pool) type Tail<'w, 'd, W, M> = <<M as Mode<W>>::Kind as Kind<W>>::Tail<'w, 'd>;

pub trait Kind<W: wire::Spec>: Marker {
    type Ingress: mode::Flag + Dispatch<W>;

    type Tail<'w, 'd>: tail::Store
    where
        W: 'w,
        'd: 'w;
}

impl<W, M> Mode<W> for M
where
    W: wire::Spec,
    M: Kind<W>,
{
    type Kind = M;
    type Ingress = <M as Kind<W>>::Ingress;
    type Reserve = <W::Receive as receive::Policy<W>>::Reserve;
}

pub trait Replay<W: wire::Spec>: Kind<W> {
    fn queue<'a, 'w, 'd>(tail: &'a mut Self::Tail<'w, 'd>) -> &'a mut tail::Queued<'w, 'd, W>
    where
        W: 'w,
        'd: 'w;
}

impl<W: Execution> Kind<W> for Borrowed {
    type Ingress = W::Borrowed;

    type Tail<'w, 'd>
        = ()
    where
        W: 'w,
        'd: 'w;
}

impl<W: Execution> Kind<W> for Coupled {
    type Ingress = W::Parked;

    type Tail<'w, 'd>
        = ()
    where
        W: 'w,
        'd: 'w;
}

impl<W: Execution> Kind<W> for Scoped {
    type Ingress = W::Parked;

    type Tail<'w, 'd>
        = tail::Queued<'w, 'd, W>
    where
        W: 'w,
        'd: 'w;
}

impl<W: Execution> Replay<W> for Scoped {
    fn queue<'a, 'w, 'd>(tail: &'a mut Self::Tail<'w, 'd>) -> &'a mut tail::Queued<'w, 'd, W>
    where
        W: 'w,
        'd: 'w,
    {
        tail
    }
}

impl<W: Execution> Kind<W> for Retained {
    type Ingress = W::Retained;

    type Tail<'w, 'd>
        = ()
    where
        W: 'w,
        'd: 'w;
}
