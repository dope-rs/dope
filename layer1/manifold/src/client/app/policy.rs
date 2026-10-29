use std::convert;

use dope_core::driver::{retained, route::Identity, schedule};
use dope_net::{
    link::pool::{
        self,
        recv::{delivery, input},
    },
    wire::{self, receive},
};

use crate::{
    app::receive::{
        Borrowed, Mode, Owned,
        ingress::{self, Dispatch, Progress},
    },
    client::app,
};

pub(in crate::client) trait Policy<'w, 'd: 'w, ID: Identity, A>:
    Mode<A::Wire> + Dispatch<A::Wire>
where
    A: app::Application<'d, ID>,
{
    fn receive<'input, 'turn, 'driver, 'context, 'owner, R>(
        recv: R,
        key: pool::Key<'d, ID>,
        input: <Self as Mode<A::Wire>>::Value<'input, 'w, 'd>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) -> crate::Receiving<'w, 'd, A::Wire, Self>
    where
        'w: 'input,
        'd: 'owner,
        R: app::Receiver<'w, 'd, ID, A>;
}

impl<'w, 'd: 'w, ID: Identity, A> Policy<'w, 'd, ID, A> for Borrowed
where
    A: app::Borrowed<'d, ID>,
    Borrowed: Dispatch<A::Wire, Strategy = ingress::Batched>,
    <Borrowed as input::Mode<A::Wire>>::Kind: delivery::BorrowedData,
{
    fn receive<'input, 'turn, 'driver, 'context, 'owner, R>(
        recv: R,
        key: pool::Key<'d, ID>,
        input: <Self as Mode<A::Wire>>::Value<'input, 'w, 'd>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) -> Progress<convert::Infallible>
    where
        'w: 'input,
        'd: 'owner,
        R: app::Receiver<'w, 'd, ID, A>,
    {
        let input = Self::into_value(input);
        recv.borrowed(key, input, turn, driver)
    }
}

impl<'w, 'd: 'w, ID: Identity, A> Policy<'w, 'd, ID, A> for Owned
where
    A: app::Owned<'w, 'd, ID>,
    <<A::Wire as wire::Spec>::Receive as receive::Policy<A::Wire>>::Credit:
        delivery::Ownership<A::Wire>,
{
    fn receive<'input, 'turn, 'driver, 'context, 'owner, R>(
        recv: R,
        key: pool::Key<'d, ID>,
        input: <Self as Mode<A::Wire>>::Value<'input, 'w, 'd>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) -> Progress<convert::Infallible>
    where
        'w: 'input,
        'd: 'owner,
        R: app::Receiver<'w, 'd, ID, A>,
    {
        let input = Self::into_value(input);
        recv.retained(key, input, turn, driver)
    }
}
