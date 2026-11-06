use std::{ops, pin};

use dope_core::{
    driver::{
        self,
        route::{self, Identity, table::conn as table},
        schedule,
    },
    io::event::net::recv,
};
use dope_net::{
    link::pool::recv::{input, reserve::Choice},
    wire::domain,
};

use crate::{
    app::{env, receive::ingress},
    network::listener::{
        self, conn,
        handler::{self, failures, policy},
    },
};

/// Receive and failure policies bound to one concrete wire owner.
pub trait Runtime<'w, 'd: 'w, ID: Identity>:
    handler::Application<'d, ID, WireFailure: failures::Policy<'w, 'd, Self, ID>>
{
    const SEND: bool;

    fn send(
        app: pin::Pin<&mut Self>,
        scope: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    );

    fn retained(&self, count: table::Capacity) -> Result<domain::Retained, crate::ConfigError>;
    fn resident(&self, count: table::Capacity) -> Result<domain::Resident, crate::ConfigError>;

    fn dispatch<E>(
        listener: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, Self, E>>,
        item: recv::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ops::ControlFlow<recv::Completion<'d>>
    where
        E: env::Spec<Wire = Self::Wire>;

    fn resume<E>(
        listener: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, Self, E>>,
        target: route::Target<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) where
        E: env::Spec<Wire = Self::Wire>;
}

impl<'w, 'd: 'w, ID: Identity, A> Runtime<'w, 'd, ID> for A
where
    A: handler::Application<'d, ID>,
    A::WireFailure: failures::Policy<'w, 'd, A, ID>,
    A::Input: policy::Policy<'w, 'd, ID, A>,
    <A::Input as input::Mode<A::Wire>>::Reserve:
        Choice<A::Wire, <A::Input as input::Mode<A::Wire>>::Ingress>,
{
    const SEND: bool = <A::Input as policy::Policy<'w, 'd, ID, A>>::SEND;

    fn send(
        app: pin::Pin<&mut Self>,
        scope: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        <A::Input as policy::Policy<'w, 'd, ID, A>>::send(app, scope, driver);
    }

    fn retained(&self, count: table::Capacity) -> Result<domain::Retained, crate::ConfigError> {
        <A::Input as policy::Policy<'w, 'd, ID, A>>::retained(self, count)
    }

    fn resident(&self, count: table::Capacity) -> Result<domain::Resident, crate::ConfigError> {
        <A::Input as policy::Policy<'w, 'd, ID, A>>::resident(self, count)
    }

    fn dispatch<E>(
        listener: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, Self, E>>,
        item: recv::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ops::ControlFlow<recv::Completion<'d>>
    where
        E: env::Spec<Wire = Self::Wire>,
    {
        <listener::Listener<'_, '_, '_, 'w, 'd, ID, Self, E> as ingress::Policy<'_, 'w, 'd, ID>>::dispatch(
            listener, item, turn, driver,
        )
    }

    fn resume<E>(
        listener: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, Self, E>>,
        target: route::Target<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) where
        E: env::Spec<Wire = Self::Wire>,
    {
        <listener::Listener<'_, '_, '_, 'w, 'd, ID, Self, E> as ingress::Policy<'_, 'w, 'd, ID>>::resume(
            listener, target, turn, driver,
        );
    }
}
