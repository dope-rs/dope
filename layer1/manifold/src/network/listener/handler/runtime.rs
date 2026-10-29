use std::{ops, pin};

use dope_core::{
    driver::{
        retained,
        route::{self, Identity, table::conn},
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
        self,
        handler::{self, failures, policy, send},
    },
};

/// Receive and failure policies bound to one concrete wire owner.
pub trait Runtime<'w, 'd: 'w, ID: Identity>:
    handler::Application<'d, ID, WireFailure: failures::Policy<'w, 'd, Self, ID>>
{
    fn send<'listener, 'turn, 'driver, 'region, 'owner, E>(
        work: send::Pending<
            'listener,
            '_,
            '_,
            '_,
            'turn,
            'driver,
            'region,
            'owner,
            'w,
            'd,
            ID,
            Self,
            E,
        >,
    ) where
        'd: 'owner,
        E: env::Spec<Wire = Self::Wire>;

    fn retained(&self, count: conn::Capacity) -> Result<domain::Retained, crate::ConfigError>;
    fn resident(&self, count: conn::Capacity) -> Result<domain::Resident, crate::ConfigError>;

    fn dispatch<E>(
        listener: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, Self, E>>,
        item: recv::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> ops::ControlFlow<recv::Completion<'d>>
    where
        E: env::Spec<Wire = Self::Wire>;

    fn resume<E>(
        listener: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, Self, E>>,
        target: route::Target<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
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
    fn send<'listener, 'turn, 'driver, 'region, 'owner, E>(
        work: send::Pending<
            'listener,
            '_,
            '_,
            '_,
            'turn,
            'driver,
            'region,
            'owner,
            'w,
            'd,
            ID,
            Self,
            E,
        >,
    ) where
        'd: 'owner,
        E: env::Spec<Wire = Self::Wire>,
    {
        <A::Input as policy::Policy<'w, 'd, ID, A>>::send(work);
    }

    fn retained(&self, count: conn::Capacity) -> Result<domain::Retained, crate::ConfigError> {
        <A::Input as policy::Policy<'w, 'd, ID, A>>::retained(self, count)
    }

    fn resident(&self, count: conn::Capacity) -> Result<domain::Resident, crate::ConfigError> {
        <A::Input as policy::Policy<'w, 'd, ID, A>>::resident(self, count)
    }

    fn dispatch<E>(
        listener: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, Self, E>>,
        item: recv::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
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
        driver: &mut retained::Context<'_, '_, 'd>,
    ) where
        E: env::Spec<Wire = Self::Wire>,
    {
        <listener::Listener<'_, '_, '_, 'w, 'd, ID, Self, E> as ingress::Policy<'_, 'w, 'd, ID>>::resume(
            listener, target, turn, driver,
        );
    }
}
