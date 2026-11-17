use std::{convert, ops, pin};

use dope_core::driver::{self, route, schedule, settings};
use dope_runtime::executor;

use crate::app::dispatch::{self, event};

/// Statically selected completion capability for one exact route.
pub trait Set<'d, M, R: route::App> {
    type Plan;

    const ROUTES: settings::Routes;

    fn select() -> Option<Self::Plan>;

    fn call<'event, 'driver, 'turn>(
        owner: pin::Pin<&mut M>,
        plan: Self::Plan,
        event: executor::raw::Dispatch<'event, 'd>,
        turn: schedule::Turn<'turn, 'd>,
        driver: driver::Context<'driver, 'd>,
    ) -> ops::ControlFlow<executor::raw::Dispatch<'event, 'd>>
    where
        'd: 'driver + 'turn;
}

impl<'d, M, R: route::App> Set<'d, M, R> for dispatch::raw::Omit {
    type Plan = convert::Infallible;

    const ROUTES: settings::Routes = settings::Routes::EMPTY;

    fn select() -> Option<Self::Plan> {
        None
    }

    fn call<'event, 'driver, 'turn>(
        _: pin::Pin<&mut M>,
        plan: Self::Plan,
        _: executor::raw::Dispatch<'event, 'd>,
        _: schedule::Turn<'turn, 'd>,
        _: driver::Context<'driver, 'd>,
    ) -> ops::ControlFlow<executor::raw::Dispatch<'event, 'd>>
    where
        'd: 'driver + 'turn,
    {
        match plan {}
    }
}

impl<'d, M, R> Set<'d, M, R> for dispatch::raw::Projected<M>
where
    R: route::App,
    M: dispatch::raw::Projection<'d>,
    M::Inner: dispatch::raw::Manifold<'d, Route = R>,
{
    type Plan = <<M::Inner as dispatch::raw::Manifold<'d>>::Dispatch as Set<'d, M::Inner, R>>::Plan;

    const ROUTES: settings::Routes =
        <<M::Inner as dispatch::raw::Manifold<'d>>::Dispatch as Set<'d, M::Inner, R>>::ROUTES;

    fn select() -> Option<Self::Plan> {
        <<M::Inner as dispatch::raw::Manifold<'d>>::Dispatch as Set<'d, M::Inner, R>>::select()
    }

    fn call<'event, 'driver, 'turn>(
        owner: pin::Pin<&mut M>,
        plan: Self::Plan,
        event: executor::raw::Dispatch<'event, 'd>,
        turn: schedule::Turn<'turn, 'd>,
        driver: driver::Context<'driver, 'd>,
    ) -> ops::ControlFlow<executor::raw::Dispatch<'event, 'd>>
    where
        'd: 'driver + 'turn,
    {
        <<M::Inner as dispatch::raw::Manifold<'d>>::Dispatch as Set<'d, M::Inner, R>>::call(
            dispatch::raw::Projection::inner_mut(owner),
            plan,
            event,
            turn,
            driver,
        )
    }
}

impl<'d, M, R> Set<'d, M, R> for event::Observe
where
    M: event::Complete<'d>,
    R: route::App,
{
    type Plan = ();

    const ROUTES: settings::Routes = settings::Routes::singleton_id(R::ID);

    fn select() -> Option<Self::Plan> {
        Some(())
    }

    fn call<'event, 'driver, 'turn>(
        owner: pin::Pin<&mut M>,
        _: Self::Plan,
        event: executor::raw::Dispatch<'event, 'd>,
        _: schedule::Turn<'turn, 'd>,
        _: driver::Context<'driver, 'd>,
    ) -> ops::ControlFlow<executor::raw::Dispatch<'event, 'd>>
    where
        'd: 'driver + 'turn,
    {
        event::Complete::dispatch(owner, event)
    }
}

impl<'d, M, R, A> Set<'d, M, R> for event::Driven<A>
where
    M: event::Drive<'d, Access = A>,
    R: route::App,
{
    type Plan = ();

    const ROUTES: settings::Routes = settings::Routes::singleton_id(R::ID);

    fn select() -> Option<Self::Plan> {
        Some(())
    }

    fn call<'event, 'driver, 'turn>(
        owner: pin::Pin<&mut M>,
        _: Self::Plan,
        event: executor::raw::Dispatch<'event, 'd>,
        _: schedule::Turn<'turn, 'd>,
        driver: driver::Context<'driver, 'd>,
    ) -> ops::ControlFlow<executor::raw::Dispatch<'event, 'd>>
    where
        'd: 'driver + 'turn,
    {
        let mut driver = dispatch::raw::Context::<dispatch::raw::Retained>::new(driver);
        event::Drive::dispatch(owner, event, driver.narrow::<A>())
    }
}

impl<'d, M, R, A> Set<'d, M, R> for event::Timed<A>
where
    M: event::Turn<'d, Access = A>,
    R: route::App,
{
    type Plan = ();

    const ROUTES: settings::Routes = settings::Routes::singleton_id(R::ID);

    fn select() -> Option<Self::Plan> {
        Some(())
    }

    fn call<'event, 'driver, 'turn>(
        owner: pin::Pin<&mut M>,
        _: Self::Plan,
        event: executor::raw::Dispatch<'event, 'd>,
        turn: schedule::Turn<'turn, 'd>,
        driver: driver::Context<'driver, 'd>,
    ) -> ops::ControlFlow<executor::raw::Dispatch<'event, 'd>>
    where
        'd: 'driver + 'turn,
    {
        let mut driver = dispatch::raw::Context::<dispatch::raw::Retained>::new(driver);
        event::Turn::dispatch(owner, event, turn, driver.narrow::<A>())
    }
}
