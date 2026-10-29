use std::{convert, ops, pin};

use dope_core::{
    driver::{route, settings},
    io,
};
use dope_runtime::executor;

use crate::app::dispatch::{self, event};

/// Statically selected completion capability for one exact route.
pub trait Set<'d, M, R: route::App> {
    type Plan;

    const ROUTES: settings::Routes;

    fn select() -> Option<Self::Plan>;

    fn call<'app, 'driver, 'turn, 'root>(
        owner: pin::Pin<&'app mut M>,
        plan: Self::Plan,
        call: event::Call<'driver, 'turn, 'root, 'd>,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'driver + 'turn + 'root,
        M: 'root;
}

impl<'d, M, R: route::App> Set<'d, M, R> for dispatch::raw::Omit {
    type Plan = convert::Infallible;

    const ROUTES: settings::Routes = settings::Routes::EMPTY;

    fn select() -> Option<Self::Plan> {
        None
    }

    fn call<'app, 'driver, 'turn, 'root>(
        _: pin::Pin<&'app mut M>,
        plan: Self::Plan,
        _: event::Call<'driver, 'turn, 'root, 'd>,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'driver + 'turn + 'root,
        M: 'root,
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

    fn call<'app, 'driver, 'turn, 'root>(
        owner: pin::Pin<&'app mut M>,
        plan: Self::Plan,
        call: event::Call<'driver, 'turn, 'root, 'd>,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'driver + 'turn + 'root,
        M: 'root,
    {
        <<M::Inner as dispatch::raw::Manifold<'d>>::Dispatch as Set<'d, M::Inner, R>>::call(
            dispatch::raw::Projection::inner_mut(owner),
            plan,
            call,
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

    fn call<'app, 'driver, 'turn, 'root>(
        owner: pin::Pin<&'app mut M>,
        plan: Self::Plan,
        call: event::Call<'driver, 'turn, 'root, 'd>,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'driver + 'turn + 'root,
        M: 'root,
    {
        event::Complete::dispatch(executor::raw::Call::new(owner, plan, call.into_event()))
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

    fn call<'app, 'driver, 'turn, 'root>(
        owner: pin::Pin<&'app mut M>,
        plan: Self::Plan,
        call: event::Call<'driver, 'turn, 'root, 'd>,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'driver + 'turn + 'root,
        M: 'root,
    {
        let (event, driver) = call.into_drive();
        let mut driver = dispatch::raw::Context::<dispatch::raw::Retained>::new(driver);
        event::Drive::dispatch(executor::raw::Call::new(
            owner,
            plan,
            (event, driver.narrow::<A>()),
        ))
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

    fn call<'app, 'driver, 'turn, 'root>(
        owner: pin::Pin<&'app mut M>,
        plan: Self::Plan,
        call: event::Call<'driver, 'turn, 'root, 'd>,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'driver + 'turn + 'root,
        M: 'root,
    {
        let (event, turn, driver) = call.into_turn();
        let mut driver = dispatch::raw::Context::<dispatch::raw::Retained>::new(driver);
        event::Turn::dispatch(executor::raw::Call::new(
            owner,
            plan,
            (event, turn, driver.narrow::<A>()),
        ))
    }
}
