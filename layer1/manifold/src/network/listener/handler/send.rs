use std::pin;

use crate::network::listener::{conn, handler};
use dope_core::driver::{self, route::Identity};

/// Static send callback selection for one application type.
pub trait Policy<'d, ID: Identity, A>
where
    A: handler::Application<'d, ID>,
{
    const NOTIFY: bool = false;

    fn send<'w>(
        _app: pin::Pin<&mut A>,
        _scope: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, A::Wire, A::Conn, A::Egress>,
        _driver: &mut driver::Context<'_, 'd>,
    ) where
        'd: 'w,
    {
    }
}

/// Application capability that needs a turn but not retained-driver access.
pub trait Notify<'d, ID: Identity>: handler::Application<'d, ID> {
    fn send<'e>(
        self: pin::Pin<&mut Self>,
        scope: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
    );
}

/// Application capability whose send turn needs retained-owner driver access.
pub trait Drive<'d, ID: Identity>: handler::Application<'d, ID> {
    fn send<'e>(
        self: pin::Pin<&mut Self>,
        scope: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    );
}

pub enum Silent {}
pub enum Ready {}
pub enum Run {}

pub(in crate::network::listener) fn gated<'w, 'd: 'w, ID: Identity, A>(
    mut app: pin::Pin<&mut A>,
    scope: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, A::Wire, A::Conn, A::Egress>,
    driver: &mut driver::Context<'_, 'd>,
) where
    A: handler::Gated<'d, ID>,
{
    let scope = if A::is_gated(scope.state()) && A::drains(scope.state()) {
        let wake = scope.wake_target();
        match scope.try_drain() {
            Ok(done) => {
                if matches!(
                    A::send_drained(app.as_mut(), done),
                    handler::PostDrain::Activate
                ) {
                    wake.wake();
                }
                return;
            }
            Err(scope) => scope,
        }
    } else {
        scope
    };
    <A::Send as Policy<'d, ID, A>>::send(app, scope, driver);
}

impl<'d, ID: Identity, A: handler::Application<'d, ID>> Policy<'d, ID, A> for Silent {}

impl<'d, ID: Identity, A> Policy<'d, ID, A> for Ready
where
    A: Notify<'d, ID>,
{
    const NOTIFY: bool = true;

    fn send<'w>(
        app: pin::Pin<&mut A>,
        scope: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, A::Wire, A::Conn, A::Egress>,
        _driver: &mut driver::Context<'_, 'd>,
    ) where
        'd: 'w,
    {
        A::send(app, scope);
    }
}

impl<'d, ID: Identity, A> Policy<'d, ID, A> for Run
where
    A: Drive<'d, ID>,
{
    const NOTIFY: bool = true;

    fn send<'w>(
        app: pin::Pin<&mut A>,
        scope: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, A::Wire, A::Conn, A::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) where
        'd: 'w,
    {
        A::send(app, scope, driver);
    }
}
