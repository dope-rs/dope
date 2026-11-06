use std::{convert, marker, pin};

use dope_core::driver::{self, route::Identity, schedule};
use dope_net::link::pool;

use crate::{
    app::env,
    network::listener::{self, conn, handler, writer::phase},
};

/// Static send callback selection for one application type.
pub trait Policy<'d, ID: Identity, A>
where
    A: handler::Application<'d, ID>,
{
    type Release: Dispatch<'d, ID, A>;

    fn release() -> Option<Self::Release> {
        None
    }

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

pub trait Report<'d, ID: Identity>: handler::Application<'d, ID> {
    const ENABLED: bool;

    fn released(self: pin::Pin<&mut Self>, release: handler::Release<'d, ID, Self::Egress>);
}

pub trait Dispatch<'d, ID: Identity, A>
where
    A: handler::Application<'d, ID>,
{
    fn dispatch(
        self,
        app: pin::Pin<&mut A>,
        conn: conn::Id<'d, ID>,
        body: A::Egress,
        cause: handler::Disposition,
    );
}

pub struct Reported<P>(marker::PhantomData<fn() -> P>);

pub struct Selected;

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

pub(in crate::network::listener) enum Next {
    Commit,
    Done,
}

pub(in crate::network::listener) fn settle<'w, 'd: 'w, ID: Identity, A, E>(
    mut root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    key: pool::Key<'d, ID>,
    next: Next,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) where
    A: handler::Runtime<'w, 'd, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    if A::SEND {
        let mut this = root.as_mut().project();
        let Some(mut egress) = this.owner.egress_mut(key) else {
            return;
        };
        A::send(
            this.app.as_mut(),
            egress.context(turn.reborrow().app()),
            driver,
        );
    }
    match next {
        Next::Commit => phase::commit_chunk(root, key, phase::Wake::Send, turn, driver),
        Next::Done => phase::close_ready(root, key, turn, driver),
    }
}

pub(in crate::network::listener) fn gated<'w, 'd: 'w, ID: Identity, A>(
    mut app: pin::Pin<&mut A>,
    scope: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, A::Wire, A::Conn, A::Egress>,
    driver: &mut driver::Context<'_, 'd>,
) where
    A: handler::Gated<'d, ID>,
{
    let scope = if A::is_gated(scope.state()) {
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

impl<'d, ID: Identity, A> Policy<'d, ID, A> for Silent
where
    A: handler::Application<'d, ID>,
{
    type Release = convert::Infallible;
}

impl<'d, ID: Identity, A> Policy<'d, ID, A> for Ready
where
    A: Notify<'d, ID>,
{
    type Release = convert::Infallible;

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
    type Release = convert::Infallible;

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

impl<'d, ID: Identity, A: Report<'d, ID>, P: Policy<'d, ID, A>> Policy<'d, ID, A> for Reported<P> {
    type Release = Selected;

    const NOTIFY: bool = P::NOTIFY;

    fn release() -> Option<Self::Release> {
        if <A as Report<'d, ID>>::ENABLED {
            Some(Selected)
        } else {
            None
        }
    }

    fn send<'w>(
        app: pin::Pin<&mut A>,
        scope: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, A::Wire, A::Conn, A::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) where
        'd: 'w,
    {
        P::send(app, scope, driver);
    }
}

impl<'d, ID: Identity, A> Dispatch<'d, ID, A> for Selected
where
    A: Report<'d, ID>,
{
    fn dispatch(
        self,
        app: pin::Pin<&mut A>,
        conn: conn::Id<'d, ID>,
        body: A::Egress,
        cause: handler::Disposition,
    ) {
        let Self = self;
        A::released(app, handler::Release::new(conn, body, cause));
    }
}

impl<'d, ID: Identity, A> Dispatch<'d, ID, A> for convert::Infallible
where
    A: handler::Application<'d, ID>,
{
    fn dispatch(
        self,
        _: pin::Pin<&mut A>,
        _: conn::Id<'d, ID>,
        _: A::Egress,
        _: handler::Disposition,
    ) {
        match self {}
    }
}
