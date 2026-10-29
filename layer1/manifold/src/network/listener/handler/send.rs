use std::{convert, marker, pin};

use dope_core::driver::{retained, route::Identity, schedule};
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

    fn release() -> Option<Self::Release>;

    /// Settles one affine post-send writer turn.
    fn settle<'listener, 'turn, 'driver, 'region, 'owner, 'w, E>(
        job: Pending<'listener, '_, '_, '_, 'turn, 'driver, 'region, 'owner, 'w, 'd, ID, A, E>,
    ) where
        'd: 'owner + 'w,
        E: env::Spec<Wire = A::Wire>,
    {
        job.finish();
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
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

pub enum Silent {}
pub enum Ready {}
pub enum Run {}

#[must_use]
pub(in crate::network::listener) enum Gate<P> {
    Open(P),
    Done,
}

pub(in crate::network::listener) enum Next {
    Commit,
    Done,
}

/// Affine post-send turn. Every policy must finish the exact writer transition.
#[must_use = "the post-send turn must be settled by its static policy"]
pub struct Pending<
    'listener,
    'n,
    'p,
    's,
    'turn,
    'driver,
    'region,
    'owner,
    'w,
    'd: 'owner + 'w,
    ID: Identity,
    A,
    E,
> where
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    root: pin::Pin<&'listener mut listener::Listener<'n, 'p, 's, 'w, 'd, ID, A, E>>,
    key: pool::Key<'d, ID>,
    next: Next,
    turn: schedule::Turn<'turn, 'd>,
    driver: &'driver mut retained::Context<'region, 'owner, 'd>,
}

impl<
    'listener,
    'n,
    'p,
    's,
    'turn,
    'driver,
    'region,
    'owner,
    'w,
    'd: 'owner + 'w,
    ID: Identity,
    A,
    E,
> Pending<'listener, 'n, 'p, 's, 'turn, 'driver, 'region, 'owner, 'w, 'd, ID, A, E>
where
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    pub(in crate::network::listener) fn new(
        root: pin::Pin<&'listener mut listener::Listener<'n, 'p, 's, 'w, 'd, ID, A, E>>,
        key: pool::Key<'d, ID>,
        next: Next,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'region, 'owner, 'd>,
    ) -> Self {
        Self {
            root,
            key,
            next,
            turn,
            driver,
        }
    }

    pub(in crate::network::listener) fn gate(mut self) -> Gate<Self>
    where
        A: handler::Gated<'d, ID>,
    {
        enum Drain {
            Open,
            Done,
        }

        let drain = {
            let mut this = self.root.as_mut().project();
            let Some(mut egress) = this.owner.egress_mut(self.key) else {
                return Gate::Done;
            };
            let scope = egress.context(self.turn.reborrow().app());
            let wake = scope.wake_target();
            match scope.drain() {
                conn::Drain::Ready(done) => {
                    if matches!(
                        handler::Gated::send_drained(this.app.as_mut(), done),
                        handler::PostDrain::Activate,
                    ) {
                        wake.wake();
                    }
                    Drain::Done
                }
                conn::Drain::Pending => Drain::Open,
            }
        };
        match drain {
            Drain::Open => Gate::Open(self),
            Drain::Done => {
                self.finish();
                Gate::Done
            }
        }
    }

    fn finish(mut self) {
        match self.next {
            Next::Commit => phase::commit_chunk(
                self.root.as_mut(),
                self.key,
                phase::Wake::Send,
                self.turn,
                self.driver,
            ),
            Next::Done => phase::close_ready(self.root, self.key, self.turn, self.driver),
        }
    }
}

impl<'d, ID: Identity, A> Policy<'d, ID, A> for Silent
where
    A: handler::Application<'d, ID>,
{
    type Release = convert::Infallible;

    fn release() -> Option<Self::Release> {
        None
    }
}

impl<'d, ID: Identity, A> Policy<'d, ID, A> for Ready
where
    A: Notify<'d, ID>,
{
    type Release = convert::Infallible;

    fn release() -> Option<Self::Release> {
        None
    }

    fn settle<'listener, 'turn, 'driver, 'region, 'owner, 'w, E>(
        mut job: Pending<'listener, '_, '_, '_, 'turn, 'driver, 'region, 'owner, 'w, 'd, ID, A, E>,
    ) where
        'd: 'owner + 'w,
        E: env::Spec<Wire = A::Wire>,
    {
        {
            let mut this = job.root.as_mut().project();
            let Some(mut egress) = this.owner.egress_mut(job.key) else {
                return;
            };
            A::send(this.app.as_mut(), egress.context(job.turn.reborrow().app()));
        }
        job.finish();
    }
}

impl<'d, ID: Identity, A> Policy<'d, ID, A> for Run
where
    A: Drive<'d, ID>,
{
    type Release = convert::Infallible;

    fn release() -> Option<Self::Release> {
        None
    }

    fn settle<'listener, 'turn, 'driver, 'region, 'owner, 'w, E>(
        mut job: Pending<'listener, '_, '_, '_, 'turn, 'driver, 'region, 'owner, 'w, 'd, ID, A, E>,
    ) where
        'd: 'owner + 'w,
        E: env::Spec<Wire = A::Wire>,
    {
        {
            let mut this = job.root.as_mut().project();
            let Some(mut egress) = this.owner.egress_mut(job.key) else {
                return;
            };
            A::send(
                this.app.as_mut(),
                egress.context(job.turn.reborrow().app()),
                job.driver,
            );
        }
        job.finish();
    }
}

impl<'d, ID: Identity, A> Policy<'d, ID, A> for Reported<Silent>
where
    A: Report<'d, ID>,
{
    type Release = Selected;

    fn release() -> Option<Self::Release> {
        if <A as Report<'d, ID>>::ENABLED {
            Some(Selected)
        } else {
            None
        }
    }
}

impl<'d, ID: Identity, A> Policy<'d, ID, A> for Reported<Run>
where
    A: Report<'d, ID> + Drive<'d, ID>,
{
    type Release = Selected;

    fn release() -> Option<Self::Release> {
        if <A as Report<'d, ID>>::ENABLED {
            Some(Selected)
        } else {
            None
        }
    }

    fn settle<'listener, 'turn, 'driver, 'region, 'owner, 'w, E>(
        job: Pending<'listener, '_, '_, '_, 'turn, 'driver, 'region, 'owner, 'w, 'd, ID, A, E>,
    ) where
        'd: 'owner + 'w,
        E: env::Spec<Wire = A::Wire>,
    {
        <Run as Policy<'d, ID, A>>::settle(job);
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
