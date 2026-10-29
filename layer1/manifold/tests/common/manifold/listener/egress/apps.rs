use std::{cell::Cell, convert, pin::Pin, rc::Rc};

use conn::write;
use dope_core::{
    BranchError,
    driver::{retained, route, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::{receive, receive::Outcome},
    network::listener::{conn, handler, handler::send},
};
use dope_net::{
    link::egress::{self, data},
    wire::Identity,
};
use dope_test::{alloc::Tracker, harness::signals::Gate};
use handler::{failures, retire};
use o3::buffer::{
    bytes::TryRetain,
    resident::{Shared, bound},
    storage,
};
use write::error;

use crate::common::manifold::listener::egress::{RESPONSE, Retirement};

type Build<'d, L> = error::Build<data::Bound<Shared<'d>, L>, ()>;
type WriteResult<'d, L, E = Build<'d, L>> =
    Result<write::Outcome<()>, error::Error<data::Bound<Shared<'d>, L>, (), E>>;

pub struct State<'d> {
    pub(crate) body: Option<Shared<'d>>,
}

pub struct Direct<F> {
    pub(crate) closed: Gate,
    pub(crate) retired: Gate,
    pub(crate) body: Cell<Option<storage::Shared>>,
    pub(crate) observe: F,
}

pub struct DirectWrite<'d> {
    pub body: Option<usize>,
    pub allocations: (usize, usize),
    pub result: WriteResult<'d, bound::Unbounded>,
}

impl<'d, ID: route::Identity, F: for<'a> Fn(DirectWrite<'a>)> handler::Application<'d, ID>
    for Direct<F>
{
    type Conn = State<'d>;
    type Build = convert::Infallible;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = Retirement;
    type Egress = data::Bound<Shared<'d>, bound::Unbounded>;
    type Input = receive::Borrowed;
    type Send = send::Silent;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(State {
            body: self.body.take().map(Shared::from),
        })
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, State<'d>, Self::Egress>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, State<'d>, Self::Egress>
    where
        'd: 'w,
    {
        self.as_ref().get_ref().closed.hit();
        conn
    }
}

impl<'d, ID: route::Identity, F: for<'a> Fn(DirectWrite<'a>)> handler::Borrowed<'d, ID>
    for Direct<F>
{
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, State<'d>, Self::Egress>,
        _: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let mut consumed = None;
        let measure = Tracker::begin();
        let result = conn.write().try_commit(driver, |state, mut prefix| {
            if let Err(cause) = prefix.write_byte(0xa5) {
                return Err(error::Build::prefix(cause, None, ()).close_after());
            }
            let body = state.body.take();
            consumed = body.as_ref().map(|body| body.as_ref().len());
            let body = body.map(|body| egress::Load::new(body).unbounded());
            match prefix.commit(body, ()) {
                Ok(commit) => Ok(commit.close_after()),
                Err(cause) => Err(cause.close_after()),
            }
        });
        let activity = measure.finish();
        let allocs = (activity.allocations, activity.allocated_bytes);
        (self.observe)(DirectWrite {
            body: consumed,
            allocations: allocs,
            result,
        });
        Outcome::Continue
    }
}

pub struct Bounded<F> {
    pub(crate) closed: Gate,
    pub(crate) retired: Gate,
    pub(crate) called: Rc<Cell<bool>>,
    pub(crate) observe: F,
}

pub enum BoundedEvent<'d> {
    Resident(Option<egress::ResidentSize>),
    Write {
        body: Option<usize>,
        allocations: (usize, usize),
        result: WriteResult<
            'd,
            bound::AtMost<0>,
            BranchError<egress::Excess<Shared<'d>>, Build<'d, bound::AtMost<0>>>,
        >,
    },
    Deferred {
        body: Option<Option<usize>>,
        called: bool,
        result: WriteResult<'d, bound::AtMost<0>>,
    },
    Activated {
        body: Option<Option<usize>>,
        result: WriteResult<'d, bound::AtMost<0>>,
    },
}

impl<'d, ID: route::Identity, F: for<'a> Fn(BoundedEvent<'a>)> handler::Application<'d, ID>
    for Bounded<F>
{
    type Conn = State<'d>;
    type Build = convert::Infallible;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = Retirement;
    type Egress = data::Bound<Shared<'d>, bound::AtMost<0>>;
    type Input = receive::Borrowed;
    type Send = send::Silent;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(State {
            body: Some(Shared::from(RESPONSE)),
        })
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn activate<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, State<'d>, Self::Egress>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, State<'d>, Self::Egress>
    where
        'd: 'w,
    {
        let this = self.as_ref().get_ref();
        let called = &this.called;
        if called.get() {
            return conn;
        }
        let mut body = None;
        let result = conn.write().try_commit(driver, |state, mut prefix| {
            called.set(true);
            body = Some(state.body.as_ref().map(|body| body.as_ref().len()));
            prefix
                .write_byte(0xc5)
                .map_err(|cause| error::Build::prefix(cause, None, ()).close_after())?;
            prefix
                .commit(None, ())
                .map(|commit| commit.close_after())
                .map_err(|cause| cause.close_after())
        });
        (this.observe)(BoundedEvent::Activated { body, result });
        conn
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, State<'d>, Self::Egress>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, State<'d>, Self::Egress>
    where
        'd: 'w,
    {
        self.as_ref().get_ref().closed.hit();
        conn
    }
}

impl<'d, ID: route::Identity, F: for<'a> Fn(BoundedEvent<'a>)> handler::Borrowed<'d, ID>
    for Bounded<F>
{
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, State<'d>, Self::Egress>,
        _: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        (self.observe)(BoundedEvent::Resident(
            conn.state()
                .body
                .as_ref()
                .map(|body| egress::Charge::of(body).resident()),
        ));
        let mut consumed = None;
        let measure = Tracker::begin();
        let result = conn.write().try_commit(driver, |state, mut prefix| {
            if let Err(cause) = prefix.write_byte(0xb5) {
                return Err(BranchError::Second(
                    error::Build::prefix(cause, None, ()).close_after(),
                ));
            }
            let body = state.body.take();
            consumed = body.as_ref().map(|body| body.as_ref().len());
            let body = body
                .map(|body| egress::Load::new(body).try_bound::<bound::AtMost<0>>())
                .transpose()
                .map_err(BranchError::First)?;
            prefix
                .commit(body, ())
                .map_err(|cause| BranchError::Second(cause.close_after()))
        });
        let activity = measure.finish();
        let allocs = (activity.allocations, activity.allocated_bytes);
        (self.observe)(BoundedEvent::Write {
            body: consumed,
            allocations: allocs,
            result,
        });
        let called = self.as_ref().get_ref().called.clone();
        let mut body = None;
        let result = conn.write().try_commit(driver, |state, prefix| {
            called.set(true);
            body = Some(state.body.take().map(|body| body.as_ref().len()));
            prefix.commit(None, ())
        });
        (self.observe)(BoundedEvent::Deferred {
            body,
            called: self.as_ref().get_ref().called.get(),
            result,
        });
        Outcome::Continue
    }
}

impl<'d, ID: route::Identity, F: for<'a> Fn(DirectWrite<'a>)> retire::Policy<'d, Direct<F>, ID>
    for Retirement
{
    fn select() -> Option<Self> {
        Some(Self)
    }

    fn retire(
        self,
        app: Pin<&mut Direct<F>>,
        _: conn::Retired<
            '_,
            '_,
            'd,
            ID,
            Identity,
            State<'d>,
            data::Bound<Shared<'d>, bound::Unbounded>,
        >,
    ) {
        app.as_ref().get_ref().retired.hit();
    }
}

impl<'d, ID: route::Identity, F: for<'a> Fn(BoundedEvent<'a>)> retire::Policy<'d, Bounded<F>, ID>
    for Retirement
{
    fn select() -> Option<Self> {
        Some(Self)
    }

    fn retire(
        self,
        app: Pin<&mut Bounded<F>>,
        _: conn::Retired<
            '_,
            '_,
            'd,
            ID,
            Identity,
            State<'d>,
            data::Bound<Shared<'d>, bound::AtMost<0>>,
        >,
    ) {
        app.as_ref().get_ref().retired.hit();
    }
}

use o3::buffer::write::Sink as _;
