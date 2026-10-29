use std::{cell::Cell, convert::Infallible, pin::Pin, rc::Rc};

use dope_core::{
    driver::{retained, route, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::receive,
    network::listener::{
        conn::{
            self, write,
            write::{body::Admit, error},
        },
        handler::{self, failures, retire, send},
    },
};
use dope_net::{
    link::egress::{self, reserve::exact},
    wire::Identity,
};
use dope_test::{alloc::Tracker, harness::signals::Gate};
use o3::buffer::{bytes::TryRetain, resident};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Fresh,
    Queued,
    Done,
}

#[derive(Debug)]
pub enum Error {
    Capacity(write::Reject),
    Mismatch(error::Cause),
}

pub enum Admission {
    Committed,
    Deferred(write::Deferred),
    Closed,
    Unavailable(exact::Failure),
    Rejected(write::Reject),
}

impl<B> From<Admit<B>> for Admission {
    fn from(admitted: Admit<B>) -> Self {
        match admitted {
            Admit::Committed => Self::Committed,
            Admit::Deferred { cause, .. } => Self::Deferred(cause),
            Admit::Closed(_) => Self::Closed,
            Admit::Unavailable(cause) => Self::Unavailable(cause.cause()),
            Admit::Rejected { cause, .. } => Self::Rejected(cause),
        }
    }
}

impl<B, R> From<error::Error<B, R, Infallible>> for Error {
    fn from(error: error::Error<B, R, Infallible>) -> Self {
        match error {
            error::Error::Capacity(cause) => Self::Capacity(cause),
            error::Error::Mismatch(cause) => Self::Mismatch(cause.into_parts().0),
            error::Error::Build(cause) => match cause {},
        }
    }
}

#[derive(Default)]
pub struct Seen {
    pub chunks: Vec<usize>,
    pub admissions: Vec<(Admission, egress::Pending)>,
    pub attempts: Vec<Attempt>,
    pub closed: Option<State>,
}

pub struct Attempt {
    pub callback: Option<State>,
    pub state: State,
    pub allocations: (usize, usize),
    pub result: Result<write::Outcome<()>, Error>,
}

pub struct Prefix {
    pub(crate) gate: Gate,
    pub(crate) seen: Rc<Cell<Seen>>,
}

impl Prefix {
    fn drive<'d, ID: route::Identity>(
        &self,
        conn: &mut conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, State>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        if *conn.state() != State::Queued {
            return;
        }
        let mut callback = None;
        let measure = Tracker::begin();
        let result = conn.write().try_prefix(driver, |state, prefix| {
            callback = Some(*state);
            *state = State::Done;
            prefix[0] = b'B';
            write::Draft::new(1, None, ()).close_after()
        });
        let activity = measure.finish();
        if !matches!(
            &result,
            Ok(write::Outcome::Committed(()) | write::Outcome::Deferred(write::Deferred::Capacity))
        ) {
            conn.close_after();
        }
        let attempt = Attempt {
            callback,
            state: *conn.state(),
            allocations: (activity.allocations, activity.deallocations),
            result: result.map_err(Error::from),
        };
        let mut seen = self.seen.take();
        seen.attempts.push(attempt);
        self.seen.set(seen);
    }
}

impl<'d, ID: route::Identity> handler::Application<'d, ID> for Prefix {
    type Conn = State;
    type Build = Infallible;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = receive::Borrowed;
    type Send = send::Silent;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(State::Fresh)
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn activate<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, State>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, State>
    where
        'd: 'w,
    {
        self.drive(&mut conn, driver);
        conn
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, State>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, State>
    where
        'd: 'w,
    {
        let state = *conn.state();
        let mut seen = self.seen.take();
        seen.closed = Some(state);
        self.seen.set(seen);
        self.gate.hit();
        conn
    }
}

impl<'d, ID: route::Identity> handler::Borrowed<'d, ID> for Prefix {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, State>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> receive::Outcome {
        let len = chunk.as_ref().len();
        let mut seen = self.seen.take();
        seen.chunks.push(len);
        self.seen.set(seen);
        if *conn.state() == State::Fresh {
            let body = egress::NonEmpty::from_array(b"A");
            let admitted = conn.write().admit(body, driver).into();
            *conn.state_mut() = State::Queued;
            let event = (admitted, conn.write().pending());
            let mut seen = self.seen.take();
            seen.admissions.push(event);
            self.seen.set(seen);
        }
        self.drive(&mut conn, driver);
        receive::Outcome::Continue
    }
}
