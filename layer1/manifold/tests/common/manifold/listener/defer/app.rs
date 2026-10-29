use std::{convert, pin::Pin};

use dope_core::{
    driver::{retained, route::Identity, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::{receive, receive::Outcome},
    network::listener::{
        conn::{self, write, write::error},
        handler::{self, Application, failures, retire, send},
    },
};
use dope_net::link::egress::{self, load::Value};
use dope_test::harness::signals::Gate;
use o3::buffer::{bytes::TryRetain, resident::Shared};

use crate::common::manifold::listener::defer::wire::Wire;

pub(crate) const BODY: &[u8; 4] = b"body";

pub enum Event<'a, 'd> {
    Chunk(usize),
    Write(&'a Result<write::Outcome<()>, error::Error<Shared<'d>, (), convert::Infallible>>),
}

pub struct App<F> {
    pub(crate) done: Gate,
    pub(crate) ordered: Gate,
    pub(crate) closed: Gate,
    pub(crate) observe: F,
}

pub enum State {
    First,
    Last,
    Done,
}

#[derive(Clone, Copy)]
pub(crate) enum Step {
    Open,
    Close,
}

impl<F: for<'a, 'd> Fn(Event<'a, 'd>)> App<F> {
    pub(crate) fn fill<'d, ID: Identity>(
        &self,
        conn: &mut conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, State>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        loop {
            let step = match conn.state() {
                State::First => Step::Open,
                State::Last => Step::Close,
                State::Done => return,
            };
            let load = egress::NonEmpty::from_array(BODY);
            let result = conn.write().try_bounded(driver, move |state, prefix| {
                let draft = write::Draft::new(1, Some(Value::Checked(load)), ());
                match step {
                    Step::Open => {
                        *state = State::Last;
                        prefix[0] = b'A';
                        draft
                    }
                    Step::Close => {
                        *state = State::Done;
                        prefix[0] = b'B';
                        draft.close_after()
                    }
                }
            });
            (self.observe)(Event::Write(&result));
            if matches!(result, Ok(write::Outcome::Committed(()))) {
                self.done.hit();
            } else {
                if matches!(
                    result,
                    Ok(write::Outcome::Deferred(write::Deferred::OrderedDrain))
                ) {
                    self.ordered.hit();
                }
                return;
            }
        }
    }
}

impl<'d, ID: Identity, F: for<'a, 'b> Fn(Event<'a, 'b>)> Application<'d, ID> for App<F> {
    type Conn = State;
    type Build = convert::Infallible;
    type Wire = Wire;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = Shared<'d>;
    type Input = receive::Borrowed;
    type Send = send::Run;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(State::First)
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn accept(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, State>,
    ) -> Outcome {
        conn.wake_target().wake();
        Outcome::Continue
    }

    fn activate<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire, State>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire, State>
    where
        'd: 'w,
    {
        self.as_ref().get_ref().fill(&mut conn, driver);
        conn
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire, State>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire, State>
    where
        'd: 'w,
    {
        self.as_ref().get_ref().closed.hit();
        conn
    }
}

impl<'d, ID: Identity, F: for<'a, 'b> Fn(Event<'a, 'b>)> send::Drive<'d, ID> for App<F> {
    fn send(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, State>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        self.as_ref().get_ref().fill(&mut conn, driver);
    }
}

impl<'d, ID: Identity, F: for<'a, 'b> Fn(Event<'a, 'b>)> handler::Borrowed<'d, ID> for App<F> {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, State>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        (self.observe)(Event::Chunk(chunk.as_ref().len()));
        self.as_ref().get_ref().fill(&mut conn, driver);
        Outcome::Continue
    }
}
