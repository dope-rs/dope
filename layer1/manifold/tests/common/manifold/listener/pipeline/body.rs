use std::{cell::Cell, convert, pin::Pin, rc::Rc};

use dope_core::{
    driver::{retained, route, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::{receive, receive::Outcome},
    network::listener::{
        conn::{self, write},
        handler::{self, Application, failures, retire, send},
    },
};
use dope_net::{
    link::egress::{self, load::Value},
    wire::Identity,
};
use dope_test::harness::observe::VecCellExt as _;
use dope_test::harness::signals::Gate;
use o3::buffer::{bytes::TryRetain, resident};

use crate::common::manifold::listener::pipeline::prefix::Error;

pub const FIRST: &[u8; 13] = b"first-direct;";
pub const SECOND: &[u8; 14] = b"second-direct;";
pub const THIRD: &[u8; 13] = b"third-direct;";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum State {
    #[default]
    First,
    Second,
    Third,
    Done,
}

pub enum Event {
    Chunk(usize),
    Submit(State),
    Write(Option<usize>, Result<write::Outcome<()>, Error>),
    Close(State),
}

pub struct Body {
    pub(crate) gate: Gate,
    pub(crate) events: Rc<Cell<Vec<Event>>>,
}

impl Body {
    fn submit<'d, ID: route::Identity>(
        &self,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, State>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let event = Event::Submit(*conn.state());
        self.events.push(event);
        let (body, next) = match *conn.state() {
            State::First => (egress::NonEmpty::from_array(FIRST), State::Second),
            State::Second => (egress::NonEmpty::from_array(SECOND), State::Third),
            State::Third => (egress::NonEmpty::from_array(THIRD), State::Done),
            State::Done => {
                conn.close_after();
                return;
            }
        };
        let body = Value::Checked(body);
        let mut capacity = None;
        let result = conn.write().try_bounded(driver, |state, prefix| {
            capacity = Some(prefix.len());
            *state = next;
            write::Draft::new(0, Some(body), ())
        });
        let event = Event::Write(capacity, result.map_err(Error::from));
        self.events.push(event);
    }
}

impl<'d, ID: route::Identity> Application<'d, ID> for Body {
    type Conn = State;
    type Build = convert::Infallible;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = receive::Borrowed;
    type Send = send::Run;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(State::default())
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, State>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, State>
    where
        'd: 'w,
    {
        let event = Event::Close(*conn.state());
        self.events.push(event);
        self.get_mut().gate.hit();
        conn
    }
}

impl<'d, ID: route::Identity> send::Drive<'d, ID> for Body {
    fn send(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, State>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        match *conn.state() {
            State::Done => conn.close_after(),
            State::First | State::Second | State::Third => {
                self.submit(conn, driver);
            }
        }
    }
}

impl<'d, ID: route::Identity> handler::Borrowed<'d, ID> for Body {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, State>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let event = Event::Chunk(chunk.as_ref().len());
        self.events.push(event);
        self.submit(conn, driver);
        Outcome::Continue
    }
}
