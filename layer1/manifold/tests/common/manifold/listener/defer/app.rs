use o3::buffer::write::Sink as _;
use std::{convert, pin::Pin};

use dope_core::{
    driver::{self, route::Identity, schedule::clock},
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
use o3::buffer::bytes::TryRetain;

use crate::common::manifold::listener::defer::wire::Wire;

pub(crate) const BODY: &[u8; 4] = b"body";

type Output<'d> = dope_net::link::egress::data::Bound<
    o3::buffer::resident::Shared<'d>,
    o3::buffer::resident::bound::AtMost<4>,
>;

pub enum Event<'a> {
    Chunk(usize),
    Write(&'a Result<(), error::Admission>),
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

impl<F: for<'a> Fn(Event<'a>)> App<F> {
    pub(crate) fn fill<'d, ID: Identity>(
        &self,
        conn: &mut conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, State, Output<'d>>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        loop {
            let (next, byte, close) = match conn.state() {
                State::First => (State::Last, b'A', false),
                State::Last => (State::Done, b'B', true),
                State::Done => return,
            };
            let mut output_slot = std::mem::MaybeUninit::uninit();
            let result = match conn.write().try_open(driver, &mut output_slot) {
                Ok(mut output) => {
                    let (state, mut prefix) = output.get_mut().parts();
                    *state = next;
                    prefix.write_byte(byte).unwrap();
                    let load = egress::NonEmpty::from_array(BODY)
                        .try_bound::<o3::buffer::resident::bound::AtMost<4>>()
                        .map_err(|rejected| rejected.into_parts().1)
                        .expect("fixture body bound");
                    let end = if close {
                        write::End::Close
                    } else {
                        write::End::Open
                    };
                    write::Output::try_publish(output, Value::Checked(load), end)
                        .map_err(error::Mismatch::into_cause)
                        .expect("publication");
                    Ok(())
                }
                Err(cause) => Err(cause),
            };
            (self.observe)(Event::Write(&result));
            if matches!(result, Ok(())) {
                self.done.hit();
            } else {
                if matches!(
                    result,
                    Err(write::error::Admission::Deferred(
                        write::Deferred::OrderedDrain
                    ))
                ) {
                    self.ordered.hit();
                }
                return;
            }
        }
    }
}

impl<'d, ID: Identity, F: for<'a> Fn(Event<'a>)> Application<'d, ID> for App<F> {
    type Conn = State;
    type Build = convert::Infallible;
    type Wire = Wire;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = Output<'d>;
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
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, State, Output<'d>>,
    ) -> Outcome {
        conn.wake_target().wake();
        Outcome::Continue
    }

    fn activate<'e>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Wire, State, Output<'d>>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        self.as_ref().get_ref().fill(&mut conn, driver);
    }

    fn close<'e>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Wire, State, Output<'d>>,
    ) {
        self.as_ref().get_ref().closed.hit();
    }
}

impl<'d, ID: Identity, F: for<'a> Fn(Event<'a>)> send::Drive<'d, ID> for App<F> {
    fn send(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, State, Output<'d>>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        self.as_ref().get_ref().fill(&mut conn, driver);
    }
}

impl<'d, ID: Identity, F: for<'a> Fn(Event<'a>)> handler::Borrowed<'d, ID> for App<F> {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, State, Output<'d>>,
        chunk: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Outcome {
        (self.observe)(Event::Chunk(chunk.as_ref().len()));
        self.as_ref().get_ref().fill(&mut conn, driver);
        Outcome::Continue
    }
}
