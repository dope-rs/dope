use o3::buffer::write::Sink as _;
use std::{convert, pin::Pin};

use dope_core::{
    driver::{self, route::Identity, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::receive::{self, Outcome},
    network::listener::{
        conn::{self, write, write::error},
        handler::{self, Application, failures, retire, send},
    },
};
use dope_net::link::{
    egress::{self, load::Value},
    slot::types::CloseGate,
};
use dope_test::{
    alloc::Tracker,
    harness::signals::{Gate, Progress as _},
};
use o3::buffer::bytes::TryRetain;

use crate::common::manifold::listener::defer::wire::Wire;

pub const BODY_LEN: usize = 1024 * 1024;
pub(crate) static BODY: [u8; BODY_LEN] = [0x52; BODY_LEN];

#[derive(Clone, Copy, Debug)]
pub enum State {
    Fresh,
    Done,
}

#[derive(Clone, Copy)]
pub enum Payload {
    Body,
    Prefix,
}

type Output<'d> = dope_net::link::egress::data::Bound<
    o3::buffer::resident::Shared<'d>,
    o3::buffer::resident::bound::AtMost<BODY_LEN>,
>;

pub enum Event<'a> {
    Write(&'a Result<(), error::Admission>, (usize, usize)),
    Close {
        state: State,
        index: usize,
        done: u32,
        deferred: u32,
        closed: u32,
    },
    Chunk(usize),
}

pub struct App<F> {
    pub(crate) done: Gate,
    pub(crate) deferred: Gate,
    pub(crate) closed: Gate,
    pub(crate) payload: Payload,
    pub(crate) observe: F,
}

impl<F: for<'a> Fn(Event<'a>)> App<F> {
    pub(crate) fn attempt<'e, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity>(
        &self,
        mut conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire, State, Output<'d>>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire, State, Output<'d>> {
        match conn.state() {
            State::Fresh => {}
            State::Done => return conn,
        }
        let body = match self.payload {
            Payload::Body => Some(Value::Checked(
                egress::NonEmpty::from_array(&BODY)
                    .try_bound::<o3::buffer::resident::bound::AtMost<BODY_LEN>>()
                    .map_err(|rejected| rejected.into_parts().1)
                    .expect("fixture body bound"),
            )),
            Payload::Prefix => None,
        };
        let measure = Tracker::begin();
        let mut output_slot = std::mem::MaybeUninit::uninit();
        let result = match conn.try_output(driver, &mut output_slot) {
            Ok(mut output) => {
                let (state, mut prefix) = output.get_mut().parts();
                *state = State::Done;
                prefix.write_byte(b'R').unwrap();
                match body {
                    Some(body) => write::Output::try_publish(output, body, write::End::Close),
                    None => write::Output::finish(output, write::End::Close),
                }
                .map_err(error::Mismatch::into_cause)
                .expect("publication");
                Ok(())
            }
            Err(cause) => Err(cause),
        };
        let activity = measure.finish();
        (self.observe)(Event::Write(
            &result,
            (activity.allocations, activity.allocated_bytes),
        ));
        if matches!(result, Ok(())) {
            self.done.hit();
        } else {
            self.deferred.hit();
        }
        conn
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

    fn accept(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, State, Output<'d>>,
    ) -> Outcome {
        conn.wake_target().wake();
        Outcome::Continue
    }

    fn activate<'e>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Wire, State, Output<'d>>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        self.as_ref().get_ref().attempt(conn, driver);
    }

    fn close_gate(
        self: Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, ID, Wire, State, Output<'d>>,
    ) -> CloseGate {
        match conn.state() {
            State::Fresh => CloseGate::Deferred,
            State::Done => CloseGate::Immediate,
        }
    }

    fn close<'e>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Wire, State, Output<'d>>,
    ) {
        (self.observe)(Event::Close {
            state: *conn.state(),
            index: conn.id().index().into_usize(),
            done: self.done.hits(),
            deferred: self.deferred.hits(),
            closed: self.closed.hits(),
        });
        self.as_ref().get_ref().closed.hit();
    }
}

impl<'d, ID: Identity, F: for<'a> Fn(Event<'a>)> handler::Borrowed<'d, ID> for App<F> {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, State, Output<'d>>,
        chunk: R,
        _: &mut driver::Context<'_, 'd>,
    ) -> Outcome {
        (self.observe)(Event::Chunk(chunk.as_ref().len()));
        Outcome::Continue
    }
}
