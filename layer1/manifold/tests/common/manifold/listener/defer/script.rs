use std::{convert, pin::Pin, task};

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
use dope_runtime::executor::root;
use dope_test::{
    alloc::Tracker,
    harness::signals::{Gate, Progress as _},
};
use o3::buffer::{bytes::TryRetain, resident};

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

pub struct Until<'a> {
    pub(crate) gate: &'a Gate,
    pub(crate) hits: u32,
}

impl<'d> root::Root<'d> for Until<'_> {
    type Output = ();

    fn poll(context: root::Context<'_, 'd, Self>) -> task::Poll<()> {
        let (this, wake, _, _) = context.into_parts();
        if this.gate.hits() >= this.hits {
            task::Poll::Ready(())
        } else {
            wake.wake();
            task::Poll::Pending
        }
    }
}

pub enum Event<'a, 'd> {
    Write(
        &'a Result<write::Outcome<()>, error::Error<resident::Shared<'d>, (), convert::Infallible>>,
        (usize, usize),
    ),
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

impl<F: for<'a, 'd> Fn(Event<'a, 'd>)> App<F> {
    pub(crate) fn attempt<'e, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity>(
        &self,
        mut conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire, State>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire, State> {
        match conn.state() {
            State::Fresh => {}
            State::Done => return conn,
        }
        let body = match self.payload {
            Payload::Body => Some(Value::Checked(egress::NonEmpty::from_array(&BODY))),
            Payload::Prefix => None,
        };
        let measure = Tracker::begin();
        let result = conn.write().try_bounded(driver, move |state, prefix| {
            *state = State::Done;
            prefix[0] = b'R';
            write::Draft::new(1, body, ()).close_after()
        });
        let activity = measure.finish();
        (self.observe)(Event::Write(
            &result,
            (activity.allocations, activity.allocated_bytes),
        ));
        if matches!(result, Ok(write::Outcome::Committed(()))) {
            self.done.hit();
        } else {
            self.deferred.hit();
        }
        conn
    }
}

impl<'d, ID: Identity, F: for<'a, 'b> Fn(Event<'a, 'b>)> Application<'d, ID> for App<F> {
    type Conn = State;
    type Build = convert::Infallible;
    type Wire = Wire;
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

    fn accept(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, State>,
    ) -> Outcome {
        conn.wake_target().wake();
        Outcome::Continue
    }

    fn activate<'e>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Wire, State>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        self.as_ref().get_ref().attempt(conn, driver);
    }

    fn close_gate(
        self: Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, ID, Wire, State>,
    ) -> CloseGate {
        match conn.state() {
            State::Fresh => CloseGate::Deferred,
            State::Done => CloseGate::Immediate,
        }
    }

    fn close<'e>(self: Pin<&mut Self>, conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Wire, State>) {
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

impl<'d, ID: Identity, F: for<'a, 'b> Fn(Event<'a, 'b>)> handler::Borrowed<'d, ID> for App<F> {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, State>,
        chunk: R,
        _: &mut driver::Context<'_, 'd>,
    ) -> Outcome {
        (self.observe)(Event::Chunk(chunk.as_ref().len()));
        Outcome::Continue
    }
}
