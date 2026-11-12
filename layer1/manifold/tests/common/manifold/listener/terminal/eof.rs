use o3::buffer::write::Sink as _;
use std::{cell::Cell, convert, num, pin::Pin, rc::Rc, time::Duration};

use dope_core::{
    driver::{
        self,
        route::Identity,
        schedule::{clock, ready::task::owner::Identity as _},
    },
    io::socket,
    random::Seed,
};
use dope_manifold::{
    app::receive::{self, Outcome, limits::Budget},
    network::listener::{
        conn,
        handler::{self, Application, failures, retire, send},
    },
};
use dope_net::{
    link::slot::types::CloseGate,
    wire::{open, receive::scoped, terminal},
};
use dope_test::harness::{
    observe::VecCellExt as _,
    signals::{Gate, Progress as _},
};
use o3::{
    base::Call,
    buffer::{
        hold::{limits, scope::Input},
        storage::Shared,
    },
};

use crate::common::manifold::listener::terminal::{
    Failure, Observation, Retirement,
    wire::{Event, Wire},
};

pub struct Probe<F, const CONTROL: bool = false> {
    pub(crate) ready: Gate,
    pub(crate) release: Gate,
    pub(crate) eof: Gate,
    pub(crate) finish: Gate,
    pub(crate) close: Gate,
    pub(crate) retired: Gate,
    pub(crate) events: Rc<Cell<Vec<Event>>>,
    pub(crate) observe: F,
}

#[derive(Clone, Copy)]
pub enum State {
    First,
    Pending,
    Released,
    Consumed,
    Eof,
    Closed,
}

impl<'d, ID: Identity, F: for<'v> Fn(Observation<'v>) + Unpin, const CONTROL: bool>
    Application<'d, ID> for Probe<F, CONTROL>
{
    type Conn = State;
    type Build = convert::Infallible;
    type Wire = Wire;
    type WireFailure = Failure;
    type Retirement = Retirement;
    type Egress = Shared;
    type Input = receive::GatedScoped;
    type Send = send::Silent;

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(State::First)
    }

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn timing(&self) -> handler::Timing {
        handler::Timing::timeout(Duration::from_millis(10))
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn timeout(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, State, Shared>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> handler::Timeout<'d> {
        if matches!(*conn.state(), State::Eof) && self.finish.hits() != 0 {
            *conn.state_mut() = State::Closed;
            return handler::Timeout::CloseAfter;
        }
        if matches!(*conn.state(), State::Pending) && self.release.hits() != 0 {
            *conn.state_mut() = State::Released;
            conn.wake_target().wake();
        }
        handler::Timeout::Continue(handler::Deadline::Ensure(driver.driver_ref().clock().now()))
    }

    fn is_blocked(
        self: Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, ID, Wire, State, Shared>,
    ) -> bool {
        matches!(*conn.state(), State::Pending)
    }

    fn eof(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, State, Shared>,
        _: receive::End,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let this = self.get_mut();
        let (lane, epoch) = conn.id().parts();
        this.events.push(Event::Generation(lane, epoch));
        this.events.push(Event::Eof);
        this.eof.hit();
        *conn.state_mut() = State::Eof;
        if !CONTROL {
            (this.observe)(Observation::Write(conn.write().try_bounded(
                driver,
                |_, mut prefix| {
                    prefix.write_slice(b"after").unwrap();
                    prefix.commit(None, ())
                },
            )));
        }
    }

    fn close_gate(
        self: Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, ID, Wire, State, Shared>,
    ) -> CloseGate {
        if matches!(*conn.state(), State::Closed) {
            CloseGate::Immediate
        } else {
            CloseGate::Deferred
        }
    }

    fn close<'e>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Wire, State, Shared>,
    ) {
        let this = self.get_mut();
        let (lane, epoch) = conn.id().parts();
        this.events.push(Event::Generation(lane, epoch));
        this.events.push(Event::Close);
        this.close.hit();
    }
}

impl<'d, ID: Identity, F: for<'v> Fn(Observation<'v>) + Unpin, const CONTROL: bool>
    handler::Gated<'d, ID> for Probe<F, CONTROL>
{
    fn send_drained(
        self: Pin<&mut Self>,
        _: conn::Drained<'_, '_, '_, '_, '_, 'd, ID, Wire, State, Shared>,
    ) -> handler::PostDrain {
        handler::PostDrain::Idle
    }
}

impl<'d, ID: Identity, F: for<'v> Fn(Observation<'v>) + Unpin, const CONTROL: bool>
    retire::Policy<'d, Probe<F, CONTROL>, ID> for Retirement
{
    fn select() -> Option<Self> {
        Some(Self)
    }

    fn retire(
        self,
        app: Pin<&mut Probe<F, CONTROL>>,
        _: conn::Retired<'_, '_, 'd, ID, Wire, State, Shared>,
    ) {
        let this = app.get_mut();
        this.events.push(Event::Retired);
        this.retired.hit();
    }
}

impl<'w, 'd: 'w, ID: Identity, F: for<'v> Fn(Observation<'v>) + Unpin, const CONTROL: bool>
    failures::Policy<'w, 'd, Probe<F, CONTROL>, ID> for Failure
{
    fn open_failed(_: Pin<&mut Probe<F, CONTROL>>, cause: open::Error<'w, Wire>) {
        cause.eliminate()
    }

    fn peer() -> Option<impl for<'a> Call<(Pin<&'a mut Probe<F, CONTROL>>, socket::Error)>> {
        Some(
            |(app, cause): (Pin<&mut Probe<F, CONTROL>>, socket::Error)| {
                app.get_mut().events.push(Event::Peer(cause));
            },
        )
    }

    fn wire_failed<'e>(
        app: Pin<&mut Probe<F, CONTROL>>,
        conn: conn::Ctx<'e, '_, '_, '_, 'w, 'd, ID, Wire, State, Shared>,
        cause: terminal::Error<'w, Wire>,
    ) {
        let (cause, closing) = cause.into_parts();
        app.get_mut()
            .events
            .push(Event::Wire(cause, closing, conn.id().index().into_usize()));
    }
}

impl<'w, 'd: 'w, ID: Identity, F: for<'v> Fn(Observation<'v>) + Unpin, const CONTROL: bool>
    handler::Scoped<'w, 'd, ID> for Probe<F, CONTROL>
{
    fn retention(&self) -> Budget {
        Budget::new(limits::Bound::new(0), 4096)
    }

    fn scoped_chunk(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, Wire, State, Shared>,
        chunk: scoped::Input<'_, 'w, 'd, Wire>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Outcome {
        let this = self.get_mut();
        this.ready.hit();
        if matches!(*conn.state(), State::First) {
            *conn.state_mut() = State::Pending;
            (this.observe)(Observation::Deadline(
                conn.deadline()
                    .try_ensure(driver.driver_ref().clock().now()),
            ));
            let (lane, epoch) = conn.id().parts();
            this.events.push(Event::Generation(lane, epoch));
            if !CONTROL {
                this.events.push(Event::Deferred(chunk.as_ref().to_vec()));
                (this.observe)(Observation::Write(conn.write().try_bounded(
                    driver,
                    |_, mut prefix| {
                        prefix.write_slice(b"before").unwrap();
                        prefix.commit(None, ())
                    },
                )));
                return Outcome::Continue;
            }
        }
        if !CONTROL && this.release.hits() == 0 {
            return Outcome::Continue;
        }
        this.events.push(Event::Chunk(chunk.as_ref().to_vec()));
        let (chunk, budget) = chunk.into_parts();
        let replay = Input::consume(chunk, budget, |input| {
            let Some(len) = num::NonZero::new(input.as_slice().len()) else {
                return (None, None);
            };
            match input.try_prefix::<{ u32::MAX }>(0, len) {
                Ok((_, end)) => (Some(end), Some(Ok(()))),
                Err(error) => (None, Some(Err(error))),
            }
        });
        (this.observe)(Observation::Replay(replay));
        *conn.state_mut() = State::Consumed;
        Outcome::Continue
    }
}
