use std::{cell::Cell, convert, num, pin::Pin, rc::Rc, time::Duration};

use dope_core::{
    driver::{
        retained,
        route::Identity,
        schedule::{clock, ready::task::owner::Identity as _},
    },
    io::socket,
    random::Seed,
};
use dope_manifold::{
    app::receive::{self, Outcome, limits::Budget},
    network::listener::{
        conn::{self, write},
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
    wire::{EofOnData, Event, Wire},
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
    Consumed,
    Eof,
    Closed,
}

impl<'d, ID: Identity, F: for<'v> Fn(Observation<'v>) + Unpin, const CONTROL: bool>
    Application<'d, ID> for Probe<F, CONTROL>
{
    type Conn = State;
    type Build = convert::Infallible;
    type Wire = Wire<EofOnData<CONTROL>>;
    type WireFailure = Failure;
    type Retirement = Retirement;
    type Egress = Shared;
    type Input = receive::Scoped;
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
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire<EofOnData<CONTROL>>, State, Shared>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Timeout<'d> {
        if matches!(*conn.state(), State::Eof) && self.finish.hits() != 0 {
            *conn.state_mut() = State::Closed;
            return handler::Timeout::CloseAfter;
        }
        if self.release.hits() != 0 {
            conn.wake_target().wake();
        }
        handler::Timeout::Continue(handler::Deadline::Ensure(driver.driver_ref().clock().now()))
    }

    fn eof(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire<EofOnData<CONTROL>>, State, Shared>,
        _: receive::End,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Eof {
        let this = self.get_mut();
        let (lane, epoch) = conn.id().parts();
        this.events.push(Event::Generation(lane, epoch));
        this.events.push(Event::Eof);
        this.eof.hit();
        *conn.state_mut() = State::Eof;
        if !CONTROL {
            (this.observe)(Observation::Write(conn.write().try_bounded(
                driver,
                |_, prefix| {
                    prefix[..5].copy_from_slice(b"after");
                    write::Draft::new(5, None, ())
                },
            )));
        }
        handler::Eof::Hold
    }

    fn close_gate(
        self: Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, ID, Wire<EofOnData<CONTROL>>, State, Shared>,
    ) -> CloseGate {
        if matches!(*conn.state(), State::Closed) {
            CloseGate::Immediate
        } else {
            CloseGate::Deferred
        }
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire<EofOnData<CONTROL>>, State, Shared>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire<EofOnData<CONTROL>>, State, Shared>
    where
        'd: 'w,
    {
        let this = self.get_mut();
        let (lane, epoch) = conn.id().parts();
        this.events.push(Event::Generation(lane, epoch));
        this.events.push(Event::Close);
        this.close.hit();
        conn
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
        _: conn::Retired<'_, '_, 'd, ID, Wire<EofOnData<CONTROL>>, State, Shared>,
    ) {
        let this = app.get_mut();
        this.events.push(Event::Retired);
        this.retired.hit();
    }
}

impl<'w, 'd: 'w, ID: Identity, F: for<'v> Fn(Observation<'v>) + Unpin, const CONTROL: bool>
    failures::Policy<'w, 'd, Probe<F, CONTROL>, ID> for Failure
{
    fn open_failed(
        _: Pin<&mut Probe<F, CONTROL>>,
        cause: open::Error<'w, Wire<EofOnData<CONTROL>>>,
    ) {
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
        conn: conn::Ctx<'e, '_, '_, '_, 'w, 'd, ID, Wire<EofOnData<CONTROL>>, State, Shared>,
        cause: terminal::Error<'w, Wire<EofOnData<CONTROL>>>,
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
        mut conn: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, Wire<EofOnData<CONTROL>>, State, Shared>,
        chunk: scoped::Input<'_, 'w, 'd, Wire<EofOnData<CONTROL>>>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let this = self.get_mut();
        if matches!(*conn.state(), State::First) {
            *conn.state_mut() = State::Pending;
            (this.observe)(Observation::Deadline(
                conn.deadline()
                    .try_ensure(driver.driver_ref().clock().now()),
            ));
            let (lane, epoch) = conn.id().parts();
            this.events.push(Event::Generation(lane, epoch));
            this.ready.hit();
            if !CONTROL {
                this.events.push(Event::Deferred(chunk.as_ref().to_vec()));
                (this.observe)(Observation::Write(conn.write().try_bounded(
                    driver,
                    |_, prefix| {
                        prefix[..6].copy_from_slice(b"before");
                        write::Draft::new(6, None, ())
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
            match input.try_prefix::<{ u32::MAX }>(len) {
                Ok((_, end)) => (Some(end), Some(Ok(()))),
                Err(error) => (None, Some(Err(error))),
            }
        });
        (this.observe)(Observation::Replay(replay));
        *conn.state_mut() = State::Consumed;
        Outcome::Continue
    }
}
