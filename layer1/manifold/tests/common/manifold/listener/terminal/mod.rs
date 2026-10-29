use dope_core::random::Seed;
pub mod eof;
pub mod wire;

use std::{cell::Cell, convert, marker::PhantomData, num, pin::Pin, rc::Rc, time::Duration};

use dope_core::{
    driver::{retained, route::Identity, schedule::clock},
    io::socket,
};
use dope_manifold::{
    app::receive::{self, Outcome, limits::Budget},
    network::listener::{
        conn::{
            self,
            deadline::Arm,
            write,
            write::{
                body::{self, Admit},
                error,
            },
        },
        handler::{self, Application, failures, retire, send},
    },
};
use dope_net::{
    link::{egress, slot::types::CloseGate},
    wire::{open, receive::scoped, terminal},
};
use dope_test::harness::{
    observe::VecCellExt as _,
    signals::{Gate, Progress as _},
};
use o3::{
    base::Call,
    buffer::{
        bytes::TryRetain,
        hold::{limits, scope::Input},
        storage::Shared,
        write::consume::scope,
    },
};
use wire::{Event, FailureMode, Wire};

pub struct Probe<W, I, F> {
    pub(crate) close: Gate,
    pub(crate) retired: Gate,
    pub(crate) events: Rc<Cell<Vec<Event>>>,
    pub(crate) ingress: Ingress,
    pub(crate) marker: PhantomData<fn() -> (W, I)>,
    pub(crate) observe: F,
}

pub enum Observation<'d> {
    Deadline(Arm<'d>),
    Write(Result<write::Outcome<()>, error::Error<Shared, (), convert::Infallible>>),
    Owned(Admit<Shared>),
    Replay(Option<Result<(), scope::Error>>),
    Failure(Option<&'d conn::Failure>),
}

pub fn accepted(event: Observation<'_>) {
    assert!(match event {
        Observation::Deadline(armed) => matches!(armed, Arm::Armed),
        Observation::Write(result) => matches!(result, Ok(write::Outcome::Committed(()))),
        Observation::Owned(result) => matches!(result, Admit::Committed),
        Observation::Replay(result) => matches!(result, Some(Ok(()))),
        Observation::Failure(_) => false,
    });
}

pub enum Ingress {
    Fail,
    Rejected(Gate),
    Close,
    Reply,
    Owned(Option<egress::NonEmpty<Shared>>),
    Armed(Rc<wire::Probe>),
    Hold,
    Held,
    HalfClose(Gate),
    Shutting(Gate),
    Released,
}

pub const OWNED_REPLY: &[u8] = concat!(
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "c",
)
.as_bytes();

pub enum Scope {
    First,
    Repeat,
}

pub struct Failure;
pub struct Retirement;

pub trait Mode {
    type Conn;

    const CONNECTION: Self::Conn;
}

pub trait Borrow: Mode {}

impl Mode for receive::Coupled {
    type Conn = ();
    const CONNECTION: Self::Conn = ();
}

impl Borrow for receive::Coupled {}

impl Mode for receive::Gated {
    type Conn = ();
    const CONNECTION: Self::Conn = ();
}

impl Borrow for receive::Gated {}

impl Mode for receive::Scoped {
    type Conn = Scope;
    const CONNECTION: Self::Conn = Scope::First;
}

impl<'d, ID: Identity, M, I, F> Application<'d, ID> for Probe<Wire<M>, I, F>
where
    M: FailureMode + 'd,
    F: for<'v> Fn(Observation<'v>) + Unpin,
    I: Mode + receive::Mode<Wire<M>>,
{
    type Conn = I::Conn;
    type Build = convert::Infallible;
    type Wire = Wire<M>;
    type WireFailure = Failure;
    type Retirement = Retirement;
    type Egress = Shared;
    type Input = I;
    type Send = send::Silent;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(I::CONNECTION)
    }

    fn timing(&self) -> handler::Timing {
        match &self.ingress {
            Ingress::Armed(_) | Ingress::Hold | Ingress::HalfClose(_) => {
                handler::Timing::timeout(Duration::from_millis(100))
            }
            Ingress::Fail
            | Ingress::Rejected(_)
            | Ingress::Close
            | Ingress::Reply
            | Ingress::Owned(_)
            | Ingress::Held
            | Ingress::Shutting(_)
            | Ingress::Released => handler::Timing::default(),
        }
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire<M>, I::Conn, Shared>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire<M>, I::Conn, Shared>
    where
        'd: 'w,
    {
        let this = self.get_mut();
        this.events.push(Event::Close);
        this.close.hit();
        conn
    }

    fn is_blocked(
        self: Pin<&Self>,
        _: conn::Ref<'_, '_, '_, '_, 'd, ID, Wire<M>, I::Conn, Shared>,
    ) -> bool {
        matches!(&self.get_ref().ingress, Ingress::Armed(probe) if probe.chunk.hits() != 0)
    }

    fn timeout(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire<M>, I::Conn, Shared>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Timeout<'d> {
        let this = self.get_mut();
        match &this.ingress {
            Ingress::HalfClose(peer) => {
                let events = this.events.take();
                let sent = events.contains(&Event::ControlSent);
                this.events.set(events);
                if sent {
                    this.ingress = Ingress::Shutting(peer.clone());
                    this.events.push(Event::ShutdownRequested);
                    conn.shutdown();
                    conn.wake_target().wake();
                }
                return handler::Timeout::Continue(handler::Deadline::Ensure(
                    driver.driver_ref().clock().now(),
                ));
            }
            Ingress::Shutting(peer) => {
                if peer.hits() == 0 || matches!(conn.write().pending(), egress::Pending::Present) {
                    return handler::Timeout::Continue(handler::Deadline::Ensure(
                        driver.driver_ref().clock().now(),
                    ));
                }
                this.events.push(Event::PeerEof);
                this.ingress = Ingress::Released;
                conn.wake_target().wake();
                return handler::Timeout::Continue(handler::Deadline::Clear);
            }
            _ => {}
        }
        this.events.push(Event::Timeout);
        match &mut this.ingress {
            ingress @ (Ingress::Armed(_) | Ingress::Held) => {
                *ingress = Ingress::Released;
                conn.wake_target().wake();
                handler::Timeout::Continue(handler::Deadline::Clear)
            }
            Ingress::Fail
            | Ingress::Rejected(_)
            | Ingress::Close
            | Ingress::Reply
            | Ingress::Owned(_)
            | Ingress::Hold
            | Ingress::HalfClose(_)
            | Ingress::Shutting(_)
            | Ingress::Released => handler::Timeout::Discard,
        }
    }

    fn eof(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire<M>, I::Conn, Shared>,
        _: receive::End,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Eof {
        let this = self.get_mut();
        this.events.push(Event::Eof);
        if matches!(this.ingress, Ingress::Hold | Ingress::HalfClose(_)) {
            if matches!(this.ingress, Ingress::Hold) {
                this.ingress = Ingress::Held;
            }
            (this.observe)(Observation::Deadline(
                conn.deadline()
                    .try_ensure(driver.driver_ref().clock().now()),
            ));
            handler::Eof::Hold
        } else if matches!(this.ingress, Ingress::Held | Ingress::Shutting(_)) {
            handler::Eof::Hold
        } else {
            handler::Eof::Close
        }
    }

    fn close_gate(
        self: Pin<&Self>,
        _: conn::Ref<'_, '_, '_, '_, 'd, ID, Wire<M>, I::Conn, Shared>,
    ) -> CloseGate {
        if matches!(
            self.ingress,
            Ingress::Held | Ingress::HalfClose(_) | Ingress::Shutting(_) | Ingress::Rejected(_)
        ) {
            CloseGate::Deferred
        } else {
            CloseGate::Immediate
        }
    }
}

impl<'d, ID: Identity, M, I, F> retire::Policy<'d, Probe<Wire<M>, I, F>, ID> for Retirement
where
    M: FailureMode + 'd,
    F: for<'v> Fn(Observation<'v>) + Unpin,
    I: Mode + receive::Mode<Wire<M>>,
{
    fn select() -> Option<Self> {
        Some(Self)
    }

    fn retire(
        self,
        app: Pin<&mut Probe<Wire<M>, I, F>>,
        conn: conn::Retired<'_, '_, 'd, ID, Wire<M>, I::Conn, Shared>,
    ) {
        let this = app.get_mut();
        if matches!(this.ingress, Ingress::Rejected(_)) {
            (this.observe)(Observation::Failure(conn.failure()));
        }
        this.events.push(Event::Retired);
        this.retired.hit();
    }
}

impl<'w, 'd: 'w, ID: Identity, M, I, F> failures::Policy<'w, 'd, Probe<Wire<M>, I, F>, ID>
    for Failure
where
    M: FailureMode + 'd,
    F: for<'v> Fn(Observation<'v>) + Unpin,
    I: Mode + receive::Mode<Wire<M>>,
{
    fn open_failed(_: Pin<&mut Probe<Wire<M>, I, F>>, cause: open::Error<'w, Wire<M>>) {
        cause.eliminate()
    }

    fn peer() -> Option<impl for<'a> Call<(Pin<&'a mut Probe<Wire<M>, I, F>>, socket::Error)>> {
        Some(|(app, cause): (Pin<&mut Probe<_, _, _>>, socket::Error)| {
            app.get_mut().events.push(Event::Peer(cause));
        })
    }

    fn wire_failed<'e>(
        app: Pin<&mut Probe<Wire<M>, I, F>>,
        conn: conn::Ctx<'e, '_, '_, '_, 'w, 'd, ID, Wire<M>, I::Conn, Shared>,
        cause: terminal::Error<'w, Wire<M>>,
    ) {
        let this = app.get_mut();
        let (cause, closing) = cause.into_parts();
        this.events
            .push(Event::Wire(cause, closing, conn.id().index().into_usize()));
    }
}

impl<'d, ID: Identity, M, I, F> handler::Borrowed<'d, ID> for Probe<Wire<M>, I, F>
where
    M: FailureMode + 'd,
    F: for<'v> Fn(Observation<'v>) + Unpin,
    I: Borrow + receive::Mode<Wire<M>>,
{
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire<M>, I::Conn, Shared>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let this = self.get_mut();
        this.events.push(Event::Chunk(chunk.as_ref().to_vec()));
        match &mut this.ingress {
            Ingress::Armed(probe) => {
                (this.observe)(Observation::Deadline(
                    conn.deadline()
                        .try_ensure(driver.driver_ref().clock().now()),
                ));
                probe.chunk.hit();
                Outcome::Continue
            }
            Ingress::Fail => Outcome::Failed(receive::Failure::Contract),
            Ingress::Rejected(ready) => {
                conn.write()
                    .reject(body::Failure::Capacity(write::Reject::Header));
                ready.hit();
                Outcome::Continue
            }
            Ingress::Hold | Ingress::Held | Ingress::HalfClose(_) | Ingress::Shutting(_) => {
                Outcome::Continue
            }
            Ingress::Reply => {
                (this.observe)(Observation::Write(conn.write().try_bounded(
                    driver,
                    |_, prefix| {
                        prefix[..5].copy_from_slice(b"reply");
                        write::Draft::new(5, None, ())
                    },
                )));
                Outcome::Continue
            }
            Ingress::Owned(body) => {
                this.events.push(Event::Owned(
                    body.as_ref().map(|body| body.as_inner().as_ref().len()),
                ));
                let Some(body) = body.take() else {
                    return Outcome::Failed(receive::Failure::Contract);
                };
                (this.observe)(Observation::Owned(conn.write().admit(body, driver)));
                Outcome::Continue
            }
            Ingress::Close | Ingress::Released => Outcome::CloseAfter,
        }
    }
}

impl<'d, ID: Identity, M: FailureMode + 'd, F: for<'v> Fn(Observation<'v>) + Unpin>
    handler::Gated<'d, ID> for Probe<Wire<M>, receive::Gated, F>
{
    fn send_drained(
        self: Pin<&mut Self>,
        _: conn::Drained<'_, '_, '_, '_, '_, 'd, ID, Wire<M>, (), Shared>,
    ) -> handler::PostDrain {
        handler::PostDrain::Idle
    }
}

impl<'w, 'd: 'w, ID: Identity, M: FailureMode + 'd, F: for<'v> Fn(Observation<'v>) + Unpin>
    handler::Scoped<'w, 'd, ID> for Probe<Wire<M>, receive::Scoped, F>
{
    fn retention(&self) -> Budget {
        Budget::new(limits::Bound::new(0), 4096)
    }

    fn scoped_chunk(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, Wire<M>, Scope, Shared>,
        chunk: scoped::Input<'_, 'w, 'd, Wire<M>>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let this = self.get_mut();
        this.events.push(Event::Chunk(chunk.as_ref().to_vec()));
        match conn.state() {
            Scope::First => {
                *conn.state_mut() = Scope::Repeat;
                Outcome::Continue
            }
            Scope::Repeat => {
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
                Outcome::Continue
            }
        }
    }
}
