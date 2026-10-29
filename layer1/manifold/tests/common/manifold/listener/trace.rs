use std::{
    cell::Cell,
    convert, io,
    net::{SocketAddr, TcpStream},
    pin::Pin,
    rc::Rc,
    task,
};

use dope_core::{
    driver::{
        retained, route,
        route::table::conn::Capacity,
        schedule::clock,
        settings,
        settings::{
            claim::{Claim, Needs},
            demands::{Accept, Files, slots::Slots},
            errors,
        },
    },
    io::socket::{Backlog, option},
    random::Seed,
};
use dope_fiber::{abi, context};
use dope_manifold::{
    app::{
        env::quota::{self, Policy},
        receive,
        receive::Outcome,
    },
    network::listener::{
        config, conn, handler,
        handler::{Application, failures, retire, send},
    },
    timing::Throughput,
};
use dope_net::{tcp, tcp::Tcp, wire::Identity};
use dope_test::harness::{peer::Peer, signals::Gate};
use o3::buffer::{bytes::TryRetain, resident};

#[derive(Default)]
pub struct Trace {
    pub(crate) accepted: Cell<usize>,
    pub(crate) closed: Gate,
    pub(crate) received: Gate,
}

pub struct App<F> {
    pub(crate) trace: Rc<Trace>,
    pub(crate) gate: Gate,
    pub(crate) observe: F,
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime)]
pub struct Pair<A, B> {
    #[pin]
    #[manifold]
    pub first: A,
    #[pin]
    #[manifold]
    pub second: B,
}

pub enum Event<'a, 'd, ID: route::Identity> {
    Accept(&'a Connection<'d, ID>),
    Close(&'a Connection<'d, ID>, conn::Id<'d, ID>),
}

pub enum Connection<'d, ID: route::Identity> {
    Opening,
    Accepted(conn::Id<'d, ID>),
}

pub fn check<ID: route::Identity>(event: Event<'_, '_, ID>) {
    match event {
        Event::Accept(state) => assert!(matches!(state, Connection::Opening)),
        Event::Close(state, key) => {
            assert!(matches!(state, Connection::Accepted(id) if *id == key));
            assert_eq!(key.index().into_usize(), 0);
        }
    }
}

impl<'d, ID: route::Identity, F: for<'a, 'b> Fn(Event<'a, 'b, ID>)> Application<'d, ID> for App<F> {
    type Build = convert::Infallible;
    type Conn = Connection<'d, ID>;
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
        Ok(Connection::Opening)
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn accept(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, Self::Conn>,
    ) -> Outcome {
        (self.observe)(Event::Accept(conn.state()));
        *conn.state_mut() = Connection::Accepted(conn.id());
        let this = self.as_ref().get_ref();
        this.trace.accepted.set(this.trace.accepted.get() + 1);
        this.gate.hit();
        Outcome::Continue
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, Self::Conn>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, Self::Conn>
    where
        'd: 'w,
    {
        (self.observe)(Event::Close(conn.state(), conn.id()));
        self.as_ref().get_ref().trace.closed.hit();
        conn
    }
}

impl<'d, ID: route::Identity, F: for<'a, 'b> Fn(Event<'a, 'b, ID>)> handler::Borrowed<'d, ID>
    for App<F>
{
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, Self::Conn>,
        _: R,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        self.as_ref().get_ref().trace.received.hit();
        Outcome::Continue
    }
}

impl One {
    pub fn config() -> config::Prepared<Tcp> {
        config::Prepared {
            connections: Capacity::fixed::<1>(),
            direct_flights: config::Flights::Minimum,
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            backlog: Backlog::fixed::<1>(),
            stream: tcp::StreamConfig {
                no_delay: Some(option::Switch::On),
                ..Default::default()
            },
            per_ip: Some(quota::PerIp::UNLIMITED),
            transport: Default::default(),
            egress: Default::default(),
        }
    }
}

impl One {
    pub fn driver() -> Result<settings::Config, errors::Claim> {
        let claim = Claim::new(settings::Routes::singleton::<route::KeyTag<0>>())
            .files(Files::accept(Accept::from(Slots::fixed::<1>())));
        Needs::new().include(claim)?.resolve::<Throughput>()
    }
}

pub trait Connect {
    fn connect_n(&self, n: usize) -> io::Result<Vec<TcpStream>>;
}
impl Connect for SocketAddr {
    fn connect_n(&self, n: usize) -> io::Result<Vec<TcpStream>> {
        (0..n).map(|_| Peer::at(*self).try_connect()).collect()
    }
}

pub struct Turns(pub(crate) u32);

pub struct One;

impl Policy for One {
    const PER_IP: quota::PerIp = quota::PerIp::fixed::<1>();
}

impl<'d> abi::Fiber<'d> for Turns {
    type Output = ();

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<()> {
        let (mut self_, cx) = call.into_parts();
        if self_.0 == 0 {
            return task::Poll::Ready(());
        }
        self_.0 -= 1;
        cx.wake();
        task::Poll::Pending
    }
}

pub struct Active {
    pub(crate) hits: Rc<Cell<usize>>,
    pub(crate) gate: Gate,
}

impl<'d, ID: route::Identity> Application<'d, ID> for Active {
    type Build = convert::Infallible;
    type Conn = ();
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
        Ok(())
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn accept(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, ()>,
    ) -> Outcome {
        conn.wake_target().wake();
        Outcome::Continue
    }

    fn activate<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, ()>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, ()>
    where
        'd: 'w,
    {
        let this = self.as_ref().get_ref();
        this.hits.set(this.hits.get() + 1);
        this.gate.hit();
        conn
    }
}

impl<'d, ID: route::Identity> handler::Borrowed<'d, ID> for Active {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, ()>,
        _: R,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        Outcome::Continue
    }
}
