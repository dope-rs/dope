use std::cell::Cell;
use std::marker::PhantomData;
use std::pin::{Pin, pin};

use dope::manifold::Manifold;
use dope::manifold::connector::{self, Codec, Ctx, Requests, Stateless};
use dope::runtime::Idle;
use dope_fiber::Fiber;
use dope_test::{drive, poll_until_ready, with_context, with_session};
use o3::buffer::Shared;

#[dope_gen::handler]
async fn echo(x: u32) -> u32 {
    x + 1
}

#[dope_gen::handler]
async fn await_chain(x: u32) -> u32 {
    let a = ready_value(x).await;
    ready_value(a + 10).await
}

fn ready_value<'d>(value: u32) -> impl Fiber<'d, Output = u32> {
    dope_fiber::ready(value)
}

struct TestCodec;

impl Codec for TestCodec {
    type Head = ();
    type ParseState = ();

    fn parse(&self, _state: &mut Self::ParseState, _buf: &Shared) -> Option<(Self::Head, usize)> {
        None
    }
}

struct TestProtocol {
    codec: TestCodec,
}

struct TestIo<'d>(PhantomData<fn(&'d ()) -> &'d ()>);

impl<'d> TestIo<'d> {
    fn activate(
        &self,
        _token: dope::driver::token::Token,
        _ready: dope::driver::ready::ReadyKey<'d>,
    ) -> bool {
        true
    }

    fn drain_requests(
        &self,
        _token: dope::driver::token::Token,
        _push: impl FnMut(Vec<u8>) -> Result<(), Vec<u8>>,
    ) -> Option<Requests> {
        Some(Requests::default())
    }
}

struct TestSession<'d> {
    protocol: TestProtocol,
    io: TestIo<'d>,
}

#[dope_gen::connector_session(codec = protocol.codec, io = io)]
impl<'d> connector::Session<'d> for TestSession<'d> {
    type Codec = TestCodec;
    type ConnState = Stateless;
    type Send = Vec<u8>;

    fn connect(&mut self, _ctx: &mut Ctx<'_, 'd, Self>) {}

    fn response(&mut self, _head: (), _ctx: &mut Ctx<'_, 'd, Self>) {}

    fn disconnect(&mut self, _ctx: &mut Ctx<'_, 'd, Self>) {}
}

struct Counter<const ID: u8> {
    dispatch_calls: Cell<u32>,
    tick_calls: Cell<u32>,
    idle_calls: Cell<u32>,
    pending: bool,
}

impl<const ID: u8> Counter<ID> {
    fn new(pending: bool) -> Self {
        Self {
            dispatch_calls: Cell::new(0),
            tick_calls: Cell::new(0),
            idle_calls: Cell::new(0),
            pending,
        }
    }
}

impl<'d, const ID: u8> Manifold<'d> for Counter<ID> {
    const ID: u8 = ID;

    fn dispatch(
        self: Pin<&mut Self>,
        _ev: dope::Event<'d>,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) {
        let this = self.as_ref().get_ref();
        this.dispatch_calls.set(this.dispatch_calls.get() + 1);
    }
    fn pre_park(self: Pin<&mut Self>, _driver: &mut dope::DriverContext<'_, 'd>) {
        let this = self.as_ref().get_ref();
        this.tick_calls.set(this.tick_calls.get() + 1);
    }
    fn idle(self: Pin<&Self>) -> Idle {
        let this = self.as_ref().get_ref();
        this.idle_calls.set(this.idle_calls.get() + 1);
        if this.pending {
            Idle::Busy
        } else {
            Idle::Park(None)
        }
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Dispatcher {
    #[pin]
    #[manifold]
    a: Counter<3>,
    #[pin]
    #[manifold]
    b: Counter<0>,
    #[pin]
    #[manifold]
    c: Counter<1>,
}

fn make_dispatcher() -> Dispatcher {
    Dispatcher {
        a: Counter::new(false),
        b: Counter::new(false),
        c: Counter::new(false),
    }
}

#[dope_gen::fiber_fn('d)]
async fn sum_repeated<'d>() -> usize {
    let mut sum = 0usize;
    for value in 1usize..=4 {
        sum += dope_fiber::ready(value).await;
    }
    sum
}

#[dope_gen::fiber_fn('d)]
async fn wait_repeated<'d>() -> usize {
    loop {
        dope_fiber::pending::<()>().await;
    }
}

fn assert_usize_output<'d>(_: &impl dope_fiber::Fiber<'d, Output = usize>) {}

#[test]
fn handler_returns_generated_fiber() {
    with_context(|cx| assert_eq!(poll_until_ready(cx, echo(7)), 8));
}

#[test]
fn connector_session_generates_structural_methods() {
    let session = TestSession {
        protocol: TestProtocol { codec: TestCodec },
        io: TestIo(PhantomData),
    };
    assert!(std::ptr::eq(
        connector::Session::codec(&session),
        &session.protocol.codec
    ));
}

#[test]
fn nested_awaits_run_to_completion() {
    with_context(|cx| assert_eq!(poll_until_ready(cx, await_chain(1)), 11));
}

#[test]
fn route_consts() {
    assert_eq!(Dispatcher::A_ROUTE, 3);
    assert_eq!(Dispatcher::B_ROUTE, 0);
    assert_eq!(Dispatcher::C_ROUTE, 1);
}

#[test]
fn block_ticks_every_field() {
    with_session(|mut sess| {
        let app = pin!(o3::cell::BrandCell::new(make_dispatcher()));
        drive(&mut sess, app.as_ref(), dope_fiber::ready(()));
        let d = app.as_ref().borrow_pin_mut(sess.token());
        assert!(d.a.tick_calls.get() >= 1);
        assert!(d.b.tick_calls.get() >= 1);
        assert!(d.c.tick_calls.get() >= 1);
    });
}

#[test]
fn fiber_fn_runs_repeated_awaits() {
    let pending = wait_repeated();
    assert_usize_output(&pending);
    drop(pending);
    with_session(|mut sess| {
        let app = pin!(o3::cell::BrandCell::new(make_dispatcher()));
        assert_eq!(drive(&mut sess, app.as_ref(), sum_repeated()), 10);
    });
}
