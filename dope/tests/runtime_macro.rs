use std::cell::Cell;
use std::pin::{Pin, pin};

use dope::manifold::Manifold;
use dope::runtime::profile;
use dope::{Driver, DriverConfig, Executor, Idle};

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

    fn dispatch(self: Pin<&mut Self>, _ev: dope::Event, _driver: &'d Driver) {
        self.dispatch_calls.set(self.dispatch_calls.get() + 1);
    }
    fn pre_park(self: Pin<&mut Self>, _driver: &'d Driver) {
        self.tick_calls.set(self.tick_calls.get() + 1);
    }
    fn idle(self: Pin<&Self>) -> Idle {
        self.idle_calls.set(self.idle_calls.get() + 1);
        if self.pending {
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

fn make_exec() -> Executor {
    let cfg = dope::DriverCfg::for_profile::<profile::Throughput>();
    Executor::new(cfg).expect("executor")
}

#[test]
fn route_consts() {
    assert_eq!(Dispatcher::A_ROUTE, 3);
    assert_eq!(Dispatcher::B_ROUTE, 0);
    assert_eq!(Dispatcher::C_ROUTE, 1);
}

#[test]
fn build_initializes_fields() {
    let app = make_dispatcher();
    assert_eq!(app.a.dispatch_calls.get(), 0);
    assert_eq!(app.b.tick_calls.get(), 0);
    assert_eq!(app.c.idle_calls.get(), 0);
}

#[test]
fn block_on_ticks_every_field() {
    let exec = make_exec();
    let mut sess = exec.enter();
    let mut app = pin!(make_dispatcher());
    dope_extra::block_on(
        &mut sess,
        app.as_mut(),
        dope::fiber::Fiber::new(std::future::ready(())),
    );
    assert!(app.a.tick_calls.get() >= 1);
    assert!(app.b.tick_calls.get() >= 1);
    assert!(app.c.tick_calls.get() >= 1);
}
