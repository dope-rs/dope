use std::cell::Cell;
use std::pin::{Pin, pin};

use dope::manifold::Manifold;
use dope::runtime::dispatcher::Idle;
use dope_fiber::abi::pending::Pending;
use dope_fiber::abi::ready::Ready;
use dope_test::{drive, with_session};

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
        sum += Ready::new(value).await;
    }
    sum
}

#[dope_gen::fiber_fn('d)]
async fn wait_repeated<'d>() -> usize {
    loop {
        Pending::<()>::new().await;
    }
}

fn assert_usize_output<'d>(_: &impl dope_fiber::abi::Fiber<'d, Output = usize>) {}

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
        drive(&mut sess, app.as_ref(), Ready::new(()));
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
