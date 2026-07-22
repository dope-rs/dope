use std::cell::Cell;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

use dope::Event;
use dope::driver::profile::DriverProfile;
use dope::driver::token::{Epoch, ROUTE_FRAMEWORK, SlotIndex, Token};
use dope::runtime::profile::RuntimeProfile;
use dope::runtime::{Dispatcher, Idle};
use dope_fiber::{Context, Either, Fiber, SessionExt, WaitQueue, Waiter};
use dope_test::with_session_for;
use o3::cell::BrandCell;

struct OneTask;

impl DriverProfile for OneTask {
    const RING_ENTRIES: u32 = 64;
    const READY_SLOTS: usize = 2;
}

impl RuntimeProfile for OneTask {
    const IDLE_WINDOW: Duration = Duration::ZERO;
}

struct BlockState {
    wait: Pin<Box<WaitQueue>>,
    allowed: Cell<bool>,
    polls: Cell<u32>,
}

struct Blocked<'d> {
    state: Rc<BlockState>,
    waiter: Waiter<'d>,
}

impl<'d> Fiber<'d> for Blocked<'d> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let waiter = unsafe { Pin::new_unchecked(&this.waiter) };
        let state = &this.state;
        state.polls.set(state.polls.get() + 1);
        if state.allowed.get() {
            return Poll::Ready(());
        }
        assert!(state.wait.as_ref().try_register(waiter, context.as_ref()));
        Poll::Pending
    }
}

struct TestDispatcher;

impl<'d> Dispatcher<'d> for TestDispatcher {
    fn dispatch(
        self: Pin<&mut Self>,
        _event: Event<'d>,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) {
    }

    fn activate(self: Pin<&mut Self>, _target: Token, _driver: &mut dope::DriverContext<'_, 'd>) {}

    fn pre_park(self: Pin<&mut Self>, _driver: &mut dope::DriverContext<'_, 'd>) {}

    fn idle(self: Pin<&Self>) -> Idle {
        Idle::Busy
    }
}

#[test]
fn race_unlinks_losing_waiter() {
    with_session_for::<OneTask, _>(|mut session| {
        let state = Rc::new(BlockState {
            wait: Box::pin(WaitQueue::with_capacity(1)),
            allowed: Cell::new(false),
            polls: Cell::new(0),
        });
        let app = pin!(BrandCell::new(TestDispatcher));
        let winner = session
            .block_on(
                app.as_ref(),
                dope_fiber::race(
                    Blocked {
                        state: Rc::clone(&state),
                        waiter: Waiter::new(),
                    },
                    dope_fiber::ready(()),
                ),
            )
            .expect("runtime park");
        assert!(matches!(winner, Either::Right(())));
        assert!(state.wait.is_empty());
        state.wait.as_ref().wake();
    });
}

struct WakeState {
    wait: Pin<Box<WaitQueue>>,
    allowed: Cell<bool>,
    polls: Cell<u32>,
    ticks: Cell<u32>,
    foreign_wakes: Cell<u32>,
    foreign: Token,
}

struct WakeDriven<'d> {
    state: Rc<WakeState>,
    waiter: Waiter<'d>,
}

impl<'d> Fiber<'d> for WakeDriven<'d> {
    type Output = (u32, u32);

    fn poll(self: Pin<&mut Self>, context: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let waiter = unsafe { Pin::new_unchecked(&this.waiter) };
        let state = &this.state;
        state.polls.set(state.polls.get() + 1);
        if state.allowed.get() {
            return Poll::Ready((state.polls.get(), state.foreign_wakes.get()));
        }
        assert!(state.wait.as_ref().try_register(waiter, context.as_ref()));
        Poll::Pending
    }
}

struct WakeDispatcher {
    state: Rc<WakeState>,
}

impl<'d> Dispatcher<'d> for WakeDispatcher {
    fn dispatch(
        self: Pin<&mut Self>,
        _event: Event<'d>,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) {
        let _ = self;
    }

    fn activate(self: Pin<&mut Self>, target: Token, _driver: &mut dope::DriverContext<'_, 'd>) {
        let state = &self.as_ref().get_ref().state;
        if target == state.foreign {
            state.foreign_wakes.set(state.foreign_wakes.get() + 1);
        }
    }

    fn pre_park(self: Pin<&mut Self>, _driver: &mut dope::DriverContext<'_, 'd>) {
        let state = &self.as_ref().get_ref().state;
        state.ticks.set(state.ticks.get() + 1);
        if state.ticks.get() == 3 {
            state.allowed.set(true);
            state.wait.as_ref().wake();
        }
    }

    fn idle(self: Pin<&Self>) -> Idle {
        let _ = self;
        Idle::Busy
    }
}

#[test]
fn block_on_consumes_only_its_exact_wake_token() {
    with_session_for::<OneTask, _>(|mut session| {
        let foreign = Token::new(ROUTE_FRAMEWORK, SlotIndex::new(0), Epoch::INITIAL);
        let state = Rc::new(WakeState {
            wait: Box::pin(WaitQueue::with_capacity(1)),
            allowed: Cell::new(false),
            polls: Cell::new(0),
            ticks: Cell::new(0),
            foreign_wakes: Cell::new(0),
            foreign,
        });
        let foreign_slot = session
            .driver()
            .make_ready_slot(foreign)
            .expect("ready slot");
        foreign_slot.activate();
        let app = pin!(BrandCell::new(WakeDispatcher {
            state: Rc::clone(&state),
        }));
        let output = session
            .block_on(
                app.as_ref(),
                WakeDriven {
                    state: Rc::clone(&state),
                    waiter: Waiter::new(),
                },
            )
            .expect("runtime park");
        assert_eq!(output, (2, 1));
    });
}
