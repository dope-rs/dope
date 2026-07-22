use std::cell::Cell;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant};

use dope::driver::Config;
use dope::driver::ready::ReadySlot;
use dope::driver::token::{Epoch, ROUTE_FRAMEWORK, SlotIndex, Token};
use dope::runtime::profile::Throughput;
use dope::runtime::{Dispatcher, Executor, Idle, ShutdownTrigger};
use dope::{DriverContext, Event};
use pin_project::pin_project;

#[pin_project]
struct CascadingReady<'d> {
    ready: ReadySlot<'d>,
    polls: Rc<Cell<usize>>,
    polls_at_first_park: Rc<Cell<Option<usize>>>,
    stop: usize,
}

#[pin_project]
struct TurnClockProbe<'d> {
    ready: ReadySlot<'d>,
    batch_times: Rc<Cell<Option<(Instant, Instant)>>>,
    pre_park_time: Rc<Cell<Option<Instant>>>,
}

impl<'d> Dispatcher<'d> for CascadingReady<'d> {
    fn dispatch(self: Pin<&mut Self>, _event: Event<'d>, _driver: &mut DriverContext<'_, 'd>) {}

    fn activate(self: Pin<&mut Self>, _target: Token, _driver: &mut DriverContext<'_, 'd>) {
        let this = self.project();
        let polls = this.polls.get() + 1;
        this.polls.set(polls);
        if polls < *this.stop {
            this.ready.activate();
        }
    }

    fn pre_park(self: Pin<&mut Self>, _driver: &mut DriverContext<'_, 'd>) {
        let this = self.project();
        if this.polls_at_first_park.get().is_none() {
            this.polls_at_first_park.set(Some(this.polls.get()));
        }
    }

    fn idle(self: Pin<&Self>) -> Idle {
        Idle::Park(None)
    }
}

impl<'d> Dispatcher<'d> for TurnClockProbe<'d> {
    fn dispatch(self: Pin<&mut Self>, _event: Event<'d>, _driver: &mut DriverContext<'_, 'd>) {}

    fn activate(self: Pin<&mut Self>, _target: Token, driver: &mut DriverContext<'_, 'd>) {
        let start = driver.turn_now();
        thread::sleep(Duration::from_millis(20));
        self.project()
            .batch_times
            .set(Some((start, driver.turn_now())));
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.project().pre_park_time.set(Some(driver.turn_now()));
    }

    fn idle(self: Pin<&Self>) -> Idle {
        Idle::Park(None)
    }
}

#[test]
fn ready_cascade_is_followed_but_bounded_per_driver_turn() {
    let polls = Rc::new(Cell::new(0));
    let polls_at_first_park = Rc::new(Cell::new(None));
    let config = Config::for_tcp_profile::<Throughput>(1);
    Executor::new(config)
        .expect("executor")
        .enter(|mut session| -> io::Result<()> {
            let trigger = ShutdownTrigger::new()?;
            trigger.try_register(&mut session.driver_access())?;
            trigger.fire()?;

            let target = Token::new(ROUTE_FRAMEWORK - 1, SlotIndex::new(0), Epoch::INITIAL);
            let ready = session.driver().make_ready_slot(target)?;
            ready.activate();
            session.with_app(
                CascadingReady {
                    ready,
                    polls: Rc::clone(&polls),
                    polls_at_first_park: Rc::clone(&polls_at_first_park),
                    stop: 4,
                },
                |mut app| app.run(),
            )
        })
        .expect("runtime turn");

    assert_eq!(polls_at_first_park.get(), Some(2));
    assert_eq!(polls.get(), 4);
}

#[test]
fn turn_clock_is_stable_during_callbacks_and_refreshed_before_park() {
    let batch_times = Rc::new(Cell::new(None));
    let pre_park_time = Rc::new(Cell::new(None));
    let config = Config::for_tcp_profile::<Throughput>(1);
    Executor::new(config)
        .expect("executor")
        .enter(|mut session| -> io::Result<()> {
            let trigger = ShutdownTrigger::new()?;
            trigger.try_register(&mut session.driver_access())?;
            trigger.fire()?;

            let target = Token::new(ROUTE_FRAMEWORK - 1, SlotIndex::new(0), Epoch::INITIAL);
            let ready = session.driver().make_ready_slot(target)?;
            ready.activate();
            session.with_app(
                TurnClockProbe {
                    ready,
                    batch_times: Rc::clone(&batch_times),
                    pre_park_time: Rc::clone(&pre_park_time),
                },
                |mut app| app.run(),
            )
        })
        .expect("runtime turn");

    let (batch_start, batch_end) = batch_times.get().expect("ready callback");
    let pre_park = pre_park_time.get().expect("pre-park callback");
    assert_eq!(batch_start, batch_end);
    assert!(pre_park > batch_end);
}
