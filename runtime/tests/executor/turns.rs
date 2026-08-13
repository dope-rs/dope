//! Runtime turn-budget integration coverage.

use std::{
    cell::Cell,
    io,
    net::UdpSocket,
    rc::Rc,
    thread,
    time::{Duration, Instant},
};

use dope_core::driver::{
    self, ops,
    route::{Epoch, FRAMEWORK, KeyTag, SlotIndex, Token},
    schedule::ready::Slot,
    settings::{Config, ScheduleCapacity},
};
use dope_runtime::{executor::Executor, shutdown};
use dope_test::dispatch;

type TestTag = KeyTag<{ FRAMEWORK - 1 }>;

struct CascadingReady {
    polls: Rc<Cell<usize>>,
    polls_at_first_park: Rc<Cell<Option<usize>>>,
    stop: usize,
}

impl<'d> dispatch::Hooks<'d, Slot<'d, TestTag>> for CascadingReady {
    fn activate(
        &mut self,
        ready: &mut Slot<'d, TestTag>,
        _target: Token,
        _driver: &mut driver::retained::Context<'_, '_, 'd>,
    ) {
        let polls = self.polls.get() + 1;
        self.polls.set(polls);
        if polls < self.stop {
            ready.activate();
        }
    }

    fn pre_park(
        &mut self,
        _ready: &mut Slot<'d, TestTag>,
        _driver: &mut driver::retained::Context<'_, '_, 'd>,
    ) {
        if self.polls_at_first_park.get().is_none() {
            self.polls_at_first_park.set(Some(self.polls.get()));
        }
    }
}

struct TurnClockProbe {
    batch_times: Rc<Cell<Option<(Instant, Instant)>>>,
    first_pre_park_time: Rc<Cell<Option<Instant>>>,
}

impl<'d> dispatch::Hooks<'d, Slot<'d, TestTag>> for TurnClockProbe {
    fn activate(
        &mut self,
        _ready: &mut Slot<'d, TestTag>,
        _target: Token,
        driver: &mut driver::retained::Context<'_, '_, 'd>,
    ) {
        let start = driver.turn_now();
        thread::sleep(Duration::from_millis(20));
        self.batch_times.set(Some((start, driver.turn_now())));
    }

    fn pre_park(
        &mut self,
        _ready: &mut Slot<'d, TestTag>,
        driver: &mut driver::retained::Context<'_, '_, 'd>,
    ) {
        if self.first_pre_park_time.get().is_none() {
            self.first_pre_park_time.set(Some(driver.turn_now()));
        }
    }
}

struct ReadyStorm {
    polls: Rc<Cell<usize>>,
    polls_at_first_park: Rc<Cell<Option<usize>>>,
}

struct DeferredCompletion {
    dispatches: Rc<Cell<usize>>,
    shutdown: Option<shutdown::Trigger>,
}

impl<'d> dispatch::Hooks<'d, ()> for DeferredCompletion {
    fn dispatch(
        &mut self,
        _ready: &mut (),
        event: &dope_core::io::Event<'d>,
    ) -> dispatch::EventDecision {
        assert!(matches!(event.kind(), dope_core::io::event::Kind::Recv(..)));
        let dispatches = self.dispatches.get() + 1;
        self.dispatches.set(dispatches);
        if dispatches == 1 {
            dispatch::EventDecision::Defer
        } else {
            self.shutdown
                .take()
                .expect("single shutdown")
                .fire()
                .expect("fire shutdown");
            dispatch::EventDecision::Consume
        }
    }
}

impl<'d> dispatch::Hooks<'d, Box<[Slot<'d, TestTag>]>> for ReadyStorm {
    fn activate(
        &mut self,
        _ready: &mut Box<[Slot<'d, TestTag>]>,
        _target: Token,
        _driver: &mut driver::retained::Context<'_, '_, 'd>,
    ) {
        self.polls.set(self.polls.get() + 1);
    }

    fn pre_park(
        &mut self,
        _ready: &mut Box<[Slot<'d, TestTag>]>,
        _driver: &mut driver::retained::Context<'_, '_, 'd>,
    ) {
        if self.polls_at_first_park.get().is_none() {
            self.polls_at_first_park.set(Some(self.polls.get()));
        }
    }
}

#[test]
fn ready_cascade_is_followed_but_bounded_per_driver_turn() {
    let polls = Rc::new(Cell::new(0));
    let polls_at_first_park = Rc::new(Cell::new(None));
    let config = Config::for_quic_udp(2, 8).expect("driver config");
    let (source, trigger) = shutdown::Pair::new().expect("shutdown pair").split();
    trigger.fire().expect("fire shutdown");
    Executor::new(config)
        .expect("executor")
        .with_shutdown(source)
        .expect("register shutdown")
        .enter(|mut session| -> io::Result<()> {
            let driver = session.driver_access().driver_ref();
            let target = driver
                .targets::<TestTag>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let ready = driver.ready().make_ready_slot(target)?;
            ready.activate();
            session
                .with_app(
                    dispatch::Builder::new(CascadingReady {
                        polls: Rc::clone(&polls),
                        polls_at_first_park: Rc::clone(&polls_at_first_park),
                        stop: 4,
                    })
                    .ready::<{ FRAMEWORK - 1 }>(ready),
                    |mut app| app.run(),
                )?
                .map(drop)
        })
        .expect("runtime turn");

    assert_eq!(polls_at_first_park.get(), Some(2));
    assert_eq!(polls.get(), 4);
}

#[test]
fn ready_storm_is_bounded_and_preserved_across_driver_turns() {
    let polls = Rc::new(Cell::new(0));
    let polls_at_first_park = Rc::new(Cell::new(None));
    let config = Config::for_quic_udp(2, 8).expect("driver config");
    let scheduler = config
        .scheduler()
        .with_ready(ScheduleCapacity::fixed::<300>());
    let config = config.with_scheduler(scheduler);
    let (source, trigger) = shutdown::Pair::new().expect("shutdown pair").split();
    trigger.fire().expect("fire shutdown");
    Executor::new(config)
        .expect("executor")
        .with_shutdown(source)
        .expect("register shutdown")
        .enter(|mut session| -> io::Result<()> {
            let driver = session.driver_access().driver_ref();
            let space = driver.targets::<TestTag>();
            let targets = (0..300_u16).map(|index| {
                space
                    .bind(SlotIndex::from(index), Epoch::INITIAL)
                    .dispatch()
            });
            let slots = driver.ready().make_ready_slots(targets)?;
            for slot in &slots {
                slot.activate();
            }
            session
                .with_app(
                    dispatch::Builder::new(ReadyStorm {
                        polls: Rc::clone(&polls),
                        polls_at_first_park: Rc::clone(&polls_at_first_park),
                    })
                    .ready_set::<{ FRAMEWORK - 1 }>(slots),
                    |mut app| app.run(),
                )?
                .map(drop)
        })
        .expect("runtime turns");

    assert_eq!(polls_at_first_park.get(), Some(256));
    assert_eq!(polls.get(), 300);
}

#[test]
fn turn_clock_is_stable_during_callbacks_and_refreshed_before_park() {
    let batch_times = Rc::new(Cell::new(None));
    let first_pre_park_time = Rc::new(Cell::new(None));
    let config = Config::for_quic_udp(2, 8).expect("driver config");
    let (source, trigger) = shutdown::Pair::new().expect("shutdown pair").split();
    trigger.fire().expect("fire shutdown");
    Executor::new(config)
        .expect("executor")
        .with_shutdown(source)
        .expect("register shutdown")
        .enter(|mut session| -> io::Result<()> {
            let driver = session.driver_access().driver_ref();
            let target = driver
                .targets::<TestTag>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let ready = driver.ready().make_ready_slot(target)?;
            ready.activate();
            session
                .with_app(
                    dispatch::Builder::new(TurnClockProbe {
                        batch_times: Rc::clone(&batch_times),
                        first_pre_park_time: Rc::clone(&first_pre_park_time),
                    })
                    .ready::<{ FRAMEWORK - 1 }>(ready),
                    |mut app| app.run(),
                )?
                .map(drop)
        })
        .expect("runtime turn");

    let (batch_start, batch_end) = batch_times.get().expect("ready callback");
    let pre_park = first_pre_park_time.get().expect("pre-park callback");
    assert_eq!(batch_start, batch_end);
    assert!(pre_park > batch_end);
}

#[test]
fn deferred_completion_is_the_only_event_retained_across_turns() {
    let dispatches = Rc::new(Cell::new(0));
    let config = Config::for_quic_udp(2, 8).expect("driver config");
    let (source, trigger) = shutdown::Pair::new().expect("shutdown pair").split();
    Executor::new(config)
        .expect("executor")
        .with_shutdown(source)
        .expect("register shutdown")
        .enter(|mut session| -> io::Result<()> {
            let (socket, address) = {
                let mut driver = session.driver_access();
                let (socket, address) = ops::Bootstrap::bind_datagram_slot(
                    &mut driver,
                    "127.0.0.1:0".parse().expect("address"),
                )?;
                let target = driver
                    .driver_ref()
                    .targets::<KeyTag<1>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL);
                let slots = driver.flight_slots::<KeyTag<1>>(1)?;
                drop(
                    ops::Submit::submit_recv(&mut driver, &slots, &socket, target)
                        .expect("arm receive"),
                );
                (socket, address)
            };

            let peer = UdpSocket::bind("127.0.0.1:0")?;
            peer.send_to(b"defer", address)?;
            let requested = session.with_app(
                dispatch::Builder::new(DeferredCompletion {
                    dispatches: Rc::clone(&dispatches),
                    shutdown: Some(trigger),
                })
                .probe::<1>(),
                |mut app| app.run(),
            )??;
            drop(requested);

            let mut driver = session.driver_access();
            ops::Files::close(&mut driver, socket);
            Ok(())
        })
        .expect("runtime completion dispatch");

    assert_eq!(dispatches.get(), 2);
}
