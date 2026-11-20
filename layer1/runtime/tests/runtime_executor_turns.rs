use dope_core::{
    driver::life::{creation::Attempt, recovery},
    io::{
        fd::handles::Descriptor,
        socket::{self, install::Install},
    },
};
use dope_runtime::executor::session::Completion;
use dope_test::{
    harness::{observe::Trace, scenario::TIMEOUT},
    reclaim::Reports,
};
pub mod common;

use std::{cell::Cell, net::UdpSocket, ops::ControlFlow, pin, rc::Rc, time::Instant};

use dope_core::{
    driver::{
        retained::flight,
        route::{self, Epoch, KeyTag, Tag, indices::SlotIndex},
        settings::{
            self, Config,
            demands::{Schedule, slots::Slots},
        },
    },
    io::{self},
};
use dope_runtime::{
    executor::{Executor, raw, session},
    shutdown,
};

use crate::common::{
    runtime::executor::turns::*,
    scenario::{dispatch, rt::Configured},
};

#[test]
fn bounds_ready_cascade() {
    let mut finish = Completion::new();
    let polls = Rc::new(Cell::new(Some(0)));
    let first = Rc::new(Cell::new(None));
    let config = Configured::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    stopped(config, |mut session| {
        let driver = session.driver().driver_ref();
        let target = driver
            .targets::<KeyTag<254>>()
            .bind(SlotIndex::ZERO, Epoch::INITIAL)
            .dispatch();
        let ready = (driver.ready().make_slot(target)).expect("ready slot");
        ready.activate();
        session
            .run_quiesce(
                &mut finish,
                dispatch::Builder::new(Cascade {
                    polls: Rc::clone(&polls),
                    first: Rc::clone(&first),
                    stop: 4,
                })
                .ready::<KeyTag<254>>(ready),
                TIMEOUT,
            )
            .expect("application teardown")
            .expect("application drive");
    });

    assert_eq!(first.get(), Some(2));
    assert_eq!(polls.get(), Some(4));
}

#[test]
fn preserves_root_and_park_admission() {
    let mut finish = Completion::new();
    let activated = Cell::new(None);
    let parked = Cell::new(None);
    let config = Configured::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    stopped(config, |mut session| {
        let driver = session.driver().driver_ref();
        let target = driver
            .targets::<KeyTag<254>>()
            .bind(SlotIndex::ZERO, Epoch::INITIAL)
            .dispatch();
        let ready = driver.ready().make_slot(target).expect("ready slot");
        ready.activate();
        let result = session
            .with_quiesce(
                &mut finish,
                dispatch::Builder::new(Saturate {
                    activated: &activated,
                    parked: &parked,
                })
                .ready::<KeyTag<254>>(ready),
                TIMEOUT,
                |app| {
                    matches!(
                        app.drive_until(Admitted),
                        Ok(std::ops::ControlFlow::Break(true))
                    )
                },
            )
            .expect("application teardown");
        assert!(result);
        assert_eq!(
            activated.get(),
            Some(dope_core::driver::schedule::LIMIT - 2)
        );
        assert_eq!(parked.get(), Some(0));
    });
}

#[test]
fn preserves_ready_storm() {
    let mut finish = Completion::new();
    let polls = Rc::new(Cell::new(Some(0)));
    let first = Rc::new(Cell::new(None));
    let config = Configured::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    let sched = settings::Schedule::fixed::<0, 0>().resolve(Schedule::new(
        Slots::fixed::<300>(),
        config.schedule().timers(),
    ));
    let config = config.set_schedule(sched);
    stopped(config, |mut session| {
        let driver = session.driver().driver_ref();
        let space = driver.targets::<KeyTag<254>>();
        let targets = [space.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch(); 300];
        let slots = (targets
            .iter()
            .copied()
            .map(|target| driver.ready().make_slot(target))
            .collect::<Result<Vec<_>, _>>()
            .map(Vec::into_boxed_slice))
        .expect("ready slots");
        for slot in &slots {
            slot.activate();
        }
        session
            .run_quiesce(
                &mut finish,
                dispatch::Builder::new(Storm {
                    polls: Rc::clone(&polls),
                    first: Rc::clone(&first),
                })
                .ready_set::<KeyTag<254>>(slots),
                TIMEOUT,
            )
            .expect("application teardown")
            .expect("application drive");
    });

    assert_eq!(first.get(), Some(256));
    assert_eq!(polls.get(), Some(300));
}

#[test]
fn keeps_turn_clock_stable() {
    let mut finish = Completion::new();
    let stable = Rc::new(Cell::new(None));
    let config = Configured::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    stopped(config, |mut session| {
        let driver = session.driver().driver_ref();
        let target = driver
            .targets::<KeyTag<254>>()
            .bind(SlotIndex::ZERO, Epoch::INITIAL)
            .dispatch();
        let ready = (driver.ready().make_slot(target)).expect("ready slot");
        ready.activate();
        session
            .run_quiesce(
                &mut finish,
                dispatch::Builder::new(Clock {
                    stable: Rc::clone(&stable),
                })
                .ready::<KeyTag<254>>(ready),
                TIMEOUT,
            )
            .expect("application teardown")
            .expect("application drive");
    });

    assert_eq!(stable.get(), Some(true));
}

#[test]
fn retains_deferred_event() {
    let mut create = Attempt::new();
    let mut finish = Completion::new();
    let mut calls = Some(0);
    let mut fired = None;
    let config = Configured::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    let pair = (shutdown::Pair::new()).expect("shutdown pair");
    let (source, trigger) = pair.split();
    let executor = Executor::new(&mut create, config);
    assert!(executor.is_ok(), "executor");
    let Some(executor) = executor.ok() else {
        return;
    };
    let executor = executor.shutdown(source);
    assert!(executor.is_ok(), "register shutdown");
    let Some(executor) = executor.ok() else {
        return;
    };
    let mut reports = Reports::new::<route::Route>(config, []);

    executor.run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let (socket, address) =
                    {
                        let mut driver = session.driver();
                        let (socket, address) = (Install::bind_udp(
                            driver.reborrow(),
                            ([127, 0, 0, 1], 0).into(),
                            socket::Config::for_datagram(None),
                        ))
                        .expect("bind datagram socket");
                        let target = driver
                            .driver_ref()
                            .targets::<KeyTag<1>>()
                            .bind(SlotIndex::ZERO, Epoch::INITIAL);
                        let flights =
                            (driver.storage.reborrow().try_flights(
                                flight::Need::<KeyTag<1>>::new(flight::Capacity::fixed::<1>()),
                            ))
                            .expect("receive flight");
                        let slots = flights.slots();
                        let flight =
                            (slots.recv(driver.reborrow(), &socket, target)).expect("arm receive");
                        drop(flight);
                        (socket, address)
                    };

                let peer = (UdpSocket::bind("127.0.0.1:0")).expect("bind deferred completion peer");
                assert!(
                    peer.send_to(b"defer", address).is_ok(),
                    "send deferred completion datagram"
                );
                let driven = session.with_quiesce(
                    &mut finish,
                    Fallback {
                        first: Missing {
                            other_route: &Trace::<_, 0>::new(),
                            app: None,
                        },
                        second: dispatch::Builder::new(Deferred {
                            calls: &mut calls,
                            shutdown: Some(trigger),
                            fired: &mut fired,
                        })
                        .probe::<KeyTag<1>>(),
                    },
                    TIMEOUT,
                    |app| app.run_for(TIMEOUT),
                );

                let mut driver = session.driver();
                Descriptor::from(socket).close(&mut driver);
                let fired = fired.transpose().expect("shutdown");
                assert!(
                    matches!(driven, Ok(Ok(_))),
                    "application drive and teardown"
                );
                assert_eq!(fired, Some(()), "shutdown");
            },
            recovery::Policy::new(|event| reports.push(event), common::recover),
        );
        let events = reports.events().expect("native retirement succeeds");
        assert!(
            events.is_empty()
                || matches!(
                    events,
                    [io::Reclaimed::Recv {
                        result: Err(error), flow: io::event::batch::Flow::Last, ..
                    }] if *error == dope_core::Errno::CANCELED
                ),
            "unexpected deferred receive retirement: {events:?}"
        );

        assert!(result.is_ok(), "runtime completion dispatch");

        assert_eq!(calls, Some(2));
    });
}

#[test]
fn dispatches_the_returned_route_only_in_the_remaining_fields() {
    let config = Configured::quic::<2, 8>().config();
    let mut reports = Reports::new::<route::Route>(config, []);
    dope_test::scope::Driver::new(config).with_scope(
        |scope| {
            let (_, mut driver, mut schedule) = scope.split();
            let udp = UdpSocket::bind(("127.0.0.1", 0)).expect("bind datagram");
            udp.connect(udp.local_addr().expect("local address"))
                .expect("connect datagram");
            let (socket, _) =
                Install::adopt_udp(driver.reborrow(), udp.into()).expect("adopt datagram");
            let reference = driver.driver_ref();
            let zero = reference
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL);
            let one = reference
                .targets::<KeyTag<1>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL);
            let capacity = flight::Capacity::fixed::<1>();
            let needs = flight::Need::<KeyTag<0>>::new(capacity)
                .pair(flight::Need::<KeyTag<1>>::new(capacity));
            let (first, second) = driver
                .storage
                .reborrow()
                .try_flights(needs)
                .expect("flight pair")
                .slots();
            drop(
                first
                    .shutdown(driver.reborrow(), &socket, zero)
                    .expect("first shutdown"),
            );
            drop(
                second
                    .shutdown(driver.reborrow(), &socket, one)
                    .expect("second shutdown"),
            );

            let deadline = Instant::now() + TIMEOUT;
            let mut saved = None;
            let mut observed = false;
            while !observed {
                let mut guard = schedule.begin();
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .expect("completion deadline");
                driver
                    .wait(guard.reactor(), Some(remaining))
                    .expect("wait for shutdown");
                let (reactor, turn) = guard.split();
                let result = driver.dispatch(reactor, |mut slot, driver| {
                    let Some(mut other) = saved.take() else {
                        saved = Some(slot.into_inner());
                        return ControlFlow::Continue(());
                    };
                    if slot.route() != KeyTag::<0>::ROUTE {
                        std::mem::swap(slot.get_mut(), &mut other);
                    }
                    assert_eq!(other.route(), KeyTag::<1>::ROUTE);
                    let swap = Swap {
                        other: Cell::new(other),
                        visits: Cell::new(0),
                    };
                    let app = pin::pin!(Missing {
                        other_route: Missing {
                            other_route: dispatch::Builder::new(&swap).probe::<KeyTag<0>>(),
                            app: Some(dispatch::Builder::new(&swap).probe::<KeyTag<1>>()),
                        },
                        app: Some(dispatch::Builder::new(&swap).probe::<KeyTag<2>>()),
                    });
                    let result = raw::App::dispatch(
                        app,
                        raw::Dispatch::new(slot),
                        turn.reborrow(),
                        driver.reborrow(),
                    );
                    let returned = result.break_value().expect("return earlier route");
                    assert_eq!(returned.route(), KeyTag::<0>::ROUTE);
                    assert_eq!(swap.visits.get(), 2);
                    observed = true;
                    ControlFlow::Continue(())
                });
                assert!(matches!(
                    result,
                    dope_core::driver::result::poll::Dispatch::Ready { retained: None, .. }
                ));
            }
            Descriptor::from(socket).close(&mut driver);
        },
        recovery::Policy::new(|report| reports.push(report), common::recover),
        |result| {
            result
                .as_ref()
                .map_err(dope_test::scope::Error::cause)
                .expect("native scope");
        },
    );
    assert!(reports.events().expect("retirement").is_empty());
}

fn stopped(
    config: Config,
    run: impl for<'scope, 'life, 'p, 'd> FnOnce(
        session::Scope<'scope, 'life, 'p, 'd, (), shutdown::Source>,
    ),
) {
    let mut create = Attempt::new();
    let pair = (shutdown::Pair::new()).expect("shutdown pair");
    let (source, trigger) = pair.split();
    assert!(trigger.fire().is_ok(), "fire shutdown");
    let executor = Executor::new(&mut create, config);
    assert!(executor.is_ok(), "executor");
    let Some(executor) = executor.ok() else {
        return;
    };
    let executor = executor.shutdown(source);
    assert!(executor.is_ok(), "register shutdown");
    let Some(executor) = executor.ok() else {
        return;
    };
    let mut reports = Vec::new();

    assert!(
        executor.run(|executor| executor
            .enter_with(
                run,
                recovery::Policy::new(|event| reports.push(event), common::recover)
            )
            .is_ok()),
        "runtime turn"
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
