use std::convert::Infallible;

use dope_core::{
    driver::life::{creation::Attempt, recovery},
    io::{
        fd::handles::Descriptor,
        socket::{self, install::Install},
    },
};
use dope_runtime::executor::session::Completion;
use dope_test::harness::scenario::TIMEOUT;
pub mod common;

use std::{cell::Cell, net::UdpSocket, rc::Rc};

use dope_core::{
    driver::{
        retained::flight,
        route::{Epoch, KeyTag, indices::SlotIndex},
        settings::{
            self, Config,
            demands::{Schedule, slots::Slots},
        },
    },
    io::{self},
};
use dope_runtime::{
    executor::{Executor, session},
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
    let mut reports = Vec::new();

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
                        first: Missing { app: None },
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
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(
            matches!(
                reports.as_slice(),
                [io::Reclaimed::Recv {
                    result: Err(error), flow: io::event::batch::Flow::Last, ..
                }] if *error == dope_core::Errno::CANCELED
            ),
            "the deferred receive must report terminal cancellation: {reports:?}"
        );

        assert!(result.is_ok(), "runtime completion dispatch");

        assert_eq!(calls, Some(2));
    });
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
                recovery::Policy::new(|event| reports.push(event), recover)
            )
            .is_ok()),
        "runtime turn"
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
