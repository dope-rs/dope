use std::convert::Infallible;

use common::{DATAGRAM, LOCALHOST, contract::reclaim::Canceled};
use dope_core::{
    driver,
    driver::life::{creation, recovery},
    io::{fd::handles::Descriptor, socket::install::Install},
};
use dope_test::scope::Error;

use crate::common::{
    driver::{Native as _, Open, Scoped as _, support::Targets as _},
    file::Inspect as _,
    wake::Ready as _,
};

pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use std::{env::consts, net::UdpSocket, ops::ControlFlow, time::Duration};

use dope_core::{
    driver::{
        life::domain,
        result::poll::{Commit, Dispatch},
        route::{Epoch, FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind, lease},
        schedule, settings,
    },
    io::event::{Kind, net::recv::Outcome},
};
use dope_test::{alloc::Tracker, reclaim::Reports};

use crate::common::{driver::support::ROUTE, rt::Runtime};

#[test]
fn reuses_odd_tail() {
    let config = Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE)
        .with_files(settings::FileSlots::fixed::<0, 3>());
    let mut creation = creation::Attempt::new();
    let driver = creation.try_driver(config);
    let driver = driver
        .map_err(|error| error.kind())
        .expect("driver creation");
    let mut reports = Reports::<_>::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    assert!(
        domain::Domain::new(driver)
            .start()
            .expect("fresh generation")
            .enter_with(
                (),
                |mut scope, _, _| {
                    scope.run_turn(|mut driver, turn| {
                        let address = LOCALHOST;
                        let first = Install::bind_udp(driver.reborrow(), address, DATAGRAM);
                        let (first, _) = first.expect("first socket");
                        let second = Install::bind_udp(driver.reborrow(), address, DATAGRAM);
                        let (second, _) = second.expect("second socket");
                        let last = Install::bind_udp(driver.reborrow(), address, DATAGRAM);
                        let (last, _) = last.expect("last socket");
                        let slot = last.index();
                        Descriptor::from(last).close(&mut driver);

                        let reused = driver.rebind(turn);
                        let (reused, _) = reused.ok().expect("reused socket");
                        assert!(reused.index() == slot);
                        Descriptor::from(first).close(&mut driver);
                        Descriptor::from(second).close(&mut driver);
                        Descriptor::from(reused).close(&mut driver);
                    });
                },
                recovery::Policy::new(|event| reports.push(event), recover)
            )
            .is_ok()
    );
    assert!(
        reports.events().expect("valid reclamation").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn stale_ready_is_inert() {
    let runtime = Runtime::quic::<2, 8>().file_slots(settings::FileSlots::fixed::<0, 1>());
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            scope.run_turn(|mut driver, turn| {
                let core = driver.driver_ref();
                let old = core.tuned_target::<KeyTag<ROUTE>>(SlotIndex::ZERO);
                let new = core.tuned_target::<KeyTag<ROUTE>>(SlotIndex::from(1_u16));

                let first = Install::bind_udp(driver.reborrow(), LOCALHOST, DATAGRAM);
                let (first, _) = first.expect("first socket");
                let physical = first.index();
                let stale = first.ready_handle();
                let (stale_to, alloc) = Tracker::usage(|| {
                    stale.set_target(old.dispatch());
                    stale.target()
                });
                assert_eq!(alloc, (0, 0));
                Descriptor::from(first).close(&mut driver);

                let second = driver.rebind(turn);
                let (second, _) = second.ok().expect("second socket");
                assert!(second.index() == physical);
                let current = second.ready_handle();
                let (active, alloc) = Tracker::usage(|| {
                    current.set_target(new.dispatch());
                    stale.set_target(old.dispatch());
                    stale_to.wake();
                    assert_eq!(turn.ready_round(|_| {}), 0);

                    current.activate();
                    let mut active = None;
                    assert_eq!(
                        turn.ready_round(|target| {
                            assert!(active.replace(target).is_none());
                        }),
                        1
                    );
                    active
                });
                assert_eq!(alloc, (0, 0));
                let active = (active).expect("current descriptor target");
                assert!(new.dispatch().matches(active));
                Descriptor::from(second).close(&mut driver);
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.events().expect("valid reclamation").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn drop_reclaims_slot() {
    let runtime = Runtime::quic::<2, 8>().file_slots(settings::FileSlots::fixed::<0, 1>());
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            scope.run_turn(|mut driver, turn| {
                let slot = {
                    let first = Install::bind_udp(driver.reborrow(), LOCALHOST, DATAGRAM);
                    let (first, _) = first.expect("first socket");
                    first.index()
                };
                assert!(
                    matches!(
                        driver::Context::commit(&mut driver, turn.reactor()),
                        Ok(Commit::Drained)
                    ),
                    "reclaim dropped descriptor"
                );

                let second = driver.rebind(turn);
                let (second, _) = second.ok().expect("second socket");
                assert!(second.index() == slot);
                Descriptor::from(second).close(&mut driver);
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.events().expect("valid reclamation").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
#[ignore = "requires macOS and exclusive process descriptor accounting"]
fn close_before_fd_reuse() {
    assert_eq!(consts::OS, "macos", "requires kqueue");
    let runtime = Runtime::quic::<2, 8>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| scope.with_driver(|mut driver, mut schedule| {
            let before = Open.collect(0..4096);
            let old = Install::bind_udp(driver.reborrow(), LOCALHOST, DATAGRAM);
            let (old, old_addr) = old.expect("old socket");
            let old_slot = old.index();
            let opened = Open
                .collect(0..4096)
                .into_iter()
                .filter(|fd| !before.contains(fd))
                .collect::<Vec<_>>();
            assert_eq!(opened.len(), 1);
            let reused = opened[0];
            let old_key = old
                .driver()
                .targets::<KeyTag<1>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL);
            driver.submit_recv(&old, old_key).expect("receive fixture");

            let peer = (UdpSocket::bind("127.0.0.1:0")).expect("datagram peer");
            assert!(peer.send_to(&[0x41; 32], old_addr).is_ok(), "send old");
            assert!(
                driver::Context::wait(
                    &mut driver,
                    schedule.begin().reactor(),
                    Some(Duration::from_secs(1)),
                )
                .is_ok(),
                "receive old"
            );

            Descriptor::from(old).close(&mut driver);
            assert!(!Open.contains(reused));

            let new = Install::bind_udp(driver.reborrow(), LOCALHOST, DATAGRAM);
            let (new, new_addr) = new.expect("new socket");
            assert!(new.index() == old_slot);
            assert!(Open.contains(reused));
            let new_key = new
                .driver()
                .targets::<KeyTag<2>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL);
            driver.submit_recv(&new, new_key).expect("receive fixture");
            assert!(peer.send_to(b"new", new_addr).is_ok(), "send new");
            let started = std::time::Instant::now();
            let mut done = None;
            while done.is_none() {
                let mut turn = schedule.begin();
                let left = Duration::from_secs(1)
                    .checked_sub(started.elapsed())
                    .expect("receive deadline");
                assert!(!left.is_zero(), "receive deadline");
                assert!(matches!(
                    driver::Context::dispatch(&mut driver, turn.reactor(), |event, _source| {
                        assert!(done.replace(event).is_none());
                        ControlFlow::Continue(())
                    }),
                    Dispatch::Ready { retained: None, .. }
                ));
                if done.is_none() {
                    assert!(
                        driver::Context::wait(&mut driver, turn.reactor(), Some(left)).is_ok(),
                        "receive new"
                    );
                }
            }
            let done = (done).expect("completion");
            let done = match done.into_kind() {
                Kind::Recv(done) => Some(done),
                _ => None,
            }
            .expect("receive completion");
            assert!(new_key.operation::<kind::Recv>().matches(done.token()));
            assert!(matches!(done.into_parts().2, Outcome::Data(buffer) if buffer.as_slice() == b"new"));

            Descriptor::from(new).close(&mut driver);
        }),
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        matches!(
            reports.events().expect("valid reclamation"),
            [dope_core::io::Reclaimed::Recv {
                result: Ok(32),
                flow: dope_core::io::event::batch::Flow::More,
                ..
            }]
        ),
        "exact old receive retirement: {reports:?}"
    );
}

#[test]
fn close_retires_generation() {
    assert_eq!(consts::OS, "linux", "requires io_uring");
    let runtime = Runtime::quic::<2, 8>().file_slots(settings::FileSlots::fixed::<0, 1>());
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_turn(
        |turn, mut driver| {
            let address = LOCALHOST;
            let old = Install::bind_udp(driver.driver().reborrow(), address, DATAGRAM);
            let (old, _) = old.expect("old datagram slot");
            let slot = old.index();
            Descriptor::from(old).close(driver.driver());

            let current = driver.driver().rebind(turn);
            let (current, addr2) = current.ok().expect("generation-reused datagram slot");
            assert!(current.index() == slot);

            let target = current
                .driver()
                .targets::<KeyTag<3>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL);
            driver
                .submit_recv(&current, target)
                .expect("receive fixture");
            let peer = (UdpSocket::bind(address)).expect("datagram peer");
            assert!(
                peer.send_to(b"current", addr2).is_ok(),
                "send to current generation"
            );
            assert!(
                driver::Context::wait(
                    driver.driver(),
                    turn.reactor(),
                    Some(Duration::from_secs(1)),
                )
                .is_ok(),
                "current generation receive"
            );

            let mut done = None;
            assert!(matches!(
                driver::Context::dispatch(driver.driver(), turn.reactor(), |event, _source| {
                    assert!(done.replace(event).is_none());
                    ControlFlow::Continue(())
                }),
                Dispatch::Ready { retained: None, .. }
            ));
            let done = (done).expect("current generation completion");
            let done = match done.into_kind() {
                Kind::Recv(done) => Some(done),
                _ => None,
            }
            .expect("receive completion");
            assert!(target.operation::<kind::Recv>().matches(done.token()));
            {
                let buffer = match done.into_parts().2 {
                    Outcome::Data(buffer) => Some(buffer),
                    _ => None,
                }
                .expect("current generation buffer");
                assert_eq!(buffer.as_slice(), b"current");
            }
            Descriptor::from(current).close(driver.driver());
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        matches!(reports.matches_one(&Canceled), Ok(true)),
        "exact cancellation: {reports:?}"
    );
}

#[test]
#[ignore = "requires macOS kqueue"]
fn kqueue_defers_work() {
    assert_eq!(consts::OS, "macos", "requires kqueue");
    const REGISTRATIONS: u16 = 200;
    let runtime = Runtime::throughput();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_turn(
        |turn, mut driver| {
            let mut sockets = Vec::with_capacity(REGISTRATIONS as usize);
            for index in 0..REGISTRATIONS {
                let socket = Install::bind_udp(driver.driver().reborrow(), LOCALHOST, DATAGRAM);
                let (socket, _) = socket.expect("bind datagram");
                let target = socket
                    .driver()
                    .targets::<KeyTag<1>>()
                    .bind(SlotIndex::from(index), Epoch::INITIAL);
                driver
                    .submit_recv(&socket, target)
                    .expect("receive fixture");
                sockets.push(socket);
            }

            assert!(
                matches!(
                    driver::Context::commit(driver.driver(), turn.reactor()),
                    Ok(Commit::Pending)
                ),
                "commit one bounded changelist batch"
            );
            assert!(
                matches!(
                    driver::Context::commit(driver.driver(), turn.reactor()),
                    Ok(Commit::Drained)
                ),
                "commit remaining changelist batch"
            );

            for socket in sockets {
                Descriptor::from(socket).close(driver.driver());
            }
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.events().expect("valid reclamation").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn uring_defers_maintenance() {
    assert_eq!(consts::OS, "linux", "requires io_uring");
    let runtime = Runtime::quic::<2, 8>().file_slots(settings::FileSlots::fixed::<
        0,
        { schedule::LIMIT as u32 + 1 },
    >());
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            scope.with_driver(|mut driver, mut ctl| {
                let address = LOCALHOST;
                {
                    let sockets = (0..=schedule::LIMIT)
                        .map(|_| {
                            Install::bind_udp(driver.reborrow(), address, DATAGRAM)
                                .map(|(socket, _)| socket)
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .expect("datagram slots");
                    assert_eq!(sockets.len(), schedule::LIMIT + 1);
                }
                drop(
                    lease::Owner::<KeyTag<17>>::try_reserve(&mut driver)
                        .expect("deferred route")
                        .into_claim(),
                );

                {
                    let mut turn = ctl.begin();
                    for _ in 0..=schedule::LIMIT {
                        if turn.is_exhausted() {
                            break;
                        }
                        assert!(
                            matches!(
                                driver::Context::commit(&mut driver, turn.reactor()),
                                Ok(Commit::Pending)
                            ),
                            "defer maintenance beyond its reactor share"
                        );
                    }
                    assert!(turn.is_exhausted());
                    drop(
                        lease::Owner::<KeyTag<17>>::try_reserve(&mut driver)
                            .expect("route retired beside descriptors")
                            .into_claim(),
                    );
                    assert!(matches!(
                        driver::Context::commit(&mut driver, turn.reactor()),
                        Ok(Commit::Pending)
                    ));
                    assert!(matches!(
                        lease::Owner::<KeyTag<17>>::try_reserve(&mut driver),
                        Err(lease::Error::Occupied)
                    ));
                }

                {
                    let mut turn = ctl.begin();
                    assert!(
                        matches!(
                            driver::Context::commit(&mut driver, turn.reactor()),
                            Ok(Commit::Drained)
                        ),
                        "commit remaining maintenance"
                    );
                    assert!(lease::Owner::<KeyTag<17>>::try_reserve(&mut driver).is_ok());
                }
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.events().expect("valid reclamation").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}

fn finish(result: Result<(), Error<'_, ()>>) {
    result
        .as_ref()
        .map_err(Error::cause)
        .expect("driver lifecycle");
}
