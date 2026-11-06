use crate::common::rt::{finish, recover};

use common::contract::reclaim::Canceled;
use dope_core::{
    driver,
    driver::life::recovery,
    io::{
        fd::handles::Descriptor,
        socket::{Listener, install::Install},
    },
};

use crate::common::driver::Native as _;

pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use std::{
    net::{SocketAddr, TcpStream, UdpSocket},
    num,
    ops::ControlFlow,
    time::Duration,
};

use dope_core::{
    driver::{
        result::poll::{Dispatch, Drain},
        retained::flight,
        route::{Epoch, FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind},
        schedule::state::scope,
        settings,
        storage::Resident,
    },
    io::{
        event::{
            Kind,
            batch::Flow,
            net::{accept, recv::Outcome},
        },
        recv::Lease,
    },
};
use dope_test::{alloc::Tracker, reclaim::Reports};
use o3::{
    alloc, buffer,
    buffer::{Residency as _, hold, hold::scope::Input, resident},
};
use resident::copy;

use crate::common::rt::Runtime;

fn copy<'d, R: buffer::Cursor + hold::raw::Retain<'d>>(source: &R, budget: &resident::Budget<'d>) {
    let plan = Input::new(source, budget)
        .try_plan(1..4)
        .expect("received range")
        .bind(hold::Policy::compact());
    let charge = budget
        .try_charge(budget.available() - 2)
        .expect("reserve remaining credit");
    let result = plan.try_retain();
    let (plan, error) = result.expect_err("two bytes cannot admit three");
    assert!(
        matches!(error, hold::Error::Copy(copy::Error::Budget(error))
        if error.into_parts() == (num::NonZero::<usize>::MIN.saturating_add(2), 2))
    );
    assert_eq!(plan.len(), 3);
    drop(charge);

    let result = Tracker::fail_after(0, || plan.try_retain());
    let (plan, error) = result.expect_err("copy allocation fails");
    assert!(matches!(
        error,
        hold::Error::Copy(copy::Error::Allocation(alloc::Exhausted))
    ));
    assert_eq!(budget.resident(), 0);
    {
        let (held, activity) = Tracker::counts(|| plan.try_into());
        let held = held.expect("retry retains the original admitted range");
        assert_eq!(activity, [1, 0]);
        assert_eq!(held.as_slice(), b"ype");
        assert_eq!(budget.resident(), 3);
    }
    assert_eq!(budget.resident(), 0);
}

fn receive_payload<'d>(
    schedule: &mut scope::Scope<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
    peer: &UdpSocket,
    address: SocketAddr,
    payload: &[u8],
) -> Option<Lease<'d>> {
    assert!(peer.send_to(payload, address).is_ok(), "send datagram");
    let started = std::time::Instant::now();
    let mut buffer = None;
    while buffer.is_none() {
        let left = Duration::from_secs(1)
            .checked_sub(started.elapsed())
            .expect("receive deadline");
        assert!(!left.is_zero(), "receive deadline");
        let mut turn = schedule.begin();
        assert!(matches!(
            driver::Context::dispatch(driver, turn.reactor(), |event, _source| {
                let recv = match event.into_kind() {
                    Kind::Recv(done) => done,
                    event => return ControlFlow::Break(event.into()),
                };
                let (_, flow, event) = recv.into_parts();
                match event {
                    Outcome::Data(received) => {
                        assert_eq!(received.as_slice(), payload);
                        assert!(buffer.replace(received).is_none());
                    }
                    Outcome::Exhausted(_) => assert_eq!(flow, Flow::Last),
                    Outcome::Failed(error) => panic!("receive failed: {error:?}"),
                    event => panic!("unexpected receive: {:?}", std::mem::discriminant(&event)),
                }
                ControlFlow::Continue(())
            }),
            Dispatch::Ready { retained: None, .. }
        ));
        if buffer.is_none() {
            assert!(
                driver::Context::wait(driver, turn.reactor(), Some(left)).is_ok(),
                "receive datagram"
            );
        }
    }
    buffer
}

#[test]
fn receive_binds_lease() {
    let runtime = Runtime::quic::<2, 8>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_turn(
        |turn, mut driver| {
            let socket = Install::bind_udp(
                driver.reborrow(),
                ([127, 0, 0, 1], 0).into(),
                common::DATAGRAM,
            );
            let (socket, address) = socket.expect("bind datagram");
            let target = socket
                .driver()
                .targets::<KeyTag<1>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL);
            driver
                .submit_recv(&socket, target)
                .expect("receive fixture");

            let peer = (UdpSocket::bind("127.0.0.1:0")).expect("datagram peer");
            assert!(peer.send_to(b"typed", address).is_ok(), "send");
            assert!(
                driver::Context::wait(&mut driver, turn.reactor(), Some(Duration::from_secs(1)),)
                    .is_ok(),
                "receive"
            );

            let mut event = None;
            assert!(matches!(
                driver::Context::dispatch(&mut driver, turn.reactor(), |completion, _source| {
                    assert!(event.replace(completion).is_none());
                    ControlFlow::Continue(())
                }),
                Dispatch::Ready {
                    drain: Drain::Drained,
                    retained: None
                }
            ));
            let event = (event).expect("completion");
            let observed = event.into_kind();
            assert!(matches!(&observed, Kind::Recv(_)), "receive completion");
            if let Kind::Recv(recv) = observed {
                let (_, _, outcome) = recv.into_parts();
                let observed = outcome;
                assert!(matches!(&observed, Outcome::Data(_)), "receive buffer");
                if let Outcome::Data(buffer) = observed {
                    assert_eq!(buffer.as_slice(), b"typed");
                    let budget = driver
                        .storage
                        .budget(Resident::bytes(3))
                        .expect("copy budget");
                    copy(&buffer, &budget);
                    {
                        let shared = buffer.into_shared();
                        copy(&shared, &budget);
                        assert_eq!(shared.as_slice(), b"typed");
                    }
                    Descriptor::from(socket).close(&mut driver);
                }
            }
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    reclaimed(&reports);
}

/// Releasing the final retained alias recycles its buffer while the second buffer remains live.
#[test]
fn account_recycles_owner() {
    let runtime = Runtime::quic::<2, 8>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            let (_, mut driver, mut schedule) = scope.split();
            let sock1 = Install::bind_udp(
                driver.reborrow(),
                ([127, 0, 0, 1], 0).into(),
                common::DATAGRAM,
            );
            let (sock1, addr1) = sock1.expect("bind datagram");
            let target1 = sock1
                .driver()
                .targets::<KeyTag<1>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL);
            let peer = (UdpSocket::bind("127.0.0.1:0")).expect("datagram peer");
            driver
                .submit_recv(&sock1, target1)
                .expect("receive fixture");

            let first = (receive_payload(&mut schedule, &mut driver, &peer, addr1, b"shared"))
                .expect("received payload");
            let ptr = first.as_slice().as_ptr();
            let shared = first.into_shared();
            let bytes = shared.resident();
            let budget =
                (driver.storage.budget(Resident::bytes(bytes))).expect("resident budget domain");
            let held = {
                let (retained, held) = {
                    let shared = shared;
                    let retained =
                        (shared.try_get(1..6, &budget)).expect("accounted receive owner");
                    let held = (retained.try_clone()).expect("retained edge must fit");
                    let held = held.project(1..4);
                    let held = held.expect("retained alias");
                    assert_eq!(held.as_ref(), b"are");
                    assert_eq!(budget.resident(), bytes);
                    (retained, held)
                };
                assert_eq!(retained.as_ref(), b"hared");
                held
            };
            assert_eq!(budget.resident(), bytes);

            let (sock2, sock3) = {
                let (sock2, second) = {
                    let held = held;
                    let sock2 = Install::bind_udp(
                        driver.reborrow(),
                        ([127, 0, 0, 1], 0).into(),
                        common::DATAGRAM,
                    );
                    let (sock2, addr2) = sock2.expect("bind second datagram");
                    let target2 = sock2
                        .driver()
                        .targets::<KeyTag<1>>()
                        .bind(SlotIndex::from(1_u16), Epoch::INITIAL);
                    driver
                        .submit_recv(&sock2, target2)
                        .expect("receive fixture");
                    let second =
                        (receive_payload(&mut schedule, &mut driver, &peer, addr2, b"second"))
                            .expect("second payload");
                    let second = second.into_shared();
                    assert_eq!(held.as_ref(), b"are");
                    (sock2, second)
                };
                assert_eq!(budget.resident(), 0);
                assert!(
                    driver::Context::commit(&mut driver, schedule.begin().reactor()).is_ok(),
                    "reclaim terminal receive buffer"
                );

                let sock3 = Install::bind_udp(
                    driver.reborrow(),
                    ([127, 0, 0, 1], 0).into(),
                    common::DATAGRAM,
                );
                let (sock3, addr3) = sock3.expect("bind third datagram");
                let target3 = sock3
                    .driver()
                    .targets::<KeyTag<1>>()
                    .bind(SlotIndex::from(2_u16), Epoch::INITIAL);
                driver
                    .submit_recv(&sock3, target3)
                    .expect("receive fixture");
                {
                    let third =
                        (receive_payload(&mut schedule, &mut driver, &peer, addr3, b"third"))
                            .expect("received payload");
                    assert_eq!(third.as_slice(), b"third");
                    assert_eq!(
                        third.as_slice().as_ptr(),
                        ptr,
                        "the third receive must reuse the exact terminally returned buffer"
                    );
                }
                assert_eq!(second.as_slice(), b"second");
                (sock2, sock3)
            };
            Descriptor::from(sock1).close(&mut driver);
            Descriptor::from(sock2).close(&mut driver);
            Descriptor::from(sock3).close(&mut driver);
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    reclaimed(&reports);
}

#[test]
fn retained_receive_buffers_allow_accept() {
    let runtime = Runtime::quic::<2, 8>().file_slots(settings::FileSlots::fixed::<1, 2>());
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    runtime.with_scope(
        |scope| {
            let (_, mut driver, mut schedule) = scope.split();
            let (socket, address) =
                Install::bind_udp(driver.reborrow(), common::LOCALHOST, common::DATAGRAM)
                    .expect("bind datagram");
            let target = driver
                .driver_ref()
                .targets::<KeyTag<1>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL);
            driver.submit_recv(&socket, target).expect("receive");
            let peer = UdpSocket::bind(common::LOCALHOST).expect("datagram peer");
            let held = [
                receive_payload(&mut schedule, &mut driver, &peer, address, b"first")
                    .expect("first buffer"),
                receive_payload(&mut schedule, &mut driver, &peer, address, b"second")
                    .expect("second buffer"),
            ];

            let listener = Listener::bind(common::LOCALHOST).expect("listener");
            let (listener, address) =
                Install::adopt_bound(driver.reborrow(), listener).expect("install listener");
            let target = driver
                .driver_ref()
                .targets::<KeyTag<2, kind::Accept>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL);
            let flights = driver
                .storage
                .reborrow()
                .try_flights(flight::Need::<KeyTag<2, kind::Accept>>::new(
                    flight::Capacity::fixed::<1>(),
                ))
                .expect("accept storage")
                .slots();
            let accept = flights
                .accept(
                    driver.reborrow(),
                    &listener,
                    target.operation::<kind::Accept>(),
                )
                .expect("accept");
            let peer = TcpStream::connect(address).expect("connect peer");
            let mut accepted = None;
            for _ in 0..16 {
                let mut turn = schedule.begin();
                let result = driver.dispatch(turn.reactor(), |event, _| {
                    match event.into_kind() {
                        Kind::Accept(done) => match done.into_parts().2 {
                            accept::Outcome::Accepted(slot) => {
                                accepted = Some(slot.bind());
                            }
                            accept::Outcome::Failed(error) => panic!("accept: {error:?}"),
                        },
                        Kind::Recv(done) => {
                            let (_, flow, outcome) = done.into_parts();
                            assert!(matches!(outcome, Outcome::Exhausted(_)));
                            assert_eq!(flow, Flow::Last);
                        }
                        event => {
                            panic!("unexpected event: {:?}", std::mem::discriminant(&event))
                        }
                    }
                    ControlFlow::Continue(())
                });
                assert!(matches!(result, Dispatch::Ready { retained: None, .. }));
                if accepted.is_some() {
                    break;
                }
                driver
                    .wait(turn.reactor(), Some(Duration::from_millis(50)))
                    .expect("accept progress");
            }
            accepted
                .expect("accept with held receive buffers")
                .close(&mut driver);
            drop(held);
            drop(accept);
            listener.close(&mut driver);
            Descriptor::from(socket).close(&mut driver);
            drop(peer);
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    for event in reports.events().expect("valid reclamation") {
        assert!(matches!(event,
            dope_core::io::Reclaimed::Recv { result: Err(error), flow: Flow::Last, .. }
            | dope_core::io::Reclaimed::Accept {
                result: Err(accept::Failure::Syscall(error)), flow: Flow::Last, ..
            } if *error == dope_core::Errno::CANCELED
        ));
    }
}

#[test]
fn dispatch_retains_blocker() {
    let runtime = Runtime::quic::<2, 8>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_turn(
        |turn, mut driver| {
            let socket = Install::bind_udp(
                driver.reborrow(),
                ([127, 0, 0, 1], 0).into(),
                common::DATAGRAM,
            );
            let (socket, address) = socket.expect("bind datagram");
            let spare = Install::bind_udp(
                driver.reborrow(),
                ([127, 0, 0, 1], 0).into(),
                common::DATAGRAM,
            );
            let (spare, _) = spare.expect("bind spare datagram");
            let target = socket
                .driver()
                .targets::<KeyTag<1>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL);
            driver
                .submit_recv(&socket, target)
                .expect("receive fixture");

            let peer = (UdpSocket::bind("127.0.0.1:0")).expect("datagram peer");
            assert!(peer.send_to(b"direct", address).is_ok(), "send");
            assert!(
                driver::Context::wait(&mut driver, turn.reactor(), Some(Duration::from_secs(1)),)
                    .is_ok(),
                "receive"
            );

            let mut calls = 0;
            let mut spare = Some(spare);
            let outcome =
                driver::Context::dispatch(&mut driver, turn.reactor(), |event, driver| {
                    calls += 1;
                    let spare = (spare.take()).expect("single callback");
                    Descriptor::from(spare).close(driver);
                    ControlFlow::Break(event)
                });
            let observed = outcome;
            assert!(
                matches!(
                    &observed,
                    Dispatch::Ready {
                        drain: Drain::Pending,
                        retained: _,
                    }
                ),
                "dispatch retains its blocking event"
            );
            if let Dispatch::Ready {
                drain: Drain::Pending,
                retained,
            } = observed
            {
                assert_eq!(calls, 1);
                assert!(spare.is_none());

                let event = (retained).expect("blocking event");
                let observed = event.into_kind();
                assert!(matches!(&observed, Kind::Recv(_)), "receive completion");
                if let Kind::Recv(recv) = observed {
                    let (_, _, outcome) = recv.into_parts();
                    let observed = outcome;
                    assert!(matches!(&observed, Outcome::Data(_)), "receive buffer");
                    if let Outcome::Data(buffer) = observed {
                        {
                            let buffer = buffer;
                            assert_eq!(buffer.as_slice(), b"direct");
                        }
                        Descriptor::from(socket).close(&mut driver);
                    }
                }
            }
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    reclaimed(&reports);
}

fn reclaimed(reports: &Reports<1>) {
    assert!(
        matches!(reports.events(), Ok([])) || matches!(reports.matches_one(&Canceled), Ok(true)),
        "unexpected reclamation: {reports:?}"
    );
}
