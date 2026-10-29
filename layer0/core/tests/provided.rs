use std::convert::Infallible;

use dope_core::{
    driver,
    driver::life::{creation, recovery},
    io::{fd::handles::Descriptor, socket::install::Install},
};

use crate::common::contract::provided::Joined;

pub mod common;

use std::{cell, mem, net, num, ops, time};

use dope_core::{
    driver::{
        life::domain::Domain, result::poll, retained::flight, route, route::indices::SlotIndex,
        storage,
    },
    io::{
        Reclaimed, event,
        event::{batch::Flow, net::recv::Outcome},
    },
};
use o3::{
    alloc,
    buffer::{Residency as _, storage::provided},
};

use crate::common::rt::Runtime;

#[test]
fn joined_arenas() {
    let mut reports = Vec::new();
    let drops = cell::Cell::new(0);
    let first =
        provided::Arena::<_, _>::try_with(num::NonZero::<u32>::MIN, || Ok::<_, alloc::Error>(()))
            .expect("first arena");
    let second =
        provided::Arena::<_, _>::try_with(num::NonZero::<u32>::MIN, || Ok::<_, alloc::Error>(()))
            .expect("second arena");
    let mut creation = creation::Attempt::new();
    let driver = creation.try_driver(Runtime::quic::<2, 8>().config());
    let driver = driver
        .map_err(|error| error.kind())
        .expect("driver creation");
    let mut domain = Domain::new(driver);
    let result = domain.start().expect("fresh domain").scope(
        storage::Scope::new((first, second)),
        Joined(&drops),
        recovery::Policy::new(|event| reports.push(event), recover),
    );
    assert!(
        matches!(result, Ok(Ok((8, 0)))),
        "arenas reclaim before driver teardown"
    );
    assert_eq!(drops.get(), 2);

    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn forgotten_account() {
    let config = Runtime::quic::<2, 8>().config();
    let mut creation = creation::Attempt::new();
    let driver = creation.try_driver(config);
    let driver = driver
        .map_err(|error| error.kind())
        .expect("native driver creation");
    let mut reports = Vec::new();

    let mut domain = Domain::new(driver);
    let result = domain.start().expect("fresh generation").enter_with(
        (),
        |mut scope, _, _| {
            scope.with_driver(|mut context, mut schedule| {
                let mut turn = schedule.begin();
                let receiver = Install::bind_udp(
                    context.reborrow(),
                    ([127, 0, 0, 1], 0).into(),
                    common::DATAGRAM,
                );
                let (socket, address) = receiver.expect("bind native receiver");
                let target = socket
                    .driver()
                    .targets::<route::KeyTag<1>>()
                    .bind(SlotIndex::ZERO, route::Epoch::INITIAL);
                let need = flight::Need::<route::KeyTag<1>>::new(flight::Capacity::fixed::<1>());
                let slots = (context.storage.reborrow().try_flights(need))
                    .expect("receive flight capacity");
                let slots = slots.slots();
                let flight = (slots.recv(context.reborrow(), &socket, target))
                    .expect("native receive submission");
                drop(flight);
                let peer = (net::UdpSocket::bind("127.0.0.1:0")).expect("bind datagram peer");
                assert_eq!(
                    peer.send_to(b"retained", address).expect("send datagram"),
                    8
                );
                let result = driver::Context::wait(
                    &mut context,
                    turn.reactor(),
                    Some(time::Duration::from_secs(1)),
                );
                assert!(matches!(result, Ok(())), "native receive wait");
                let alloc = context.storage.allocator();
                let mut account = None;
                let mut count = 0;
                let outcome =
                    driver::Context::dispatch(&mut context, turn.reactor(), |event, _source| {
                        let recv = match event.into_kind() {
                            event::Kind::Recv(recv) => Some(recv),
                            _ => None,
                        }
                        .expect("expected receive completion");
                        let (_, _, outcome) = recv.into_parts();
                        let owner = match outcome {
                            Outcome::Data(owner) => Some(owner),
                            _ => None,
                        }
                        .expect("expected selected buffer");
                        assert_eq!(owner.as_slice(), b"retained");
                        let shared = owner.into_shared();
                        let resident = shared.resident();
                        assert!(resident >= shared.len().into_raw());
                        let budget = alloc.try_budget(resident).expect("receive budget");
                        let held = shared
                            .try_get(1..7, &budget)
                            .expect("account selected buffer");
                        let alias = (held.try_clone()).expect("accounted alias");
                        assert_eq!(held.resident(), resident);
                        assert_eq!(budget.resident(), resident);
                        mem::forget(alias);
                        mem::forget(held);
                        mem::forget(shared);
                        assert!(account.replace((budget, resident)).is_none());
                        count += 1;
                        ops::ControlFlow::Continue(())
                    });
                assert!(matches!(
                    outcome,
                    poll::Dispatch::Ready {
                        drain: poll::Drain::Drained,
                        retained: None
                    }
                ));
                assert_eq!(count, 1);
                let (budget, resident) = account.expect("receive account must survive owner leaks");
                assert_eq!(budget.resident(), resident);
                Descriptor::from(socket).close(&mut context);
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
    );
    assert!(
        matches!(
            reports.as_slice(),
            [Reclaimed::Recv {
                result: Err(error), flow: Flow::Last, ..
            }] if *error == dope_core::Errno::CANCELED
        ),
        "the detached multishot receive must report terminal cancellation: {reports:?}"
    );

    assert!(
        result.is_ok(),
        "forgotten accounts must not prevent native finalization"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}
