use dope_core::io::{fd::outbound::Reserved, socket::option};
pub mod common;

use std::{convert::Infallible, net::SocketAddr, ptr, time::Instant};

use dope_core::{
    driver,
    driver::{
        life::recovery,
        result::poll::Commit,
        route::{KeyTag, Tag, table::conn},
        schedule::clock,
        settings,
    },
    io::Reclaimed,
};
use dope_net::{
    ConfigError, Resource,
    link::{
        egress::{self, reserve::demand},
        pool::{change, raw, recv::input},
    },
    tcp::Tcp,
    wire::{
        Identity,
        domain::{Local, Resident, Retained},
    },
};
use dope_test::{
    reclaim::Reports,
    scope::{self, Driver},
};
use o3::buffer::storage::Shared;

use crate::common::rt::throughput;

type Route = KeyTag<23>;

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("driver recovery: {:?}", failure.cause())
}

fn finish(result: Result<(), scope::Error<'_, ()>>) {
    result
        .as_ref()
        .map_err(scope::Error::cause)
        .expect("native lifecycle");
}

#[test]
/// Linux-only pending creation; kqueue creates sockets synchronously.
fn retries_creation_cancel() {
    use std::env::consts;

    use dope_core::{
        driver::{
            result::SubmitCause,
            route::indices::{Edge, SlotIndex},
        },
        io::socket,
    };
    use dope_net::link::oneshot::{self, Busy, Dispatch, Progress, Unsent};
    use o3::base::time::Positive;

    if consts::OS != "linux" {
        return;
    }
    let config = settings::Config::default()
        .with_queues(settings::QueueLayout::fixed::<64, 128>())
        .with_files(settings::FileSlots::fixed::<0, 129>());
    let base = Edge::from(conn::Capacity::fixed::<1>());
    let mut reports = Reports::new(config, [Route::ROUTE]);
    let mut count = 0;
    Driver::new(config).with_scope(
        |scope| {
            let mut driver = scope.context();
            let cap = conn::Capacity::fixed::<128>().into();
            let held = Reserved::<Route>::try_new(
                &driver,
                settings::FileSlots::fixed::<0, 129>().outbound(),
            )
            .expect("outbound reservation");
            let mut pool =
                oneshot::Pool::<Route, u16, socket::Inet, Shared>::prepare(base, cap, &mut driver)
                    .expect("oneshot pool");
            let window = Positive::from_secs::<1>();
            let until = driver.driver_ref().clock().now().clamp_add(window);
            for index in 0_u16..128 {
                let request = oneshot::Request::new(
                    socket::Inet::from_std(([127, 0, 0, 1], 32123).into()),
                    option::Set::EMPTY,
                    index,
                    Shared::try_copy(b"cancel").expect("cancel payload"),
                    until,
                    window,
                );
                let result = pool.try_send(&held, SlotIndex::from(index), request, &mut driver);
                if matches!(result, Dispatch::Sent) {
                    count += 1;
                    continue;
                }
                let error = match result {
                    Dispatch::Unsent(Unsent::Busy {
                        cause: Busy::Submit(error),
                        ..
                    }) => Some(error),
                    _ => None,
                }
                .expect("native submission queue full");
                assert_eq!(error.cause(), SubmitCause::Full);
                break;
            }
            assert!(count > 0 && count < 128, "submission queue saturated");
            assert!(matches!(
                pool.try_cancel(SlotIndex::ZERO, &mut driver),
                Ok(Progress::Complete {
                    authority: 0,
                    result: Err(oneshot::Error::Cancelled)
                })
            ));
            assert!(pool.has_retries());
            {
                // SAFETY: this scope retains the original pool through native quiescence.
                // Only creation and cancellation are submitted; no address or payload is lent.
                let mut io = unsafe { oneshot::raw::Io::new(&mut pool, &mut driver) };
                assert!(matches!(io.retry_one(), Err(error) if error.cause() == SubmitCause::Full));
            }
            assert!(pool.has_retries(), "full cancellation remains queued");
            let mut released = 0;
            scope.finish(true, |_, finish| pool.finish(finish, |_| released += 1));
            assert_eq!(
                released,
                count - 1,
                "cancelled authority returned exactly once"
            );
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    let events = reports.into_events().expect("reclaimed creations");
    assert_eq!(events.len(), count);
    for event in events {
        assert!(
            matches!(&event,
                Reclaimed::Socket {
                    slot,
                    result,
                    close: None,
                    ..
                } if slot.route() == Route::ROUTE
                    && base.offset(SlotIndex::from(*slot))
                        .is_some_and(|local| (local.index().get() as usize) < count)
                    && (result.is_ok()
                        || result.is_err_and(|error| {
                            error.step() == socket::CreationStep::Socket && error.errno().is_canceled()
                        }))
            ),
            "unexpected reclaimed operation: {event:?}"
        );
    }
}

#[test]
fn oneshot_option_failure() {
    use std::time::Duration;

    use dope_core::{
        Errno,
        driver::route::indices::{Edge, SlotIndex},
        io::{event::Kind, socket},
    };
    use dope_net::link::oneshot::{self, Dispatch, Progress};
    use o3::base::time::Positive;

    let interval = option::Period::try_from(Duration::from_secs(i32::MAX as u64))
        .expect("ABI-representable period");
    let options = option::Set::new(None, None, None, None, None, Some(interval), None);
    let config = settings::Config::default()
        .with_queues(settings::QueueLayout::fixed::<64, 128>())
        .with_files(settings::FileSlots::fixed::<0, 2>());
    let mut reports = Reports::new(config, [Route::ROUTE]);
    let mut observed = None;
    Driver::new(config).with_scope(
        |scope| {
            let (_held, mut pool) = {
                let mut driver = scope.context();
                let cap = settings::FileSlots::fixed::<0, 1>().outbound();
                let held = Reserved::<Route>::try_new(
                    &driver,
                    settings::FileSlots::fixed::<0, 2>().outbound(),
                )
                .expect("oneshot descriptor reservation");
                let mut pool = oneshot::Pool::<Route, (), socket::Inet, Shared>::prepare(
                    Edge::from(conn::Capacity::fixed::<1>()),
                    cap,
                    &mut driver,
                )
                .expect("oneshot storage");
                let window = Positive::from_secs::<5>();
                let until = driver.driver_ref().clock().now().clamp_add(window);
                let request = oneshot::Request::new(
                    socket::Inet::from_std(([127, 0, 0, 1], 0).into()),
                    options,
                    (),
                    Shared::try_copy(b"option").expect("payload"),
                    until,
                    window,
                );
                assert!(
                    matches!(
                        pool.try_send(&held, SlotIndex::ZERO, request, &mut driver),
                        Dispatch::Sent
                    ),
                    "oneshot operation must be admitted before testing option rejection"
                );
                (held, pool)
            };
            for _ in 0..2 {
                let event = common::discard::next(scope).expect("socket establishment");
                let progress = {
                    let mut driver = scope.context();
                    // SAFETY: the original pool remains installed through completion
                    // and the explicit quiescence below, retaining address and payload.
                    let mut io = unsafe { oneshot::raw::Io::new(&mut pool, &mut driver) };
                    match event.into_kind() {
                        Kind::Socket(done) => io.socket(done),
                        Kind::Connect(done) => io.connect(done).ok().expect("connect owner"),
                        _ => panic!("unexpected establishment event"),
                    }
                };
                if let Progress::Complete { result, .. } = progress {
                    observed = Some(result);
                    break;
                }
            }
            scope.finish(true, |_, finish| pool.finish(finish, |_| {}));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    let events = reports.events().expect("reclaimed operations");
    assert!(events.is_empty(), "unexpected reclamation: {events:?}");
    match observed.expect("terminal option rejection") {
        Err(oneshot::Error::Option(errno)) => assert_eq!(errno, Errno::INVALID_ARGUMENT),
        Err(oneshot::Error::Connect(errno)) => {
            panic!("option rejection classified as connect: {errno:?}");
        }
        _ => panic!("missing option rejection"),
    }
}

#[test]
fn reports_file_shortfall() {
    let mut reports = Vec::new();
    throughput().with_driver(|mut driver| {
            let cap = conn::Capacity::fixed::<1>();
            let result = change::try_prepare::<
                Route, Tcp, Identity, (), input::Borrowed, Shared, 32, _, _, _, _,
            >(
                Retained::ZERO,
                Resident::ZERO,
                demand::Fallible::prepare(egress::Config::DEFAULT, cap),
                Local::new(Identity),
                &mut driver,
                driver::Context::reborrow,
                |ready, driver| {
                    let rejected = raw::Prepared::try_reserve(ready, settings::FileSlots::fixed::<0, 1>().accept(), driver);
                    assert!(matches!(rejected,
                        Err(Resource::Config(ConfigError::PhysicalBelowLogical { logical, physical }))
                        if logical == cap.into() && physical == settings::FileSlots::fixed::<0, 1>().accept()
                    ));
                },
            );
            assert!(result.is_ok(), "pool storage preparation failed");
    }, recovery::Policy::new(|event| reports.push(event), recover), finish);
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn outbound_reclaims_slots() {
    let mut reports = Vec::new();
    throughput().with_scope(
        |scope| {
            let (_, mut driver, mut ctl) = scope.split();
            let mut turn = ctl.begin_at(clock::Sample::new(Instant::now()));
            let cap = conn::Capacity::fixed::<1>();
            let result = change::try_prepare::<
                Route,
                Tcp,
                Identity,
                (),
                input::Borrowed,
                Shared,
                32,
                _,
                _,
                _,
                _,
            >(
                Retained::ZERO,
                Resident::ZERO,
                demand::Fallible::prepare(egress::Config::DEFAULT, cap),
                Local::new(Identity),
                &mut driver,
                driver::Context::reborrow,
                |ready, driver| {
                    let reserved = raw::Prepared::try_reserve(ready, cap.into(), driver)
                        .expect("outbound reservation");
                    let admitted = reserved.try_flights(driver.storage.reborrow());
                    assert!(admitted.is_ok(), "pool flight preparation failed");
                },
            );
            assert!(result.is_ok(), "pool storage preparation failed");
            let commit = driver::Context::commit(&mut driver, turn.reactor());
            assert!(matches!(commit, Ok(Commit::Drained)));
            let files = Reserved::<Route>::try_new(&driver, cap.into());
            assert!(files.is_ok(), "outbound file reservation was not reclaimed");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn oneshot_rejects_outside() {
    let mut reports = Vec::new();
    use dope_core::{
        driver::route::indices::{Edge, SlotIndex},
        io::socket,
    };
    use dope_net::link::{
        oneshot,
        oneshot::{Dispatch, Progress, Unsent},
    };
    use o3::base::time::Positive;

    throughput().with_driver(
        |mut driver| {
            let cap = settings::FileSlots::fixed::<0, 1>().outbound();
            let held =
                Reserved::<Route>::try_new(&driver, cap).expect("oneshot descriptor reservation");
            let mut pool = oneshot::Pool::<Route, Box<u32>, socket::Inet, Shared>::prepare(
                Edge::from(conn::Capacity::fixed::<1>()),
                cap,
                &mut driver,
            )
            .expect("oneshot storage");
            let outside = SlotIndex::from(1_u16);
            let cause = cap.excludes(outside).expect("excluded local position");
            assert_eq!(pool.try_cancel(outside, &mut driver).err(), Some(cause));
            assert!(matches!(
                pool.try_cancel(SlotIndex::ZERO, &mut driver),
                Ok(Progress::Idle)
            ));
            let mut cursor = u32::MAX;
            assert!(pool.cancel_next(&mut cursor, &mut driver).is_none());
            assert_eq!(cursor, u32::MAX);

            let addr = SocketAddr::from(([127, 0, 0, 1], 32123));
            let payload = Shared::try_copy(b"original-request-payload").expect("payload");
            let bytes = payload.as_slice().as_ptr();
            let owner = Box::new(73);
            let original = ptr::from_ref(&*owner);
            let window = Positive::from_secs::<7>();
            let until = driver.driver_ref().clock().now().clamp_add(window);
            let request = oneshot::Request::new(
                socket::Inet::from_std(addr),
                option::Set::EMPTY,
                owner,
                payload,
                until,
                window,
            );
            let result = pool.try_send(&held, outside, request, &mut driver);
            let (request, actual) = match result {
                Dispatch::Unsent(Unsent::Outside { request, cause }) => Some((request, cause)),
                _ => None,
            }
            .expect("outside request must fail before submission");
            assert_eq!(actual, cause);
            let parts = request.into_parts();
            assert_eq!(ptr::from_ref(&*parts.authority), original);
            assert_eq!(*parts.authority, 73);
            assert_eq!(parts.payload.as_slice().as_ptr(), bytes);
            assert_eq!(parts.payload.as_slice(), b"original-request-payload");
            assert_eq!(
                socket::Addr::copy_from(&parts.address).into_std().ok(),
                Some(addr)
            );
            assert_eq!(parts.connect_deadline.into_raw(), until.into_raw());
            assert_eq!(parts.send_window, window);
            assert!(!pool.has_io());
            assert!(pool.earliest().is_none());
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
