use crate::common::rt::{finish, recover};

use dope_core::{
    driver,
    driver::{
        life::{creation, recovery},
        route::KeyTag,
    },
    io::{
        fd::handles::Descriptor,
        socket::{install::Install, option::Tuning},
    },
};

use crate::common::driver::{Scoped as _, support::Targets as _, tuning::Tuned as _};

pub mod common;

use std::{io::Write as _, os::unix::net::UnixStream, time::Duration};

use dope_core::{
    driver::{
        life::domain,
        result::poll::{Dispatch, Drain},
        route::{FrameworkTag, Tag, kind},
    },
    io::event,
};
use dope_test::reclaim::Reports;

use crate::common::rt::Runtime;

#[test]
fn keeps_unrelated_event() {
    let (source, mut notify) = (UnixStream::pair()).expect("shutdown wake pair");
    assert!(source.set_nonblocking(true).is_ok());
    assert!(notify.set_nonblocking(true).is_ok());
    let config = Runtime::quic::<2, 8>().config();
    let mut creation = creation::Attempt::new();
    let driver = creation.try_driver(config);
    let driver = driver
        .map_err(|error| error.kind())
        .expect("driver creation");
    let mut teardown = domain::Teardown::default();
    let generation = domain::Generation::new(driver)
        .fd(source)
        .expect("registered shutdown source");
    let mut reports = Reports::<_>::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    assert!(
        generation
            .start(&mut teardown)
            .enter_with(
                (),
                |mut scope, _storage, _source| {
                    scope.run_turn(|mut driver, turn| {
                        assert!(notify.write_all(&[1]).is_ok());
                        assert!(
                            driver::Context::wait(
                                &mut driver,
                                turn.reactor(),
                                Some(Duration::from_secs(1))
                            )
                            .is_ok()
                        );

                        let socket = Install::bind_udp(
                            driver.reborrow(),
                            ([127, 0, 0, 1], 0).into(),
                            common::DATAGRAM,
                        );
                        assert!(socket.is_ok(), "datagram bootstrap");
                        let (socket, _) = socket.expect("bound datagram socket");

                        let mut done = None;
                        assert!(matches!(
                            driver::Context::dispatch(
                                &mut driver,
                                turn.reactor(),
                                |event, _source| {
                                    assert!(done.replace(event).is_none());
                                    std::ops::ControlFlow::Continue(())
                                }
                            ),
                            Dispatch::Ready {
                                drain: Drain::Drained,
                                retained: None
                            }
                        ));
                        assert!(matches!(
                            done.map(|done| done.into_kind()),
                            Some(event::Kind::Shutdown)
                        ));
                        Descriptor::from(socket).close(&mut driver);
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
fn dropped_tuning_reuses() {
    use std::{ops::ControlFlow, time::Instant};

    use dope_core::{
        driver::{route::indices::SlotIndex, settings::FileSlots},
        io,
        io::{
            fd::handles::Descriptor,
            socket::{install, option},
        },
    };

    let setting = option::Buffer::try_from(4096_usize).expect("socket buffer size");
    let options = option::Set::new(Some(setting), Some(setting), None, None, None, None, None);
    let runtime = Runtime::quic::<2, 8>().file_slots(FileSlots::fixed::<0, 1>());
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<KeyTag<1>, 1>(|mut driver, turn, flights| {
                    let (socket, _) = Install::bind_udp(
                        driver.reborrow(),
                        ([127, 0, 0, 1], 0).into(),
                        common::DATAGRAM,
                    )
                    .expect("first socket owns the sole slot");
                    let socket = Descriptor::from(socket);
                    let index = socket.index();
                    let owner = driver
                        .driver_ref()
                        .tuned_target::<KeyTag<1>>(SlotIndex::ZERO);
                    let expected = {
                        let tuning = Tuning::try_new(
                            &mut driver.reborrow(),
                            &flights,
                            owner,
                            socket,
                            options,
                        )
                        .map_err(|error| {
                            let (socket, cause) = error.into_parts();
                            (socket.index(), cause)
                        })
                        .expect("first tuning submission");
                        let terminals = usize::from(matches!(&tuning, Tuning::Pending(_)));
                        drop(tuning);
                        terminals
                    };

                    let first = Install::bind_udp(
                        driver.reborrow(),
                        ([127, 0, 0, 1], 0).into(),
                        common::DATAGRAM,
                    );
                    assert!(matches!(
                        first,
                        Err(install::Error::Slot(install::slot::Error::Retiring {
                            available: 0
                        }))
                    ));
                    let deadline = Instant::now() + Duration::from_secs(1);
                    let mut retired = 0;
                    let socket = loop {
                        let left = deadline
                            .checked_duration_since(Instant::now())
                            .expect("dropped tuning retirement deadline");
                        assert!(driver.wait(turn.reactor(), Some(left)).is_ok());
                        let done = driver.dispatch(turn.reactor(), |event, _| {
                            let done = match event.into_kind() {
                                event::Kind::Tuning(done) => Some(done),
                                _ => None,
                            }
                            .expect("dropped tuning completion");
                            assert!(owner.operation::<kind::Tuning>().matches(done.token()));
                            retired += 1;
                            ControlFlow::Continue(())
                        });
                        assert!(matches!(done, Dispatch::Ready { .. }));
                        match Install::bind_udp(
                            driver.reborrow(),
                            ([127, 0, 0, 1], 0).into(),
                            common::DATAGRAM,
                        ) {
                            Ok((socket, _)) => break Descriptor::from(socket),
                            Err(error) => assert!(matches!(error, install::Error::Slot(_))),
                        }
                    };
                    assert_eq!(
                        retired, expected,
                        "pending tuning settles before slot reuse"
                    );
                    assert_eq!(socket.index(), index);
                    let tuning =
                        Tuning::try_new(&mut driver.reborrow(), &flights, owner, socket, options)
                            .map_err(|error| {
                                let (socket, cause) = error.into_parts();
                                (socket.index(), cause)
                            })
                            .expect("fresh tuning submission");
                    let socket = match tuning {
                        Tuning::Applied(socket) => socket,
                        Tuning::Pending(pending) => {
                            use common::driver::{polling::Error, tuning::Failure};

                            let done = driver
                                .wait_tuning(turn)
                                .map_err(|error| match error {
                                    Error::Timeout(waited) => {
                                        format!("tuning timed out after {waited:?}")
                                    }
                                    Error::Overflow(duration) => {
                                        format!("tuning deadline overflow: {duration:?}")
                                    }
                                    Error::Source(Failure::Wait(error)) => {
                                        format!("tuning wait: {error:?}")
                                    }
                                    Error::Source(Failure::Dispatch(error)) => {
                                        format!("tuning dispatch: {error:?}")
                                    }
                                    Error::Source(Failure::Event(event)) => format!(
                                        "unexpected tuning event: {:?}, route: {:?}",
                                        std::mem::discriminant(&event),
                                        dope_core::io::Event::from(event).route()
                                    ),
                                })
                                .expect("fresh tuning");
                            let (socket, outcome) = pending
                                .complete(done)
                                .map_err(|error| {
                                    let (pending, done) = error.into_parts();
                                    let token = done.token();
                                    (
                                        pending.fd().index(),
                                        token.route(),
                                        token.slot(),
                                        token.epoch(),
                                    )
                                })
                                .expect("fresh completion matches the reused owner");
                            assert!(matches!(outcome, Ok(())));
                            socket
                        }
                    };
                    socket.close(&mut driver);
                })
                .expect("flight fixture");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    for event in reports.events().expect("valid reclamation") {
        assert!(matches!(event, io::Reclaimed::Shutdown));
    }
}
