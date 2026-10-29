use std::convert::Infallible;

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
use dope_test::scope::Error;

use crate::common::driver::{Scoped as _, support::Targets as _, tuning::Tuned as _};

pub mod common;

use std::{env::consts, io::Write as _, os::unix::net::UnixStream, time::Duration};

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
    if consts::OS != "linux" {
        return;
    }
    let (source, mut notify) = (UnixStream::pair()).expect("shutdown wake pair");
    assert!(source.set_nonblocking(true).is_ok());
    assert!(notify.set_nonblocking(true).is_ok());
    let config = Runtime::quic::<2, 8>().config();
    let mut creation = creation::Attempt::new();
    let driver = creation.try_driver(config);
    let driver = driver
        .map_err(|error| error.kind())
        .expect("driver creation");
    let domain = domain::Domain::new(driver).fd(source);
    assert!(domain.is_ok(), "shutdown registration");
    let mut domain = domain.expect("registered shutdown source");
    let mut reports = Reports::<_>::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    assert!(
        domain
            .start()
            .expect("fresh generation")
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
                    {
                        let pending = Tuning::try_new(
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
                        assert!(matches!(&pending, option::Tuning::Pending(_)));
                    }

                    let first = Install::bind_udp(
                        driver.reborrow(),
                        ([127, 0, 0, 1], 0).into(),
                        common::DATAGRAM,
                    );
                    assert!(matches!(first, Err(install::Error::Slot(_))));
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
                    assert_eq!(retired, 1, "the old tuning terminal precedes slot reuse");
                    assert_eq!(socket.index(), index);
                    let pending =
                        Tuning::try_new(&mut driver.reborrow(), &flights, owner, socket, options)
                            .map_err(|error| {
                                let (socket, cause) = error.into_parts();
                                (socket.index(), cause)
                            })
                            .expect("fresh tuning submission");
                    assert!(matches!(&pending, option::Tuning::Pending(_)));
                    if let option::Tuning::Pending(pending) = pending {
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
                        socket.close(&mut driver);
                    }
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

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}

fn finish(result: Result<(), Error<'_, ()>>) {
    result
        .as_ref()
        .map_err(Error::cause)
        .expect("driver lifecycle");
}

const _: fn() = || {
    use common::contract;
    use dope_core::{
        driver::route::{Erased, kind},
        io::event::net::tuning::Completion,
    };

    contract::not_from::<
        Completion<'static>,
        (Erased<'static, kind::Tuning>, Result<(), dope_core::Errno>),
        _,
    >();
};
