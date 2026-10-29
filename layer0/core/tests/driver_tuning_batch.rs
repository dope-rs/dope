use std::convert::Infallible;

use dope_core::{
    driver,
    driver::life::recovery,
    io::socket::{install::Install, option::Tuning},
};
use dope_test::scope::Error;

use crate::common::driver::{
    Scoped as _, polling,
    support::Targets as _,
    tuning::{Failure, Tuned as _},
};

pub mod common;

use std::{collections::HashMap, env::consts};

use dope_core::{
    driver::{
        result::{SubmitCause, poll::Commit},
        route::{FrameworkTag, KeyTag, Tag, kind},
        settings,
    },
    io::{
        fd::handles,
        socket::{option, pending},
    },
};
use dope_test::reclaim::Reports;

use crate::common::rt::Runtime;

#[test]
fn tracks_fixed_slots() {
    let setting = option::Buffer::try_from(4096_usize);
    assert!(setting.is_ok(), "socket buffer size");
    let setting = setting.expect("valid buffer size");
    assert_eq!(consts::OS, "linux", "requires io_uring");
    const SUBMISSIONS: usize = 193;
    let runtime = Runtime::quic::<2, 8>()
        .queue_layout(settings::QueueLayout::fixed::<64, 128>())
        .file_slots(settings::FileSlots::fixed::<0, 256>());
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<KeyTag<1>, SUBMISSIONS>(|mut driver, turn, flights| {
                    let options =
                        option::Set::new(Some(setting), None, None, None, None, None, None);

                    let mut sockets = Vec::with_capacity(SUBMISSIONS);
                    for _ in 0..SUBMISSIONS {
                        let socket = Install::bind_udp(
                            driver.reborrow(),
                            ([127, 0, 0, 1], 0).into(),
                            common::DATAGRAM,
                        );
                        assert!(socket.is_ok(), "bind datagram");
                        let (socket, _) = socket.expect("bound datagram socket");
                        sockets.push(socket);
                    }
                    let mut pending = HashMap::with_capacity(SUBMISSIONS);
                    let mut commits = 0;
                    for socket in sockets {
                        let mut socket: handles::Descriptor<'_> = socket.into();
                        let owner = driver
                            .driver_ref()
                            .tuned_target::<KeyTag<1>>(socket.index());
                        let target = owner.operation::<kind::Tuning>();
                        let wait = loop {
                            match Tuning::try_new(
                                &mut driver.reborrow(),
                                &flights,
                                owner,
                                socket,
                                options,
                            ) {
                                Ok(tuning) => {
                                    assert!(
                                        matches!(&tuning, option::Tuning::Pending(_)),
                                        "nonempty io_uring tuning is asynchronous"
                                    );
                                    match tuning {
                                        option::Tuning::Pending(pending) => break pending,
                                        option::Tuning::Applied(returned) => socket = returned,
                                    }
                                }
                                Err(rejected) => {
                                    let (returned, cause) = rejected.into_parts();
                                    socket = returned;
                                    let error = match cause {
                                        pending::Cause::Submit(error) => Ok(error),
                                        pending::Cause::Option(error) => Err(error),
                                    }
                                    .expect("setsockopt must succeed");
                                    assert_eq!(error.cause(), SubmitCause::Full);
                                    commits += 1;
                                    assert!(
                                        matches!(
                                            driver::Context::commit(&mut driver, turn.reactor()),
                                            Ok(Commit::Pending)
                                        ),
                                        "commit saturated tuning batch"
                                    );
                                }
                            }
                        };
                        assert!(pending.insert(target, wait).is_none());
                    }
                    assert_eq!(commits, 3);

                    assert!(
                        matches!(
                            driver::Context::commit(&mut driver, turn.reactor()),
                            Ok(Commit::Drained)
                        ),
                        "commit tuning submissions"
                    );
                    while !pending.is_empty() {
                        let done = driver
                            .wait_tuning(turn)
                            .map_err(|error| match error {
                                polling::Error::Timeout(timeout) => format!("timeout: {timeout:?}"),
                                polling::Error::Overflow(timeout) => {
                                    format!("overflow: {timeout:?}")
                                }
                                polling::Error::Source(Failure::Wait(error)) => {
                                    format!("wait: {error:?}")
                                }
                                polling::Error::Source(Failure::Dispatch(error)) => {
                                    format!("dispatch: {error:?}")
                                }
                                polling::Error::Source(Failure::Event(event)) => format!(
                                    "unexpected event: {:?}, route: {:?}",
                                    std::mem::discriminant(&event),
                                    dope_core::io::Event::from(event).route(),
                                ),
                            })
                            .expect("tuning completion");
                        let target = driver
                            .driver_ref()
                            .targets::<KeyTag<1>>()
                            .parse(done.token());
                        let target = target
                            .map_err(|error| error.into_target().route())
                            .expect("matching tuning route");
                        let wait = (pending.remove(&target))
                            .expect("completion addresses one live transaction");
                        let (socket, _) = wait
                            .complete(done)
                            .map_err(|error| {
                                let (wait, done) = error.into_parts();
                                let token = done.token();
                                (
                                    wait.fd().index(),
                                    token.route(),
                                    token.slot(),
                                    token.epoch(),
                                )
                            })
                            .expect("batch completion must match its authority");
                        socket.close(&mut driver);
                    }
                })
                .expect("flight fixture");
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
