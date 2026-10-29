use std::{cell::Cell, convert::Infallible, pin, ptr, time::Duration};

use dope_core::{
    Errno,
    driver::{
        life::{creation::Attempt, recovery},
        schedule,
    },
};
use dope_runtime::{
    executor::{
        Executor, InstallError, drive,
        session::{Completion, Error, Kind},
    },
    shutdown, topology,
    topology::lending,
};

pub mod common;

use crate::common::runtime::{
    executor::disarm::{Child, Observer, Output},
    topology::{PROFILE_CONFIG, Profile},
};

#[test]
fn install_retains_disarm() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let executor = Executor::new(&mut create, PROFILE_CONFIG);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    let events = Cell::new(0);
                    let cause = Errno::try_from(libc::EPERM).expect("permission errno");
                    let mut called = false;
                    let app = Observer::<{ schedule::LIMIT + 1 }> {
                        events: &events,
                        stalled: false,
                        cause,
                    };
                    let result = session.with_app(&mut finish, app, |_| called = true);
                    assert!(
                        matches!(&result, Err(Error::Install {
                            cause: InstallError::Coordinators { count, limit },
                            disarm: Some(error),
                        }) if count.get() == schedule::LIMIT + 1
                            && *limit == schedule::LIMIT && *error == cause),
                        "{result:?}"
                    );
                    assert!(!called);
                    assert_eq!(events.get(), 0x1234);
                },
                recovery::Policy::new(|_| {}, recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
}

#[test]
fn quiesce_disarm_output() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let executor = Executor::new(&mut create, PROFILE_CONFIG);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    let events = Cell::new(0);
                    let out = Cell::new(false);
                    let cause = Errno::try_from(libc::EPERM).expect("permission errno");
                    let app = Observer::<0> {
                        events: &events,
                        stalled: true,
                        cause,
                    };
                    {
                        let error = session
                            .with_quiesce(&mut finish, app, Duration::ZERO, |_| {
                                Err::<(), _>(Output(&out))
                            })
                            .err()
                            .expect("retained output and cleanup failures");
                        let kind = error
                            .kind()
                            .map(|output| matches!(output, Err(output) if ptr::eq(output.0, &out)));
                        assert!(
                            matches!(&kind, Kind::Quiesce {
                                output: true,
                                drive: drive::Error::QuiescenceTimeout,
                                disarm: Some(error),
                            } if *error == cause),
                            "{kind:?}"
                        );
                        assert_eq!(events.get(), 0x1234);
                        assert!(!out.get(), "callback output remains owned by its caller");
                    }
                    assert!(out.get());
                },
                recovery::Policy::new(|_| {}, recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
}

#[test]
fn lending_disarm_drop() {
    let events = Cell::new(0);
    let cause = Errno::try_from(libc::EPERM).expect("permission errno");
    let mut complete = Completion::new();
    let prepared = topology::Plan::new((), ())
        .for_install::<Profile>()
        .expect("observer composition");
    let (source, trigger) = shutdown::Pair::new().expect("shutdown source").split();
    trigger.fire().expect("request shutdown");
    prepared.enter_with(
        source,
        |worker: topology::Worker<'_, '_, '_, '_, _, _, _>| {
            worker.lend(|_, stage| {
                let owner = Observer::<0> {
                    events: &events,
                    stalled: false,
                    cause,
                };
                stage.hold(owner, |held, stage| {
                    let result = held.lend_scope(stage, &mut complete, |issued| {
                        let (client, permit) = issued.into_parts();
                        let state = pin::pin!(client);
                        permit.execute::<_, Infallible>(Child {
                            client: state.as_ref(),
                        })
                    });
                    let error = match result.err().expect("reported cleanup failure") {
                        lending::Error::Build(error) => match error {},
                        lending::Error::Run(error) => error,
                    };
                    let kind = error.kind().map(Result::is_ok);
                    assert!(
                        matches!(&kind, Kind::Quiesce {
                            output: true,
                            drive: drive::Error::TimerDisarm(error),
                            disarm: None,
                        } if *error == cause),
                        "{kind:?}"
                    );
                    assert_eq!(events.get(), 0x162534, "client precedes provider cleanup");
                });
            });
        },
        recovery::Policy::new(|_| {}, recover),
        |entered| {
            entered
                .as_ref()
                .map_err(|error| error.kind())
                .expect("lifecycle domain");
        },
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
