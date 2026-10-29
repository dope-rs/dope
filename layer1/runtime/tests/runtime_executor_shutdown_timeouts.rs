use std::convert::Infallible;

use dope_core::driver::{
    life::{creation::Attempt, recovery},
    route::KeyTag,
};
use dope_runtime::executor::session::Completion;
use dope_test::harness::scenario::TIMEOUT;
pub mod common;

use std::{cell::Cell, rc::Rc, time};

use dope_runtime::executor::{
    drive,
    session::{Error, Kind},
};

use crate::common::{
    runtime::executor::shutdown::timeouts::*,
    scenario::{app::Empty, dispatch, rt::Configured},
};

#[test]
fn timeout_reuses_session() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let done = Rc::new(Cell::new(false));
    let gone = Rc::new(Cell::new(false));
    let out = Rc::new(Cell::new(false));
    let mut reports = Vec::new();

    let executor = Configured::quic::<2, 8>().try_executor(&mut create);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut sess| {
                    let clock = sess.driver().driver_ref().clock();
                    let start = clock.now();
                    {
                        {
                            let error = sess.with_quiesce(
                                &mut finish,
                                dispatch::Builder::new(Stall {
                                    done: Rc::clone(&done),
                                    gone: Rc::clone(&gone),
                                    out: Rc::clone(&out),
                                })
                                .probe::<KeyTag<0>>(),
                                time::Duration::ZERO,
                                |_| {
                                    assert!(clock.now() >= start);
                                    Err::<(), _>(Output(Rc::clone(&out)))
                                },
                            );
                            assert!(error.is_err(), "stalled shutdown must report its deadline");
                            if let Err(error) = error {
                                let kind = error.kind().map(|output| {
                                    matches!(output, Err(output) if Rc::ptr_eq(&output.0, &out))
                                });
                                assert!(matches!(
                                    kind,
                                    Kind::Quiesce {
                                        output: true,
                                        drive: drive::Error::QuiescenceTimeout,
                                        disarm: None,
                                    }
                                ));
                                assert!(!out.get());
                                let valid = match error {
                                    Error::Quiesce {
                                        output,
                                        drive,
                                        disarm: None,
                                    } => {
                                        let output = output.expect_err("callback failure retained");
                                        assert!(matches!(drive, drive::Error::QuiescenceTimeout));
                                        assert!(done.get() && gone.get());
                                        assert!(!out.get());
                                        assert!(Rc::ptr_eq(&output.0, &out));
                                        true
                                    }
                                    _ => false,
                                };
                                assert!(valid, "unexpected stalled application failure");
                            }
                        }
                    }
                    assert!(out.get());
                    assert!(done.get());
                    assert!(gone.get());

                    let reuse = clock.now();
                    assert!(
                        sess.with_quiesce(&mut finish, Empty {}, TIMEOUT, |_| {
                            assert!(clock.now() >= reuse);
                        })
                        .is_ok(),
                        "forced finalization must leave the session reusable"
                    );
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
