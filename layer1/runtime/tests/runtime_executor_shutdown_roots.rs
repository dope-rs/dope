use std::convert::Infallible;

use dope_core::driver::{
    life::{creation::Attempt, recovery},
    route::KeyTag,
};
use dope_runtime::executor::session::Completion;
use dope_test::harness::scenario::TIMEOUT;
pub mod common;

use std::{cell::Cell, ops::ControlFlow, rc::Rc, task::Poll, time};

use dope_core::driver::schedule::clock;
use dope_runtime::{
    executor::{drain, drive},
    shutdown,
};
use o3::base::time::Positive;

use crate::common::{
    runtime::executor::shutdown::{Count, DrainProbe, roots::*},
    scenario::{dispatch, rt::Configured},
};

#[test]
fn manual_observes_drop() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let stops = Rc::new(Cell::new(Some(0)));
    let ends = Rc::new(Cell::new(Some(0)));
    let root = Rc::new(Cell::new(false));
    let ordered = Rc::new(Cell::new(true));
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
                    let start = (sess
                        .driver()
                        .driver_ref()
                        .clock()
                        .now()
                        .try_add(time::Duration::from_millis(1)))
                    .expect("manual start deadline");
                    assert!(
                        sess.with_at_for(
                            &mut finish,
                            dispatch::Builder::new(Count {
                                stops: Rc::clone(&stops),
                                ends: Rc::clone(&ends),
                                root: Some((Rc::clone(&root), Rc::clone(&ordered)))
                            })
                            .probe::<KeyTag<0>>(),
                            start,
                            TIMEOUT,
                            |mut app| {
                                let fixed =
                                    (app.drive_for(start, TIMEOUT, ReadyDrop(Rc::clone(&root))))
                                        .expect("manual fixed point");
                                assert!(clock::Deadline::from(fixed.now()) == start);
                                assert!(fixed.into_output(), "root is alive when polled");
                            }
                        )
                        .is_ok()
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

    assert!(root.get());
    assert!(
        ordered.get(),
        "progress and application shutdown follow root drop"
    );
    assert_eq!(stops.get(), Some(1));
    assert_eq!(ends.get(), Some(1));
}

#[test]
fn terminal_drops_root() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let stops = Rc::new(Cell::new(Some(0)));
    let ends = Rc::new(Cell::new(Some(0)));
    let drop = Rc::new(Cell::new(false));
    let valid = Rc::new(Cell::new(true));
    let pair = (shutdown::Pair::new()).expect("shutdown pair");
    let (source, trig) = pair.split();
    assert!(trig.fire().is_ok(), "fire shutdown");
    let mut reports = Vec::new();

    let executor = Configured::quic::<2, 8>().try_source(&mut create, source);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut sess| {
                    assert!(
                        sess.with_quiesce(
                            &mut finish,
                            dispatch::Builder::new(Count {
                                stops: Rc::clone(&stops),
                                ends: Rc::clone(&ends),
                                root: None
                            })
                            .probe::<KeyTag<0>>(),
                            TIMEOUT,
                            |mut app| {
                                assert!(matches!(
                                    app.drive(Dropped {
                                        dropped: Rc::clone(&drop),
                                        valid: Rc::clone(&valid),
                                    }),
                                    Err(drive::Error::Shutdown)
                                ));
                                assert!(drop.get(), "root drops before application shutdown");
                            }
                        )
                        .is_ok()
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

    assert!(drop.get());
    assert!(valid.get(), "root is alive whenever polled");
    assert_eq!(stops.get(), Some(1));
    assert_eq!(ends.get(), Some(1));
}

#[test]
fn drain_orders_completion() {
    let fatal = Poll::Ready(Err(19));
    for (output, shutdown, left) in [
        (Poll::Pending, true, 3),
        (Poll::Pending, true, usize::MAX),
        (fatal, false, 0),
    ] {
        let mut finish = Completion::new();
        let mut create = Attempt::new();
        let begun = Rc::new(Cell::new(false));
        let forced = Rc::new(Cell::new(false));
        let dropped = Cell::new(false);
        let polls = Cell::new(Some(0));
        let (source, trigger) = shutdown::Pair::new().expect("shutdown pair").split();
        if shutdown {
            trigger.fire().expect("pre-install shutdown");
        }
        let config = if left == usize::MAX {
            Positive::from_millis::<1>()
        } else {
            Positive::from_millis::<50>()
        };
        let mut reports = Vec::new();
        let executor = Configured::quic::<2, 8>().try_source(&mut create, source);
        executor
            .as_ref()
            .map_err(|error| error.kind())
            .expect("executor creation");
        executor.ok().expect("validated executor").run(|executor| {
            executor
                .enter_with(
                    |mut sess| {
                        let result = sess
                            .with_app(
                                &mut finish,
                                DrainProbe {
                                    begun: Rc::clone(&begun),
                                    forced: Rc::clone(&forced),
                                    remaining: left,
                                    shutdown: None,
                                    fired: |fired: Result<(), shutdown::FireError>| {
                                        fired.expect("shutdown notification")
                                    },
                                },
                                |app| {
                                    let root = Outcome {
                                        output,
                                        polls: &polls,
                                        dropped: &dropped,
                                        begun: &begun,
                                    };
                                    match app.drive_drain(config, root).expect("root or shutdown") {
                                        ControlFlow::Break(output) => output,
                                        ControlFlow::Continue(report) => {
                                            let expected = if left == usize::MAX {
                                                drain::Outcome::Forced
                                            } else {
                                                drain::Outcome::Quiesced
                                            };
                                            assert!(report.outcome() == expected);
                                            Ok(())
                                        }
                                    }
                                },
                            )
                            .expect("application cleanup");
                        assert_eq!(result, if output.is_pending() { Ok(()) } else { Err(19) });
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
        assert!(dropped.get(), "root drops before drain begins");
        assert_eq!(polls.get(), Some(1));
        assert_eq!(begun.get(), output.is_pending());
        assert_eq!(forced.get(), left == usize::MAX);
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
