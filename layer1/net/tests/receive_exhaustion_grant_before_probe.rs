pub mod common;

use std::{convert::Infallible, io::Write, net::Shutdown, ops::ControlFlow, time::Duration};

use common::{discard::eof, wire::Input};
use dope_core::{
    Errno,
    driver::{life::recovery, result::poll, schedule::ready},
    io::{
        Reclaimed,
        event::{
            batch::Flow,
            net::{
                accept::Failure,
                recv::{Kind, Outcome, Status},
            },
        },
    },
};
use dope_net::link::{
    event::{self, Dispatch, ParkRecv},
    pool::recv::{self, input},
};
use dope_test::{harness::scenario::process::Deadline, scope};

#[derive(Clone, Copy, Debug)]
enum Order {
    Live,
    Paused,
    Queued,
    Active,
}

#[test]
fn grant_before_probe() {
    Deadline::with(Duration::from_secs(10), |_| {
        for order in [Order::Live, Order::Paused, Order::Queued, Order::Active] {
            let paused = matches!(order, Order::Paused);
            let queued = matches!(order, Order::Queued | Order::Active);
            let active = matches!(order, Order::Active);
            let mut tail = false;
            let (reports, input) = eof::run::<input::Scoped>(
                |pool, key, scope, mut peer| {
                    eof::arm(pool, key, scope).expect("receive admission");
                    let [first, second] = [(); 2].map(|()| {
                        peer.write_all(b"H").expect("hold native buffer");
                        let (flow, item) = eof::receive(scope).expect("native data");
                        assert_eq!(flow, Flow::More);
                        match item {
                            Kind::Data(done) => Some(done),
                            _ => None,
                        }
                        .expect("native data owner")
                    });
                    peer.write_all(b"N").expect("pending nonfinal byte");
                    let (flow, item) = eof::receive(scope).expect("native exhaustion");
                    assert_eq!(flow, Flow::Last);
                    let done = match item {
                        Kind::Control(done) => Some(done),
                        _ => None,
                    }
                    .expect("terminal exhaustion");
                    assert!(matches!(done.status(), Status::Exhausted));
                    if queued {
                        pool.suspend(key);
                        assert!(matches!(
                            pool.recv().dispatch(done),
                            Dispatch::Parked(ParkRecv::Deferred)
                        ));
                        pool.resume(key);
                        assert!(matches!(
                            eof::take_ready(scope, |token| {
                                assert!(key.target().dispatch().matches(token));
                            }),
                            ready::Readiness::Ready
                        ));
                        eof::arm(pool, key, scope).expect("second native flight");
                    } else {
                        assert!(matches!(pool.recv().dispatch(done), Dispatch::Ready(_)));
                        eof::flush(pool, scope).expect("optimistic probe submission");
                    }
                    let (flow, item) = eof::receive(scope).expect("second native exhaustion");
                    assert_eq!(flow, Flow::Last);
                    let probe = match item {
                        Kind::Control(done) => Some(done),
                        _ => None,
                    }
                    .expect("terminal probe exhaustion");
                    assert!(matches!(probe.status(), Status::Exhausted));

                    let probe = if paused || queued {
                        if paused {
                            pool.suspend(key);
                        }
                        assert!(matches!(
                            pool.recv().dispatch(probe),
                            Dispatch::Parked(ParkRecv::Deferred)
                        ));
                        None
                    } else {
                        Some(probe)
                    };

                    drop(first);
                    scope.with_driver(|mut driver, mut control| {
                        let mut turn = control.begin();
                        assert!(matches!(
                            driver.commit(turn.reactor()),
                            Ok(poll::Commit::Drained)
                        ));
                    });
                    let mut wakes = 0;
                    let found = eof::take_ready(scope, |token| {
                        assert!(key.target().dispatch().matches(token));
                        wakes += 1;
                    });
                    if queued {
                        assert!(matches!(found, ready::Readiness::Idle));
                        assert_eq!(wakes, 0);
                    } else {
                        assert!(matches!(found, ready::Readiness::Ready));
                        assert_eq!(wakes, 1);
                    }
                    let resumed = match pool.recv().resume(key.target()) {
                        recv::Resumption::Ready(resumed) => Some(resumed),
                        _ => None,
                    }
                    .expect("live receive owner");
                    assert_eq!(resumed.key(), key);
                    let dispatch = match probe {
                        Some(probe) => pool.recv().dispatch(probe),
                        None => {
                            if paused {
                                eof::flush(pool, scope).expect("flush while application paused");
                                assert!(!pool.inspect().has_rearm());
                                pool.resume(key);
                                assert!(matches!(
                                    eof::take_ready(scope, |token| {
                                        assert!(key.target().dispatch().matches(token));
                                    }),
                                    ready::Readiness::Ready
                                ));
                            }
                            if queued {
                                let done = match pool.recv().pop_ready(&resumed) {
                                    recv::Ready::Completion(done) => Some(done),
                                    _ => None,
                                }
                                .expect("first queued terminal");
                                let done = match done.classify() {
                                    event::Kind::Control(done) => Some(done),
                                    _ => None,
                                }
                                .expect("first exhaustion control");
                                assert!(matches!(done.status(), Status::Exhausted));
                                assert!(matches!(pool.recv().dispatch(done), Dispatch::Ready(_)));
                                eof::flush(pool, scope).expect("newer probe before queued replay");
                                assert!(!pool.inspect().has_rearm());
                                if active {
                                    let (flow, item) =
                                        eof::receive(scope).expect("active probe data");
                                    assert_eq!(flow, Flow::More);
                                    let done = match item {
                                        Kind::Data(done) => Some(done.rejoin()),
                                        _ => None,
                                    }
                                    .expect("probe native data");
                                    let done = match done.dispatch(|_, _, bytes| {
                                        assert!(matches!(&bytes, Outcome::Data(bytes)
                                            if bytes.as_slice() == b"N"));
                                        ControlFlow::<_, Infallible>::Break(bytes)
                                    }) {
                                        ControlFlow::Break(done) => done,
                                        ControlFlow::Continue(never) => match never {},
                                    };
                                    let done = match done.classify() {
                                        Kind::Data(done) => Some(done),
                                        _ => None,
                                    }
                                    .expect("preserved native data");
                                    assert!(matches!(
                                        pool.reserve().data(done),
                                        event::Reservation::Parked(ParkRecv::Deferred)
                                    ));
                                    assert!(!pool.inspect().has_rearm());
                                }
                            }
                            let done = match pool.recv().pop_ready(&resumed) {
                                recv::Ready::Completion(done) => Some(done),
                                _ => None,
                            }
                            .expect("queued probe completion");
                            let done = match done.classify() {
                                event::Kind::Control(done) => Some(done),
                                _ => None,
                            }
                            .expect("queued exhaustion control");
                            assert!(matches!(done.status(), Status::Exhausted));
                            if active {
                                pool.suspend(key);
                                assert!(!pool.inspect().has_rearm());
                            }
                            pool.recv().dispatch(done)
                        }
                    };
                    if active {
                        assert!(matches!(dispatch, Dispatch::Parked(ParkRecv::Deferred)));
                        assert!(pool.inspect().has_rearm());
                        pool.resume(key);
                    } else {
                        assert!(matches!(dispatch, Dispatch::Settled));
                        if queued {
                            assert!(!pool.inspect().has_rearm());
                        }
                    }
                    eof::flush(pool, scope).expect("rearm from the returned grant");
                    if active {
                        assert!(!pool.inspect().has_rearm());
                    }

                    let bytes = if active {
                        let done = match pool.recv().pop_ready(&resumed) {
                            recv::Ready::Completion(done) => Some(done),
                            _ => None,
                        }
                        .expect("queued native byte");
                        let done = match done.classify() {
                            event::Kind::Data(done) => Some(done),
                            _ => None,
                        }
                        .expect("preserved queued byte");
                        let control = match pool.recv().pop_ready(&resumed) {
                            recv::Ready::Completion(control) => Some(control),
                            _ => None,
                        }
                        .expect("queued exhaustion");
                        let control = match control.classify() {
                            event::Kind::Control(control) => Some(control),
                            _ => None,
                        }
                        .expect("replayed exhaustion control");
                        assert!(matches!(control.status(), Status::Exhausted));
                        assert!(matches!(pool.recv().dispatch(control), Dispatch::Settled));
                        assert!(matches!(
                            pool.recv().pop_ready(&resumed),
                            recv::Ready::Empty
                        ));
                        (None, Some(done))
                    } else {
                        let (flow, item) = eof::receive(scope).expect("live receive after replay");
                        assert_eq!(flow, Flow::More);
                        let bytes = match item {
                            Kind::Data(done) => Some(done.into_parts().2),
                            _ => None,
                        }
                        .expect("pending native byte");
                        assert_eq!(bytes.as_slice(), b"N");
                        (Some(bytes), None)
                    };
                    assert!(
                        !pool
                            .inspect()
                            .get(key)
                            .expect("same receive owner")
                            .is_closing()
                    );
                    drop(bytes);
                    drop(second);

                    pool.get_mut(key)
                        .expect("settled receive owner")
                        .io()
                        .close();
                    peer.shutdown(Shutdown::Write)
                        .expect("peer FIN after progress");
                    let (flow, item) = eof::receive(scope).expect("native terminal after close");
                    assert_eq!(flow, Flow::Last);
                    let done = match item {
                        Kind::Control(done) => Some(done),
                        _ => None,
                    }
                    .expect("native terminal completion");
                    let status = done.status();
                    assert!(matches!(status, Status::Eof | Status::Exhausted));
                    if matches!(status, Status::Eof) {
                        assert!(matches!(pool.recv().dispatch(done), Dispatch::Settled));
                    } else {
                        let done = match pool.recv().dispatch(done) {
                            Dispatch::Retired(done) => Some(done),
                            _ => None,
                        }
                        .expect("exact closing exhaustion");
                        scope.context().reclaim(done);
                        tail = true;
                    }
                    assert!(!pool.inspect().has_rearm());
                },
                |failure: recovery::Recovery<'_, '_>| -> Infallible {
                    panic!("driver recovery: {:?}", failure.cause())
                },
                |result| {
                    result
                        .as_ref()
                        .map_err(scope::Error::cause)
                        .expect("native lifecycle");
                    result
                        .ok()
                        .expect("driver result")
                        .expect("receive fixture");
                },
            );
            assert_eq!(input, Input::Empty);
            let mut errors = 0;
            for report in reports {
                if matches!(report, Reclaimed::RecvRoute {
                    result: Err(error), flow: Flow::Last, ..
                } if error == Errno::NO_BUFFER_SPACE)
                {
                    errors += 1;
                } else {
                    assert!(
                        matches!(report, Reclaimed::Accept {
                            result: Err(Failure::Syscall(error)), flow: Flow::Last, ..
                        } if error.is_canceled()),
                        "unsettled native operation: {report:?}"
                    );
                }
            }
            assert_eq!(errors, usize::from(tail));
        }
    })
    .expect("test deadline");
}
