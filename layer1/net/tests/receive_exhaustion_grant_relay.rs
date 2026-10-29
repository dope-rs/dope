pub mod common;

use std::{convert::Infallible, io::Write, net::Shutdown, time::Duration};

use common::{discard::eof, wire::Input};
use dope_core::{
    Errno,
    driver::{life::recovery, result::poll, route::kind, schedule::ready},
    io::{
        Reclaimed,
        event::{
            batch::Flow,
            net::{
                accept::Failure,
                recv::{Kind, Status},
            },
        },
    },
};
use dope_net::link::{
    event::Dispatch,
    pool::recv::{self, input},
};
use dope_test::{harness::scenario::process::Deadline, scope};

#[test]
fn idle_peer_progress() {
    #[derive(Debug)]
    enum Phase {
        ProbeActive,
        ProbeReady,
        Waiting,
    }

    Deadline::with(Duration::from_secs(10), |_| {
        for phase in [Phase::ProbeActive, Phase::ProbeReady, Phase::Waiting] {
            let (probes, message) = match phase {
                Phase::ProbeActive | Phase::ProbeReady => (1, "B progresses while A stays idle"),
                Phase::Waiting => (2, "B progresses after idle retry"),
            };
            let mut tails = 0;
            let (reports, input) = eof::run_pair::<input::Scoped>(
                |pool, [a, b], scope, [left, mut right]| {
                    eof::arm(pool, b, scope).expect("receive admission");
                    let [first, second] = [(); 2].map(|()| {
                        right.write_all(b"H").expect("hold native buffer");
                        let (flow, item) = eof::receive(scope).expect("native data");
                        assert_eq!(flow, Flow::More);
                        let done = match item {
                            Kind::Data(done) => Some(done),
                            _ => None,
                        }
                        .expect("native data owner");
                        let (token, _, bytes) = done.into_parts();
                        assert!(b.target().operation::<kind::Recv>().matches(token));
                        bytes
                    });
                    right.write_all(b"N").expect("waiting peer input");
                    let (flow, item) = eof::receive(scope).expect("waiting peer exhaustion");
                    assert_eq!(flow, Flow::Last);
                    let waiting = match item {
                        Kind::Control(waiting) => Some(waiting),
                        _ => None,
                    }
                    .expect("waiting peer control");

                    eof::arm(pool, a, scope).expect("idle receive admission");
                    let (flow, item) = eof::receive(scope).expect("idle native exhaustion");
                    assert_eq!(flow, Flow::Last);
                    let done = match item {
                        Kind::Control(done) => Some(done),
                        _ => None,
                    }
                    .expect("idle exhaustion control");
                    for (key, done) in [(a, done), (b, waiting)] {
                        assert!(key.target().operation::<kind::Recv>().matches(done.token()));
                        assert!(matches!(done.status(), Status::Exhausted));
                        assert!(matches!(pool.recv().dispatch(done), Dispatch::Ready(_)));
                    }
                    pool.get_mut(a)
                        .expect("idle owner")
                        .io()
                        .wake_target()
                        .wake();
                    let mut woke = false;
                    assert!(matches!(
                        eof::take_ready(scope, |token| {
                            let key = if a.target().dispatch().matches(token) {
                                woke = true;
                                a
                            } else {
                                assert!(b.target().dispatch().matches(token));
                                b
                            };
                            let resumed = match pool.recv().resume(key.target()) {
                                recv::Resumption::Ready(resumed) => Some(resumed),
                                _ => None,
                            }
                            .expect("pending receive owner");
                            assert_eq!(resumed.key(), key);
                        }),
                        ready::Readiness::Ready
                    ));
                    assert!(woke, "the unrelated wake reaches idle A");

                    for key in [b, a].into_iter().take(probes) {
                        eof::arm(pool, key, scope).expect("native exhaustion probe");
                        let (flow, item) = eof::receive(scope).expect("probe exhaustion");
                        assert_eq!(flow, Flow::Last);
                        let done = match item {
                            Kind::Control(done) => Some(done),
                            _ => None,
                        }
                        .expect("probe exhaustion control");
                        assert!(key.target().operation::<kind::Recv>().matches(done.token()));
                        assert!(matches!(done.status(), Status::Exhausted));
                        assert!(matches!(pool.recv().dispatch(done), Dispatch::Settled));
                    }

                    drop(first);
                    scope.with_driver(|mut driver, mut control| {
                        let mut turn = control.begin();
                        assert!(matches!(
                            driver.commit(turn.reactor()),
                            Ok(poll::Commit::Drained)
                        ));
                    });
                    if matches!(phase, Phase::ProbeActive) {
                        eof::arm(pool, a, scope).expect("idle probe submission");
                        scope.with_driver(|mut driver, mut control| {
                            let mut turn = control.begin();
                            assert!(matches!(
                                driver.commit(turn.reactor()),
                                Ok(poll::Commit::Drained)
                            ));
                        });
                    }
                    for initial in [true, false] {
                        let mut woke = false;
                        let found = eof::take_ready(scope, |token| {
                            let key = if a.target().dispatch().matches(token) {
                                woke = true;
                                a
                            } else {
                                assert!(b.target().dispatch().matches(token));
                                b
                            };
                            let resumed = match pool.recv().resume(key.target()) {
                                recv::Resumption::Ready(resumed) => Some(resumed),
                                _ => None,
                            }
                            .expect("live receive owner");
                            assert_eq!(resumed.key(), key);
                        });
                        if initial {
                            assert!(matches!(found, ready::Readiness::Ready));
                            assert!(woke, "the returned backing first granted idle A");
                        }
                        eof::flush(pool, scope).expect("submit the granted idle retry");
                        scope.with_driver(|mut driver, mut control| {
                            let mut turn = control.begin();
                            assert!(matches!(
                                driver.commit(turn.reactor()),
                                Ok(poll::Commit::Drained)
                            ));
                        });
                    }
                    assert!(!pool.inspect().has_rearm());

                    let (flow, item) = eof::receive(scope).expect(message);
                    assert_eq!(flow, Flow::More);
                    let done = match item {
                        Kind::Data(done) => Some(done),
                        _ => None,
                    }
                    .expect("waiting peer data");
                    let (token, _, bytes) = done.into_parts();
                    assert!(b.target().operation::<kind::Recv>().matches(token));
                    assert_eq!(bytes.as_slice(), b"N");
                    for key in [a, b] {
                        assert!(!pool.inspect().get(key).expect("live owner").is_closing());
                    }
                    drop(bytes);
                    drop(second);

                    for key in [a, b] {
                        pool.get_mut(key).expect("closing owner").io().close();
                    }
                    left.shutdown(Shutdown::Write)
                        .expect("idle peer FIN after progress");
                    right
                        .shutdown(Shutdown::Write)
                        .expect("waiting peer FIN after progress");
                    let mut seen = [false; 2];
                    for _ in 0..2 {
                        let (flow, item) =
                            eof::receive(scope).expect("native terminal after close");
                        assert_eq!(flow, Flow::Last);
                        let done = match item {
                            Kind::Control(done) => Some(done),
                            _ => None,
                        }
                        .expect("native terminal completion");
                        let index = if a.target().operation::<kind::Recv>().matches(done.token()) {
                            0
                        } else {
                            assert!(b.target().operation::<kind::Recv>().matches(done.token()));
                            1
                        };
                        assert!(!seen[index], "one terminal per receive owner");
                        seen[index] = true;
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
                            tails += 1;
                        }
                    }
                    assert_eq!(seen, [true; 2]);
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
            assert_eq!(errors, tails);
        }
    })
    .expect("test deadline");
}
