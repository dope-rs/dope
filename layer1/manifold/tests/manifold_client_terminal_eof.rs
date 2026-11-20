pub mod common;

use crate::common::manifold::client::reached;

use std::{cell::Cell, net::Shutdown, rc::Rc, time::Duration};

use dope_core::driver::route::KeyTag;
use dope_manifold::{
    app::{env::Bundle, receive},
    client::{
        close::{LocalOrigin, Reason},
        deadline,
    },
    timing::Balanced,
};
use dope_net::{tcp::Tcp, wire};
use dope_test::harness::{
    GUARD, peer,
    signals::{self, Progress as _},
};
use o3::base::time::Positive;

use crate::common::{
    contract::Report,
    manifold::client::{
        switch::{
            eof::{self, Stages, decode},
            terminal::eof::{Decode, Eof},
        },
    },
    scenario::case::{Connector, Value},
};

#[test]
fn eof_retries_finish() {

    peer::TcpScript::with(
        |stream| {
            stream.write_all(b"T")?;
            stream.shutdown(Shutdown::Write)?;
            stream.read_to_end(&mut Vec::new())
        },
        |peer| {
            let parses = Rc::new(Cell::new(0));
            let finishes = Rc::new(Cell::new(0));
            let phase = Rc::new(Cell::new(0));
            let blocked = signals::Gate::new();
            let commit = signals::Gate::new();
            let settled = signals::Gate::new();
            let closed = signals::Gate::new();
            let audit = Rc::new(Report::default());

            let planned = Connector::<1>::new(
                peer.addr(),
                Duration::from_millis(10),
                Duration::from_millis(50),
            )
            .prepare::<KeyTag<0>, _, Bundle<Tcp, wire::Identity, Balanced>>(eof::Eof {
                audit: Rc::clone(&audit),
                decode: decode::Decode {
                    parses: parses.clone(),
                    finishes: finishes.clone(),
                },
                phase: phase.clone(),
                stages: Stages {
                    blocked: blocked.clone(),
                    commit: commit.clone(),
                    settled: settled.clone(),
                    closed: closed.clone(),
                },
                deferred: None,
            })
            .expect("prepare actual client layout");

            planned.run(|case| {
                reached(case.try_wait(&blocked, 1), &audit);
                reached(case.try_wait(&commit, 1), &audit);
                reached(case.try_wait(&settled, 1), &audit);
                reached(case.try_wait(&closed, 1), &audit);
            });

            audit.checked((0, 0));
            assert_eq!(
                [blocked.hits(), commit.hits(), settled.hits(), closed.hits()],
                [1; 4]
            );
            assert_eq!(phase.get(), 4);
            assert_eq!(parses.get(), 1);
            assert_eq!(finishes.get(), 2);
            assert!(matches!(peer.finish(), Ok(Ok(_))), "finish terminal peer");
        },
    )
    .expect("spawn terminal peer");
}

#[test]
fn eof_blocked_waits_and_cancels() {
    for cancel in [
        None,
        Some(Reason::Timeout(deadline::Kind::Lifetime)),
        Some(Reason::Local(LocalOrigin::RuntimeShutdown)),
    ] {
        peer::TcpScript::with(
            |stream| {
                stream.write_all(b"T")?;
                stream.shutdown(Shutdown::Write)?;
                stream.read_to_end(&mut Vec::new())
            },
            |peer| {
                let parses = Rc::new(Cell::new(0));
                let finishes = Rc::new(Cell::new(0));
                let phase = Rc::new(Cell::new(0));
                let blocked = signals::Gate::new();
                let waiting = signals::Gate::new();
                let release = signals::Gate::new();
                let closed = signals::Gate::new();
                let audit = Rc::new(Report::default());
                let connector = Connector::<1>::new(
                    peer.addr(),
                    Duration::from_millis(10),
                    Duration::from_millis(50),
                );
                let connector = if matches!(cancel, Some(Reason::Timeout(_))) {
                    connector.lifetime(Positive::from_millis::<250>())
                } else {
                    connector
                };
                let planned = connector
                    .prepare::<KeyTag<0>, _, Bundle<Tcp, wire::Identity, Balanced>>(eof::Eof {
                        audit: Rc::clone(&audit),
                        decode: decode::Decode {
                            parses,
                            finishes: finishes.clone(),
                        },
                        phase: phase.clone(),
                        stages: Stages {
                            blocked: blocked.clone(),
                            commit: signals::Gate::new(),
                            settled: signals::Gate::new(),
                            closed: closed.clone(),
                        },
                        deferred: Some(eof::Deferred {
                            waiting: waiting.clone(),
                            release: release.clone(),
                            cancel,
                        }),
                    })
                    .expect("prepare blocked EOF client");
                planned.run(|case| {
                    reached(case.try_wait(&blocked, 1), &audit);
                    // The application probe is reached only after the connection's
                    // pending queue becomes empty; a self-requeued close never reaches it.
                    reached(case.try_wait(&waiting, 1), &audit);
                    assert_eq!(finishes.get(), 1);
                    if cancel.is_none() {
                        release.hit();
                    }
                    if cancel != Some(Reason::Local(LocalOrigin::RuntimeShutdown)) {
                        reached(case.try_wait(&closed, 1), &audit);
                    }
                });
                audit.checked((0, 0));
                assert_eq!(closed.hits(), 1);
                assert_eq!(finishes.get(), if cancel.is_none() { 2 } else { 1 });
                assert_eq!(phase.get(), if cancel.is_none() { 4 } else { 1 });
                assert!(matches!(peer.finish(), Ok(Ok(_))));
            },
        )
        .expect("spawn blocked EOF peer");
    }
}

#[test]
fn eof_commits_then_settles() {
    for (split, excess) in [(true, false), (true, true), (false, false), (false, true)] {
        let listener =
            peer::TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("bind terminal peer");
        let phase = Rc::new(Cell::new(0));
        let conn = signals::Gate::new();
        let commit = signals::Gate::new();
        let settled = signals::Gate::new();
        let closed = signals::Gate::new();
        let partial = signals::Gate::new();
        let full = signals::Gate::new();
        let audit = Rc::new(Report::default());

        let planned = Connector::<1>::new(
            listener.local_addr().expect("terminal peer address"),
            Duration::from_millis(10),
            Duration::from_millis(50),
        )
        .prepare::<KeyTag<0>, _, Bundle<Tcp, wire::Identity, Balanced>>(Value::new(Eof {
            audit: Rc::clone(&audit),
            decode: Decode {
                partial: partial.clone(),
                full: full.clone(),
            },
            phase: phase.clone(),
            conn: conn.clone(),
            commit: commit.clone(),
            settled: settled.clone(),
            closed: closed.clone(),
            reason: if excess {
                Reason::Ingress(receive::Failure::ParseCapacity)
            } else {
                Reason::Remote
            },
        }))
        .expect("prepare actual client layout");

        let stream = planned.run(|case| {
            reached(case.try_wait(&conn, 1), &audit);
            let stream = listener
                .accept_for(GUARD)
                .expect("accept terminal peer")
                .expect("connected terminal peer");
            stream.set_nodelay(true).expect("terminal nodelay");
            stream
                .read_wait(Some(GUARD))
                .expect("terminal read deadline");
            stream
                .write_wait(Some(GUARD))
                .expect("terminal write deadline");
            if split {
                stream.write_all(b"T").expect("write terminal input");
                reached(case.try_wait(&partial, 1), &audit);
                stream.write_all(b"T").expect("fill terminal input");
            } else {
                stream.write_all(b"TT").expect("write full terminal input");
            }
            reached(case.try_wait(&full, 1), &audit);
            if !split {
                assert_eq!(partial.hits(), 0, "first input fills the limit");
            }
            if excess {
                stream.write_all(b"X").expect("exceed terminal capacity");
            } else {
                stream.shutdown(Shutdown::Write).expect("terminal EOF");
                reached(case.try_wait(&commit, 1), &audit);
                reached(case.try_wait(&settled, 1), &audit);
            }
            reached(case.try_wait(&closed, 1), &audit);
            stream
        });

        audit.checked((0, 0));
        let accepted = u32::from(!excess);
        assert_eq!(
            [commit.hits(), settled.hits(), closed.hits()],
            [accepted, accepted, 1]
        );
        assert_eq!(full.hits(), 1 + u32::from(excess));
        assert_eq!(phase.get(), if excess { 0 } else { 3 });
        stream
            .read_to_end(&mut Vec::new())
            .expect("wait for terminal connection close");
    }
}
