pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    fmt::Debug,
    io::{Read as _, Write as _},
    net::Shutdown,
    rc::Rc,
    time::Duration,
};

use dope_core::driver::{
    life::recovery,
    route::{Id, KeyTag},
};
use dope_manifold::{app::env::Bundle, timing::Balanced};
use dope_net::{tcp::Tcp, wire};
use dope_runtime::{executor::session::Completion, topology::error::Kind};
use dope_test::{
    fibers,
    harness::{
        peer,
        signals::{self, Progress as _},
    },
    reclaim::Reports,
};

use crate::common::{
    contract::Report,
    manifold::client::{
        error,
        switch::{
            eof::{self, Stages, decode},
            terminal::eof::{Decode, Eof},
        },
    },
    scenario::case::{Connector, Value},
};

#[test]
fn eof_retries_finish() {
    let mut finish = Completion::new();

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
            .prepare_off::<KeyTag<0>, _, Bundle<Tcp, wire::Identity, Balanced>>(eof::Eof {
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
            })
            .expect("prepare actual client layout");
            let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

            planned
                .with_run(
                    &mut finish,
                    |case| {
                        reached(case.try_wait(&blocked, 1), &audit);
                        reached(case.try_wait(&commit, 1), &audit);
                        reached(case.try_wait(&settled, 1), &audit);
                        reached(case.try_wait(&closed, 1), &audit);
                    },
                    recovery::Policy::new(|event| reclaim1.push(event), recover),
                    native,
                )
                .expect("application scenario");
            assert!(
                matches!(reclaim1.events(), Ok(events) if events.is_empty()),
                "unexpected reclaimed operations: {reclaim1:?}"
            );

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
fn eof_commits_then_settles() {
    let mut finish = Completion::new();

    peer::TcpScript::with(
        |stream| {
            assert!(stream.write_all(b"T").is_ok(), "write terminal input");
            assert!(stream.shutdown(Shutdown::Write).is_ok(), "terminal EOF");
            let mut closed = Vec::new();
            assert!(
                stream.read_to_end(&mut closed).is_ok(),
                "wait for terminal connection close"
            );
        },
        |peer| {
            let phase = Rc::new(Cell::new(0));
            let commit = signals::Gate::new();
            let settled = signals::Gate::new();
            let closed = signals::Gate::new();
            let audit = Rc::new(Report::default());

            let planned = Connector::<1>::new(
                peer.addr(),
                Duration::from_millis(10),
                Duration::from_millis(50),
            )
            .prepare_off::<KeyTag<0>, _, Bundle<Tcp, wire::Identity, Balanced>>(Value::new(Eof {
                audit: Rc::clone(&audit),

                decode: Decode,
                phase: phase.clone(),
                commit: commit.clone(),
                settled: settled.clone(),
                closed: closed.clone(),
            }))
            .expect("prepare actual client layout");
            let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

            planned
                .with_run(
                    &mut finish,
                    |case| {
                        reached(case.try_wait(&commit, 1), &audit);
                        reached(case.try_wait(&settled, 1), &audit);
                        reached(case.try_wait(&closed, 1), &audit);
                    },
                    recovery::Policy::new(|event| reclaim1.push(event), recover),
                    native,
                )
                .expect("application scenario");
            assert!(
                matches!(reclaim1.events(), Ok(events) if events.is_empty()),
                "unexpected reclaimed operations: {reclaim1:?}"
            );

            audit.checked((0, 0));
            assert_eq!([commit.hits(), settled.hits(), closed.hits()], [1; 3]);
            assert_eq!(phase.get(), 3);
            let finish = peer.finish();
            assert!(finish.is_ok(), "finish terminal peer");
        },
    )
    .expect("spawn terminal peer");
}

fn native<S: Debug, T: Debug>(result: Result<T, Kind<'_, S, &T>>) -> T {
    result.expect("native lifecycle")
}

fn reached(wait: Result<fibers::Wait, fibers::Run>, audit: &Report<error::Error, 16>) {
    audit.checked((0, 0));
    assert!(
        matches!(wait, Ok(fibers::Wait::Reached)),
        "progress: {wait:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
