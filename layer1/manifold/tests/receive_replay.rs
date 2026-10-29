pub mod common;

use std::{
    convert::Infallible, fmt::Debug, io::Write as _, net::TcpStream, rc::Rc, time::Duration,
};

use common::{
    manifold::listener::{
        finish::{close::control::Control, scoped},
        trace::One,
    },
    observed::{App, Observed, State},
    scenario::case::listener::Listener,
};
use dope_core::driver::{
    life::{creation::Attempt, recovery},
    route::{FrameworkTag, KeyTag, Tag, kind},
};
use dope_manifold::{
    app::{Host, env::Bundle},
    network::listener,
    timing::Throughput,
};
use dope_net::{
    tcp::Tcp,
    wire::{Identity, domain},
};
use dope_runtime::{
    executor::{
        Executor,
        session::{self, Completion},
    },
    topology::error::Kind,
};
use dope_test::{
    fibers::{self, Run, Wait},
    harness::{
        observe::Trace,
        peer::Peer,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

fn native<R: Debug>(outcome: Result<R, Kind<'_, Infallible, &R>>) -> R {
    outcome.expect("native lifecycle")
}

fn reached(outcome: Result<Wait, Run>, actual: u32, want: u32) {
    assert_eq!(
        outcome.expect("drive progress"),
        Wait::Reached,
        "{actual} of {want} hits"
    );
}

#[test]
fn replays_owner() {
    let mut finish = Completion::new();
    let closed = Gate::new();
    let done = Gate::new();
    let seen = Rc::new(Trace::new());
    let prepared = Listener::new::<4>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            scoped::App {
                closed: closed.clone(),
                retired: done.clone(),
                seen: Rc::clone(&seen),
            },
            Identity,
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .exchange(b"one receive owner".to_vec(), |peer| {
                        reached(case.try_wait(&closed, 1), closed.hits(), 1);
                        reached(case.try_wait(&done, 1), done.hits(), 1);
                        let reply = peer.finish().expect("peer finish").expect("peer I/O");
                        assert!(reply.is_empty());
                    })
                    .expect("peer spawn");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    let seen = seen.values().expect("scoped observations");
    assert!(
        matches!(&*seen, [
        scoped::Event::Chunk(first), scoped::Event::Chunk(replay), scoped::Event::Consumed(Ok(())),
    ] if first == replay && first == b"one receive owner"),
        "scoped receive events: {seen:?}"
    );
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn replay_preserves_wake() {
    let writes = Rc::new(Trace::new());
    let state = Rc::new(State::default());
    let mut attempt = Attempt::new();
    let config = One::driver().expect("listener layout");
    let mut reports = Reports::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    let executor = Executor::new(&mut attempt, config);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("create executor");
    let executor = executor.ok().expect("executor");
    executor.run(|executor| {
        let result = executor.enter_with(
            |mut scope| {
                let hash = scope.hash_state(listener::Domain::DEFAULT);
                let mut finish = Completion::new();
                listener::Install::<KeyTag<0>, Bundle<Tcp, Control, Throughput>, _>::new(
                    One::config(),
                    domain::Local::new(Rc::clone(&writes)),
                )
                .with_driver(
                    App(Rc::clone(&state)),
                    hash,
                    &mut scope,
                    session::Scope::driver,
                    |listener, scope| {
                        let addr = listener.local_addr();
                        let result = scope.with_quiesce(
                            &mut finish,
                            Host::new(Observed {
                                listener,
                                state: Rc::clone(&state),
                            }),
                            Duration::from_secs(2),
                            |mut app| {
                                let mut peer = TcpStream::connect(addr).expect("connect peer");
                                peer.write_all(b"A").expect("write parked receive");
                                reached(
                                    fibers::TEST.try_wait(&mut app, &state.received, 2),
                                    state.received.hits(),
                                    2,
                                );
                                assert_eq!(state.bytes.get(), 1);
                            },
                        );
                        assert!(result.is_ok(), "finish replay application");
                    },
                )
                .expect("install listener");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(result.is_ok(), "finish replay domain");
    });
    reports.closed().expect("reclaimed close operations");
    assert_eq!(
        state.early.get(),
        0,
        "receive must follow the exhausted replay"
    );
    let writes = writes.values().expect("wire writes");
    assert!(writes.is_empty(), "wire write failures: {writes:?}");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
