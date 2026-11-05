pub mod common;

use std::{cell::Cell, fmt::Debug, rc::Rc, time::Duration};

use dope_core::driver::{
    life::recovery,
    route::{Id, KeyTag},
};
use dope_manifold::{
    app::env::Bundle,
    client::{
        app::request,
        close,
        session::{
            Session,
            codec::{Codec, staging::Buffered},
            policy,
        },
    },
    timing::Balanced,
};
use dope_net::{
    link::pool::recv::{input, reserve},
    tcp::Tcp,
    wire::{
        self, domain,
        receive::{
            self,
            mode::{Disabled, access},
        },
    },
};
use dope_runtime::{executor::session::Completion, topology::error::Kind};
use dope_test::{
    fibers::Wait,
    harness::{
        GUARD,
        observe::Trace,
        peer,
        signals::{self, Progress as _},
    },
    reclaim::Reports,
};

use crate::common::{
    contract::Report,
    manifold::client::switch::terminal::{
        clean::Clean,
        failure::{Cause, Event, Failure, Fault, outgoing::Outgoing},
        reject::{Decode, Reject},
    },
    recover,
    scenario::case::{Connector, Value},
};

#[test]
fn wire_failure_wins() {
    let mut finish = Completion::new();
    let listener =
        peer::TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("bind wire-failure peer");
    let conn = signals::Gate::new();
    let events = Rc::new(Trace::new());
    let closed = signals::Gate::new();

    let planned = Connector::<1>::new(
        listener.local_addr().expect("wire-failure peer address"),
        Duration::from_millis(10),
        Duration::from_millis(50),
    )
    .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Fault, Balanced>>(Value::new(Failure {
        events: events.clone(),
        conn: conn.clone(),
        recv_drained: Cell::new(false),
        closed: closed.clone(),
    }))
    .expect("prepare actual client layout");
    let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

    let result = planned.with_run(
        &mut finish,
        |case| {
            let ready = case.try_wait(&conn, 1);
            assert!(
                matches!(ready, Ok(Wait::Reached)),
                "connect: {ready:?}; {:?}",
                events.values().expect("terminal observations")
            );
            let rule = close::Directive::permanent(close::Reason::Local(
                close::LocalOrigin::ApiRequest,
            ));
            assert!(
                matches!(events.values().expect("terminal observations").first(), Some(Event::Connected {
                    applied: Some(applied)
                }) if *applied == rule),
                "connection: {:?}",
                events.values().expect("terminal observations")
            );
            let stream = listener
                .accept_for(GUARD)
                .expect("accept wire-failure peer")
                .expect("connected wire-failure peer");
            stream.set_nodelay(true).expect("wire-failure nodelay");
            stream.read_wait(Some(GUARD)).expect("wire-failure read deadline");
            stream.write_wait(Some(GUARD)).expect("wire-failure write deadline");
            stream.write_all(b"X").expect("write failing wire input");
            let ended = case.try_wait(&closed, 1);
            assert!(
                matches!(ended, Ok(Wait::Reached)),
                "close: {ended:?}; {:?}",
                events.values().expect("terminal observations")
            );
            stream
        },
        recovery::Policy::new(|event| reclaim1.push(event), recover),
        native,
    );
    assert!(
        result.is_ok(),
        "application: {result:?}; {:?}",
        events.values().expect("terminal observations")
    );
    assert!(
        matches!(reclaim1.events(), Ok(events) if events.is_empty()),
        "unexpected reclaimed operations: {reclaim1:?}"
    );
    let events = events.values().expect("terminal observations");
    assert!(
        matches!(
            &*events,
            [
                Event::Connected { .. },
                Event::Quarantined,
                Event::Failed(Cause::Receive, None),
                Event::Closed(close::Reason::Transport)
            ]
        ),
        "terminal sequence: {events:?}"
    );
    let stream = result.expect("wire-failure scenario");
    stream
        .read_to_end(&mut Vec::new())
        .expect("wait for failed connection close");
}

#[test]
fn close_error_once() {
    outgoing::<false>();
}

#[test]
fn completion_error_once() {
    outgoing::<true>();
}

fn outgoing<const LATE: bool>() {
    let mut finish = Completion::new();
    peer::TcpScript::with(
        |stream| {
            let mut output = Vec::new();
            stream.read_to_end(&mut output).map(|_| output)
        },
        |peer| {
            let conn = signals::Gate::new();
            let closed = signals::Gate::new();
            let events = Rc::new(Trace::new());
            let planned = Connector::<1>::new(
                peer.addr(),
                Duration::from_millis(10),
                Duration::from_millis(50),
            )
            .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Outgoing<LATE>, Balanced>>(Value::new(
                Failure {
                    events: Rc::clone(&events),
                    conn: conn.clone(),
                    recv_drained: Cell::new(true),
                    closed: closed.clone(),
                },
            ))
            .expect("prepare outgoing failure client");
            let mut reports = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);
            planned
                .with_run(
                    &mut finish,
                    |case| {
                        assert_eq!(
                            case.try_wait(&closed, 1).expect("drive close"),
                            Wait::Reached
                        );
                    },
                    recovery::Policy::new(|event| reports.push(event), recover),
                    native,
                )
                .expect("outgoing failure client");
            assert!(reports.events().expect("reclaimed events").is_empty());
            let events = events.values().expect("terminal observations");
            let expected = if LATE {
                Cause::Completion
            } else {
                Cause::Close
            };
            assert!(
                matches!(
                    &*events,
                    [
                        Event::Connected { .. },
                        Event::Quarantined,
                        Event::Failed(cause, None),
                        Event::Closed(close::Reason::Transport),
                    ] if *cause == expected
                ),
                "terminal sequence: {events:?}"
            );
            assert_eq!([conn.hits(), closed.hits()], [1; 2]);
            let expected: &[u8] = if LATE { b"!" } else { b"" };
            assert_eq!(
                peer.finish().expect("peer finish").expect("peer I/O"),
                expected
            );
        },
    )
    .expect("spawn outgoing failure peer");
}

#[test]
fn protocol_closes() {
    run::<wire::Identity>();
}

#[test]
fn app_failure_wins() {
    run::<Clean>();
}

fn run<W>()
where
    W: input::Execution,
    W::Bootstrap: domain::Init<W>,
    W::Receive: receive::Policy<W, Access = access::Direct, Credit = Disabled>,
    <W::Receive as receive::Policy<W>>::Reserve: reserve::Choice<W, W::Retained>,
    for<'d> Reject:
        Session<'d, Requests = request::Disabled> + policy::Compatible<'d, 'd, W, KeyTag<0>>,
    for<'d> <Reject as Session<'d>>::Codec: Codec<Staging = Buffered>,
    for<'d> W::Binding<'d, KeyTag<0>>: Default,
{
    peer::TcpScript::with(
        |stream| {
            stream.write_all(b"X")?;
            stream.read_to_end(&mut Vec::new())
        },
        |peer| {
            let mut finish = Completion::new();
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
            .prepare_off::<KeyTag<0>, _, Bundle<Tcp, W, Balanced>>(Value::new(Reject {
                audit: Rc::clone(&audit),

                decode: Decode,
                phase: phase.clone(),
                commit: commit.clone(),
                settled: settled.clone(),
                closed: closed.clone(),
            }))
            .expect("prepare actual client layout");
            let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

            let result = planned.with_run(
                &mut finish,
                |case| {
                    for gate in [&commit, &settled, &closed] {
                        let waited = case.try_wait(gate, 1);
                        audit.checked((0, 0));
                        assert!(matches!(waited, Ok(Wait::Reached)), "progress: {waited:?}");
                    }
                },
                recovery::Policy::new(|event| reclaim1.push(event), recover),
                native,
            );
            assert!(result.is_ok(), "application: {result:?}");
            assert!(
                matches!(reclaim1.events(), Ok(events) if events.is_empty()),
                "unexpected reclaimed operations: {reclaim1:?}"
            );

            audit.checked((0, 0));
            assert_eq!([commit.hits(), settled.hits(), closed.hits()], [1; 3]);
            assert_eq!(phase.get(), 3);
            assert!(matches!(peer.finish(), Ok(Ok(_))), "finish protocol peer");
        },
    )
    .expect("spawn protocol peer");
}

fn native<S: Debug, T: Debug>(result: Result<T, Kind<'_, S, &T>>) -> T {
    result.expect("native lifecycle")
}
