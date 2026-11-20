pub mod common;

use std::{cell::Cell, convert::Infallible, rc::Rc, time::Duration};

use dope_core::driver::route::KeyTag;
use dope_manifold::{
    app::env::Bundle,
    client::close,
    network::client::topology::{factory::Application, plan},
    timing::Balanced,
};
use dope_net::{
    link::pool::recv::input,
    tcp::Tcp,
    wire::{
        self,
        receive::{self, mode::Disabled},
    },
};
use dope_test::{
    fibers::Wait,
    harness::{
        GUARD,
        observe::Trace,
        peer,
        signals::{self, Progress as _},
    },
};

use crate::common::{
    contract::Report,
    manifold::client::switch::terminal::{
        clean::Clean,
        failure::{Cause, Event, Failure, Fault, outgoing::Outgoing},
        reject::{Decode, Reject},
    },
    scenario::case::{ClientHost, Connector, Value},
};

#[test]
fn wire_failure_wins() {
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
    .prepare::<KeyTag<0>, _, Bundle<Tcp, Fault, Balanced>>(Value::new(Failure {
        events: events.clone(),
        conn: conn.clone(),
        recv_drained: Cell::new(false),
        closed: closed.clone(),
    }))
    .expect("prepare actual client layout");

    let stream = planned.run(|case| {
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
    });
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
            .prepare::<KeyTag<0>, _, Bundle<Tcp, Outgoing<LATE>, Balanced>>(Value::new(Failure {
                events: Rc::clone(&events),
                conn: conn.clone(),
                recv_drained: Cell::new(true),
                closed: closed.clone(),
            }))
            .expect("prepare outgoing failure client");
            planned.run(|case| {
                assert_eq!(
                    case.try_wait(&closed, 1).expect("drive close"),
                    Wait::Reached
                );
            });
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
    W::Receive: receive::Policy<W, Credit = Disabled>,
    for<'life, 'p, 'd> Value<Reject>: Application<
            'life,
            'p,
            'd,
            (),
            KeyTag<0>,
            1,
            Bundle<Tcp, W, Balanced>,
            Input = plan::Local,
            Session = Reject,
            Error = Infallible,
        >,
    for<'q, 'h, 'p, 'd> ClientHost<'q, 'h, 'p, 'd, KeyTag<0>, 1, Reject, Bundle<Tcp, W, Balanced>>:
        dope_runtime::executor::raw::App<'d>,
{
    peer::TcpScript::with(
        |stream| {
            stream.write_all(b"X")?;
            stream.read_to_end(&mut Vec::new())
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
            .prepare::<KeyTag<0>, Value<Reject>, Bundle<Tcp, W, Balanced>>(Value::new(Reject {
                audit: Rc::clone(&audit),

                decode: Decode,
                phase: phase.clone(),
                commit: commit.clone(),
                settled: settled.clone(),
                closed: closed.clone(),
            }))
            .expect("prepare actual client layout");

            planned.run(|case| {
                for gate in [&commit, &settled, &closed] {
                    let waited = case.try_wait(gate, 1);
                    audit.checked((0, 0));
                    assert!(matches!(waited, Ok(Wait::Reached)), "progress: {waited:?}");
                }
            });

            audit.checked((0, 0));
            assert_eq!([commit.hits(), settled.hits(), closed.hits()], [1; 3]);
            assert_eq!(phase.get(), 3);
            assert!(matches!(peer.finish(), Ok(Ok(_))), "finish protocol peer");
        },
    )
    .expect("spawn protocol peer");
}
