pub mod common;

use crate::common::manifold::{
    client::{assert_reclaimed},
    native,
};

use std::{net::Shutdown, rc::Rc, time::Duration};

use dope_core::driver::{
    life::recovery,
    route::{Id, KeyTag},
};
use dope_manifold::{app::env::Bundle, client::close, timing::Balanced};
use dope_net::{tcp::Tcp, wire::Identity};
use dope_runtime::executor::session::Completion;
use dope_test::{
    fibers::Wait,
    harness::{GUARD, observe::Trace, peer, signals::Gate},
    reclaim::Reports,
};
use o3::base::time::Positive;

use crate::common::{
    manifold::client::switch::terminal::{
        failure::{Cause, Fault},
        plaintext::{Event, Mode, Plaintext},
    },
    recover,
    scenario::case::{Connector, Value},
};

#[test]
fn plaintext_precedes_failure() {
    for mode in [Mode::Ready, Mode::Yield, Mode::Blocked, Mode::Timeout] {
        let mut finish = Completion::new();
        let listener =
            peer::TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("bind plaintext peer");
        let events = Rc::new(Trace::new());
        let conn = Gate::new();
        let closed = Gate::new();
        let connector = Connector::<1>::new(
            listener.local_addr().expect("plaintext peer address"),
            Duration::from_millis(10),
            Duration::from_millis(50),
        );
        let connector = if matches!(mode, Mode::Timeout) {
            connector.lifetime(Positive::from_millis::<100>())
        } else {
            connector
        };
        let planned = connector
            .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Fault, Balanced>>(Value::new(Plaintext {
                mode,
                events: events.clone(),
                conn: conn.clone(),
                parsed: None,
                closed: closed.clone(),
            }))
            .expect("prepare terminal client");
        let mut reclaimed = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);
        let result = planned.with_run(
            &mut finish,
            |case| {
                assert!(matches!(case.try_wait(&conn, 1), Ok(Wait::Reached)));
                let stream = listener
                    .accept_for(GUARD)
                    .expect("accept plaintext peer")
                    .expect("connected plaintext peer");
                stream.set_nodelay(true).expect("plaintext nodelay");
                stream
                    .read_wait(Some(GUARD))
                    .expect("plaintext read deadline");
                stream
                    .write_wait(Some(GUARD))
                    .expect("plaintext write deadline");
                stream
                    .write_all(b"H")
                    .expect("write head with wire failure");
                let result = case.try_wait(&closed, 1);
                assert!(
                    matches!(result, Ok(Wait::Reached)),
                    "{mode:?}: {result:?}; {:?}",
                    events.values().expect("terminal events")
                );
                stream
            },
            recovery::Policy::new(|event| reclaimed.push(event), recover),
            native,
        );
        assert!(result.is_ok(), "{mode:?}: {result:?}");
        assert_reclaimed(&reclaimed);
        let events = events.values().expect("terminal events");
        let ordered = match mode {
            Mode::Ready => matches!(
                &*events,
                [
                    Event::Head(b'H'),
                    Event::Failed(Cause::Receive, None),
                    Event::Quarantined,
                    Event::Closed(close::Reason::Transport)
                ]
            ),
            Mode::Yield | Mode::Blocked => matches!(
                &*events,
                [
                    Event::Head(b'H'),
                    Event::Settled {
                        byte: b'H',
                        closing: false,
                        expired: false
                    },
                    Event::Failed(Cause::Receive, None),
                    Event::Quarantined,
                    Event::Closed(close::Reason::Transport)
                ]
            ),
            Mode::Timeout => matches!(
                &*events,
                [
                    Event::Head(b'H'),
                    Event::Failed(Cause::Receive, None),
                    Event::Quarantined,
                    Event::Settled {
                        byte: b'H',
                        closing: true,
                        expired: true
                    },
                    Event::Closed(close::Reason::Transport)
                ]
            ),
        };
        assert!(ordered, "{mode:?}: terminal order {events:?}");
        let stream = result.expect("terminal plaintext scenario");
        assert_eq!(stream.read(&mut [0]).expect("wait for close"), 0);
    }
}

#[test]
fn plain_parse_preserves_partial_and_retry() {
    let mut finish = Completion::new();
    let listener = peer::TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("bind plain peer");
    let events = Rc::new(Trace::new());
    let conn = Gate::new();
    let parsed = Gate::new();
    let closed = Gate::new();
    let planned = Connector::<1>::new(
        listener.local_addr().expect("plain peer address"),
        Duration::from_millis(10),
        Duration::from_millis(50),
    )
    .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Identity, Balanced>>(Value::new(Plaintext {
        mode: Mode::Ready,
        events: events.clone(),
        conn: conn.clone(),
        parsed: Some(parsed.clone()),
        closed: closed.clone(),
    }))
    .expect("prepare plain client");
    let mut reclaimed = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);
    let stream = planned
        .with_run(
            &mut finish,
            |case| {
                assert!(matches!(case.try_wait(&conn, 1), Ok(Wait::Reached)));
                let stream = listener
                    .accept_for(GUARD)
                    .expect("accept plain peer")
                    .expect("connected plain peer");
                stream.set_nodelay(true).expect("plain nodelay");
                stream.read_wait(Some(GUARD)).expect("plain read deadline");
                stream
                    .write_wait(Some(GUARD))
                    .expect("plain write deadline");
                stream.write_all(b"P").expect("write partial input");
                assert!(matches!(case.try_wait(&parsed, 1), Ok(Wait::Reached)));
                assert!(matches!(
                    &*events.values().expect("partial input events"),
                    [Event::Parse { len: 1, .. }]
                ));
                stream.write_all(b"HH").expect("complete input");
                assert!(matches!(case.try_wait(&parsed, 4), Ok(Wait::Reached)));
                stream.shutdown(Shutdown::Write).expect("input EOF");
                assert!(matches!(case.try_wait(&closed, 1), Ok(Wait::Reached)));
                stream
            },
            recovery::Policy::new(|event| reclaimed.push(event), recover),
            native,
        )
        .expect("plain parse scenario");
    assert!(matches!(reclaimed.events(), Ok(events) if events.is_empty()));
    let events = events.values().expect("plain parse events");
    assert!(
        matches!(
            &*events,
            [
                Event::Parse { len: 1, .. },
                Event::Parse { pointer: first, len: 2 },
                Event::Parse { pointer: retry, len: 2 },
                Event::Head(b'P'),
                Event::Parse { pointer: tail, len: 1 },
                Event::Head(b'H'),
                Event::Quarantined,
                Event::Closed(close::Reason::Remote)
            ] if first == retry && *tail != first.wrapping_add(2)
        ),
        "plain parse order: {events:?}"
    );
    assert_eq!(stream.read(&mut [0]).expect("wait for close"), 0);
}
