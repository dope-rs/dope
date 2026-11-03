pub mod common;

use std::{
    fmt::Debug,
    io::{Read as _, Write as _},
    rc::Rc,
    time::Duration,
};

use dope_core::driver::{
    life::recovery,
    route::{Id, KeyTag},
};
use dope_manifold::{app::env::Bundle, client::close, timing::Balanced};
use dope_net::tcp::Tcp;
use dope_runtime::{executor::session::Completion, topology::error::Kind};
use dope_test::{
    fibers::Wait,
    harness::{observe::Trace, peer, signals::Gate},
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
        peer::TcpScript::with(
            |stream| {
                stream
                    .write_all(b"H")
                    .expect("write head with wire failure");
                assert_eq!(stream.read(&mut [0]).expect("wait for close"), 0);
            },
            |peer| {
                let events = Rc::new(Trace::new());
                let closed = Gate::new();
                let connector = Connector::<1>::new(
                    peer.addr(),
                    Duration::from_millis(10),
                    Duration::from_millis(50),
                );
                let connector = if matches!(mode, Mode::Timeout) {
                    connector.lifetime(Positive::from_millis::<100>())
                } else {
                    connector
                };
                let planned = connector
                    .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Fault, Balanced>>(Value::new(
                        Plaintext {
                            mode,
                            events: events.clone(),
                            closed: closed.clone(),
                        },
                    ))
                    .expect("prepare terminal client");
                let mut reclaimed = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);
                let result = planned.with_run(
                    &mut finish,
                    |case| {
                        let result = case.try_wait(&closed, 1);
                        assert!(
                            matches!(result, Ok(Wait::Reached)),
                            "{mode:?}: {result:?}; {:?}",
                            events.values().expect("terminal events")
                        );
                    },
                    recovery::Policy::new(|event| reclaimed.push(event), recover),
                    native,
                );
                assert!(result.is_ok(), "{mode:?}: {result:?}");
                assert!(
                    matches!(reclaimed.events(), Ok(events) if events.is_empty()),
                    "unexpected reclaimed operations: {reclaimed:?}"
                );
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
                peer.finish().expect("finish terminal plaintext peer");
            },
        )
        .expect("spawn terminal plaintext peer");
    }
}

fn native<S: Debug, T: Debug>(result: Result<T, Kind<'_, S, &T>>) -> T {
    result.expect("native lifecycle")
}
