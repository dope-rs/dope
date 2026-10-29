pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    io::{Read as _, Write as _},
    net::Shutdown,
    rc::Rc,
    time::Duration,
};

use common::{
    manifold::listener::terminal::{
        Observation, eof,
        wire::{self, EofOnData, Event, Wire},
    },
    scenario::case::listener::Listener,
};
use dope_core::driver::{
    life::recovery,
    route::{FrameworkTag, KeyTag, Tag, kind},
};
use dope_manifold::{
    app::{env::Bundle, receive},
    timing::Throughput,
};
use dope_net::tcp::Tcp;
use dope_runtime::executor::session::Completion;
use dope_test::{
    fibers::Wait,
    harness::{
        GUARD,
        peer::Peer,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

fn terminal(event: Observation<'_>) {
    use dope_manifold::network::listener::conn::{
        deadline::Arm,
        write::{self, body::Admit},
    };
    assert!(match event {
        Observation::Deadline(armed) => matches!(armed, Arm::Armed),
        Observation::Write(result) => matches!(result, Ok(write::Outcome::Committed(()))),
        Observation::Owned(result) => matches!(result, Admit::Committed),
        Observation::Replay(result) => matches!(result, Some(Ok(()))),
        Observation::Failure(_) => false,
    });
}

#[test]
fn eof_holds_tail() {
    let mut finish = Completion::new();
    let ready = Gate::new();
    let release = Gate::new();
    let eof = Gate::new();
    let close = Gate::new();
    let done = Gate::new();
    let end = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events)));
    let prepared = Listener::<receive::Scoped>::new::<1>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire<EofOnData>, Throughput>>((
            eof::Probe {
                ready: ready.clone(),
                release: release.clone(),
                eof: eof.clone(),
                finish: end.clone(),
                close: close.clone(),
                retired: done.clone(),
                events: Rc::clone(&events),
                observe: terminal,
            },
            Rc::clone(&wire),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                let mut peer = Peer::at(case.addr()).try_read(GUARD).expect("connect peer");
                peer.write_all(b"x").expect("write request");
                assert_eq!(
                    case.try_wait(&ready, 1).expect("initial scoped input"),
                    Wait::Reached,
                    "initial scoped input"
                );
                case.try_pause(Duration::from_millis(20))
                    .expect("settle pending send");
                let mut before = [0; 6];
                peer.read_exact(&mut before).expect("read before replay");
                assert_eq!(&before, b"before");
                assert_eq!(eof.hits(), 0, "pending plaintext must precede EOF");
                assert_eq!(close.hits(), 0);

                peer.shutdown(Shutdown::Write).expect("late native FIN");
                case.try_pause(Duration::from_millis(20))
                    .expect("settle late FIN");
                assert_eq!(
                    eof.hits(),
                    0,
                    "late control must preserve the plaintext tail"
                );
                assert_eq!(wire.eof_count(), 0, "authenticated EOF owns classification");
                assert_eq!(close.hits(), 0);

                release.hit();
                assert_eq!(
                    case.try_wait(&eof, 1).expect("EOF after tail replay"),
                    Wait::Reached,
                    "EOF after tail replay"
                );
                case.try_pause(Duration::from_millis(20))
                    .expect("settle live output");
                let mut after = [0; 5];
                peer.read_exact(&mut after).expect("read after EOF");
                assert_eq!(&after, b"after");
                assert_eq!(eof.hits(), 1);
                assert_eq!(wire.eof_count(), 0);
                assert_eq!(close.hits(), 0, "Hold preserves the write direction");

                end.hit();
                assert_eq!(
                    case.try_wait(&close, 1)
                        .expect("explicit application close"),
                    Wait::Reached,
                    "explicit application close"
                );
                assert_eq!(
                    case.try_wait(&done, 1).expect("native retirement"),
                    Wait::Reached,
                    "native retirement"
                );
                assert!(
                    Peer::try_read_all(&peer)
                        .expect("explicit close")
                        .is_empty()
                );
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            |outcome| outcome.expect("native lifecycle"),
        )
        .expect("listener run");
    assert!(wire.writes.take().iter().all(Result::is_ok));
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    assert_eq!(eof.hits(), 1);
    assert_eq!(wire.eof_count(), 0);
    assert_eq!(close.hits(), 1);
    let events = events.take();
    let owners: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::Generation(lane, epoch) => Some((*lane, *epoch)),
            _ => None,
        })
        .collect();
    assert_eq!(owners.len(), 3);
    assert!(owners.iter().all(|owner| *owner == owners[0]));
    assert!(
        events
            .into_iter()
            .filter(|event| !matches!(event, Event::Generation(_, _)))
            .collect::<Vec<_>>()
            == [
                Event::Deferred(b"x".to_vec()),
                Event::Chunk(b"x".to_vec()),
                Event::Eof,
                Event::Close,
                Event::Retired,
            ]
    );
}

#[test]
fn eof_flushes_control() {
    let mut finish = Completion::new();
    let eof = Gate::new();
    let close = Gate::new();
    let done = Gate::new();
    let end = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events)));
    let prepared = Listener::<receive::Scoped>::new::<1>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire<EofOnData<true>>, Throughput>>((
            eof::Probe::<_, true> {
                ready: Gate::new(),
                release: Gate::new(),
                eof: eof.clone(),
                finish: end.clone(),
                close: close.clone(),
                retired: done.clone(),
                events: Rc::clone(&events),
                observe: |event: Observation<'_>| {
                    assert!(
                        !matches!(event, Observation::Write(_)),
                        "wire control must not rely on application writes"
                    );
                    terminal(event);
                },
            },
            Rc::clone(&wire),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                let mut peer = Peer::at(case.addr()).try_read(GUARD).expect("connect peer");
                peer.write_all(b"x").expect("write request");
                assert_eq!(case.try_wait(&eof, 1).expect("wire EOF"), Wait::Reached);
                case.try_pause(Duration::from_millis(20))
                    .expect("flush wire control");
                let mut control = [0; 1];
                peer.read_exact(&mut control).expect("control while Hold");
                assert_eq!(&control, b"!");
                assert_eq!(close.hits(), 0, "control must precede explicit close");
                assert_eq!(done.hits(), 0);
                end.hit();
                assert_eq!(case.try_wait(&done, 1).expect("retirement"), Wait::Reached);
                assert!(Peer::try_read_all(&peer).expect("peer EOF").is_empty());
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            |outcome| outcome.expect("native lifecycle"),
        )
        .expect("listener run");
    assert!(matches!(wire.writes.take().as_slice(), [Ok(())]));
    assert_eq!(wire.eof_count(), 0);
    assert_eq!(eof.hits(), 1);
    assert_eq!(close.hits(), 1);
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    assert!(
        events
            .take()
            .into_iter()
            .filter(|event| !matches!(event, Event::Generation(_, _)))
            .collect::<Vec<_>>()
            == [
                Event::Chunk(b"x".to_vec()),
                Event::Eof,
                Event::ControlPrepared,
                Event::ControlSent,
                Event::Close,
                Event::Retired,
            ]
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
