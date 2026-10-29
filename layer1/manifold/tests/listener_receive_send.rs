pub mod common;

use std::{
    convert::Infallible,
    fmt::Debug,
    io::{self, Read, Write},
    net::Shutdown,
    rc::Rc,
    time::Duration,
};

use common::{
    manifold::listener::coupled::{App, BODY_LEN, DynamicApp, DynamicEvent, Event, State},
    scenario::case::listener::Listener,
};
use dope_core::{
    driver::{
        life::recovery,
        route::{FrameworkTag, KeyTag, Tag, kind},
    },
    io::socket::option,
    platform::wake,
};
use dope_manifold::{
    app::{env::Bundle, receive},
    network::listener::conn::write,
    timing::Throughput,
};
use dope_net::{link::egress, tcp, wire::Identity};
use dope_runtime::{executor::session::Completion, topology::error::Kind};
use dope_test::{
    fibers::{Run, Wait},
    harness::{
        observe::Trace,
        peer::Peer,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

fn coupled(event: Event<'_>) {
    assert!(match event {
        Event::Prefix(state, capacity) =>
            matches!(state, State::Second) && capacity == write::Prefix::CAPACITY,
        Event::Written(result) => matches!(result, Ok(write::Outcome::Committed(()))),
    });
}

fn gated(event: DynamicEvent<'_>) {
    assert!(match event {
        DynamicEvent::Chunk(phase, completions) => phase == 0 || (phase == 2 && completions > 0),
        DynamicEvent::Prefix(phase, capacity) => phase == 1 && capacity == write::Prefix::CAPACITY,
        DynamicEvent::Written(result) => matches!(result, Ok(write::Outcome::Committed(()))),
        DynamicEvent::Drained(phase) => phase == 1,
    });
}

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
fn send_replays_receive() {
    let mut finish = Completion::new();
    for direct in [0, 1] {
        let setting = option::Buffer::try_from(1024_usize);
        let setting = setting.expect("socket buffer size");
        let opened = Gate::new();
        let first = Gate::new();
        let second = Gate::new();
        let closed = Gate::new();
        let seen = Rc::new(Trace::new());
        let (wait, next) = wake::Ends::blocking().expect("input release pipe").split();
        let (done, sent) = wake::Ends::blocking().expect("peer write pipe").split();
        let (start, read) = wake::Ends::blocking().expect("read release pipe").split();
        let prepared = Listener::<receive::Coupled>::new::<4>(Default::default())
            .direct(direct)
            .egress(egress::Config::shared(egress::Credit::fixed::<
                4,
                { BODY_LEN as u32 },
            >()))
            .stream(tcp::StreamConfig {
                send_buffer_size: Some(setting),
                no_delay: Some(option::Switch::On),
                ..Default::default()
            })
            .prepare::<KeyTag<0>, _, Bundle<tcp::Tcp, Identity, Throughput>>((
                App {
                    state: State::First(
                        egress::NonEmpty::try_new(State::try_body().expect("segmented body"))
                            .expect("nonempty body"),
                    ),
                    opened: opened.clone(),
                    first: first.clone(),
                    second: second.clone(),
                    closed: closed.clone(),
                    seen: seen.clone(),
                    observe: coupled,
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
                        .with(
                            move |socket| -> io::Result<Vec<u8>> {
                                socket.write_all(b"A")?;
                                assert!(wait.wait().is_ok(), "release second coupled byte");
                                socket.write_all(b"B")?;
                                socket.shutdown(Shutdown::Write)?;
                                assert!(sent.notify().is_ok(), "signal coupled peer write");
                                assert!(start.wait().is_ok(), "release coupled peer read");
                                let mut body = Vec::new();
                                socket.read_to_end(&mut body)?;
                                Ok(body)
                            },
                            |peer| {
                                reached(case.try_wait(&first, 1), first.hits(), 1);
                                assert!(next.notify().is_ok(), "release coupled input");
                                assert!(
                                    done.wait_timeout(Duration::from_secs(1)).is_ok(),
                                    "observe coupled peer write"
                                );
                                case.try_pause(Duration::from_millis(100))
                                    .expect("drive pause");
                                assert_eq!(
                                    &*seen.values().expect("coupled observations"),
                                    &[b"A".to_vec()],
                                    "a fresh completion overtook the live send owner"
                                );

                                assert!(read.notify().is_ok(), "release coupled response read");
                                reached(case.try_wait(&second, 1), second.hits(), 1);
                                reached(case.try_wait(&closed, 1), closed.hits(), 1);
                                let body = peer
                                    .finish()
                                    .expect("finish client peer")
                                    .expect("client peer failed");
                                assert_eq!(body.len(), BODY_LEN);
                                assert!(body.iter().all(|byte| *byte == 0x5a));
                            },
                        )
                        .expect("spawn coupled peer");
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                native,
            )
            .expect("listener run");
        assert_eq!(
            (opened.hits(), first.hits(), second.hits(), closed.hits()),
            (1, 1, 1, 1)
        );
        assert_eq!(
            &*seen.values().expect("coupled observations"),
            &[b"A".to_vec(), b"B".to_vec()]
        );
        assert!(
            reports.events().expect("reclaimed events").is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );
    }
}

#[test]
fn gate_drains() {
    let mut finish = Completion::new();
    let setting = option::Buffer::try_from(1024_usize);
    let setting = setting.expect("socket buffer size");
    let first = Gate::new();
    let prefix = Gate::new();
    let drain = Gate::new();
    let second = Gate::new();
    let closed = Gate::new();
    let seen = Rc::new(Trace::new());
    let (wait, next) = wake::Ends::blocking().expect("input release pipe").split();
    let (start, read) = wake::Ends::blocking().expect("read release pipe").split();
    let prepared = Listener::<receive::Gated>::new::<4>(Default::default())
        .direct(0)
        .stream(tcp::StreamConfig {
            send_buffer_size: Some(setting),
            no_delay: Some(option::Switch::On),
            ..Default::default()
        })
        .prepare::<KeyTag<0>, _, Bundle<tcp::Tcp, Identity, Throughput>>((
            DynamicApp {
                first: first.clone(),
                prefix: prefix.clone(),
                drained: drain.clone(),
                second: second.clone(),
                closed: closed.clone(),
                seen: seen.clone(),
                observe: gated,
            },
            Identity,
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            move |case| {
                Peer::at(case.addr())
                    .with(
                        move |socket| -> io::Result<Vec<u8>> {
                            socket.write_all(b"A")?;
                            assert!(wait.wait().is_ok(), "release second gated byte");
                            socket.write_all(b"B")?;
                            socket.shutdown(Shutdown::Write)?;
                            assert!(start.wait().is_ok(), "release gated peer read");
                            let mut body = Vec::new();
                            socket.read_to_end(&mut body)?;
                            Ok(body)
                        },
                        |peer| {
                            reached(case.try_wait(&first, 1), first.hits(), 1);
                            assert!(next.notify().is_ok(), "release gated input");
                            reached(case.try_wait(&prefix, 1), prefix.hits(), 1);
                            case.try_pause(Duration::from_millis(100))
                                .expect("drive pause");
                            assert_eq!(
                                &*seen.values().expect("gated observations"),
                                &[b"A".to_vec()]
                            );
                            assert_eq!(drain.hits(), 0, "prefix progress forged a drain proof");

                            assert!(read.notify().is_ok(), "release gated response read");
                            reached(case.try_wait(&drain, 1), drain.hits(), 1);
                            reached(case.try_wait(&second, 1), second.hits(), 1);
                            reached(case.try_wait(&closed, 1), closed.hits(), 1);
                            let body = peer
                                .finish()
                                .expect("finish client peer")
                                .expect("client peer failed");
                            assert_eq!(body.len(), BODY_LEN);
                            assert!(body.iter().all(|byte| *byte == 0x6b));
                            assert_eq!(
                                &*seen.values().expect("gated observations"),
                                &[b"A".to_vec(), b"B".to_vec()]
                            );
                        },
                    )
                    .expect("spawn gated peer");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
