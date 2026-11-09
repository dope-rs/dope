pub mod common;

use common::manifold::listener::{native, reached};

use std::{
    net::Shutdown,
    rc::Rc,
    time::{Duration, Instant},
};

use common::{
    manifold::listener::coupled::{App, BODY_LEN, BorrowedApp, BorrowedWire, DynamicApp, State},
    recover,
    scenario::case::listener::Listener,
};
use dope_core::{
    driver::{
        life::recovery,
        route::{FrameworkTag, KeyTag, Tag, kind},
    },
    io::socket::option,
};
use dope_manifold::{
    app::{env::Bundle, receive},
    timing::Throughput,
};
use dope_net::{link::egress, tcp, wire::Identity};
use dope_runtime::executor::session::Completion;
use dope_test::{
    fibers,
    harness::{
        GUARD,
        observe::Trace,
        peer::{self, Peer},
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

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
                    let peer = Peer::at(case.addr()).try_connect().expect("connect peer");
                    peer.set_nodelay(true).expect("peer nodelay");
                    peer.write_wait(Some(GUARD)).expect("peer write timeout");
                    peer.write_all(b"A").expect("first input");
                    reached(case.try_wait(&first, 1), first.hits(), 1);
                    peer.write_all(b"B").expect("second input");
                    peer.shutdown(Shutdown::Write).expect("peer half-close");
                    case.try_pause(Duration::from_millis(100))
                        .expect("drive pause");
                    assert_eq!(
                        &*seen.values().expect("coupled observations"),
                        &[b"A".to_vec()],
                        "a fresh completion overtook the live send owner"
                    );
                    peer.nonblock(true);
                    let started = Instant::now();
                    let mut received = 0;
                    let mut bytes = [0; 4096];
                    loop {
                        assert!(started.elapsed() < GUARD, "peer drain timeout");
                        match peer.read(&mut bytes) {
                            Ok(0) => break,
                            Ok(count) => {
                                assert!(bytes[..count].iter().all(|byte| *byte == 0x5a));
                                received += count;
                            }
                            Err(error) => {
                                assert!(
                                    matches!(error, peer::Error::Syscall(error)
                                        if error.is_block() || error.is_intr()),
                                    "read peer: {error:?}"
                                );
                                case.try_drive(fibers::Yield::new()).expect("drive peer");
                            }
                        }
                    }
                    assert_eq!(received, BODY_LEN);
                    reached(case.try_wait(&second, 1), second.hits(), 1);
                    reached(case.try_wait(&closed, 1), closed.hits(), 1);
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
fn gated_drain_and_independent_receive() {
    for gate in [true, false] {
        receive_gated::<true>(gate);
    }
}

#[test]
fn gated_receive_keeps_send() {
    receive_gated::<false>(true);
}

fn receive_gated<const DRAIN: bool>(gate: bool) {
    let mut finish = Completion::new();
    let setting = option::Buffer::try_from(1024_usize);
    let setting = setting.expect("socket buffer size");
    let first = Gate::new();
    let prefix = Gate::new();
    let drain = Gate::new();
    let second = Gate::new();
    let closed = Gate::new();
    let seen = Rc::new(Trace::new());
    let prepared = Listener::<receive::GatedScoped>::new::<4>(Default::default())
        .direct(0)
        .stream(tcp::StreamConfig {
            send_buffer_size: Some(setting),
            no_delay: Some(option::Switch::On),
            ..Default::default()
        })
        .prepare::<KeyTag<0>, _, Bundle<tcp::Tcp, Identity, Throughput>>((
            DynamicApp::<DRAIN> {
                gated: gate,
                first: first.clone(),
                prefix: prefix.clone(),
                drained: drain.clone(),
                second: second.clone(),
                closed: closed.clone(),
                seen: seen.clone(),
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
                let peer = Peer::at(case.addr()).try_connect().expect("connect peer");
                peer.set_nodelay(true).expect("peer nodelay");
                peer.write_wait(Some(GUARD)).expect("peer write timeout");
                peer.write_all(b"A").expect("first input");
                reached(case.try_wait(&first, 1), first.hits(), 1);
                peer.write_all(b"B").expect("second input");
                reached(case.try_wait(&prefix, 1), prefix.hits(), 1);
                case.try_pause(Duration::from_millis(100))
                    .expect("drive pause");
                if !gate {
                    reached(case.try_wait(&second, 1), second.hits(), 1);
                }
                assert_eq!(
                    &*seen.values().expect("gated observations"),
                    &if gate {
                        vec![b"A".to_vec()]
                    } else {
                        vec![b"A".to_vec(), b"B".to_vec()]
                    }
                );
                assert_eq!(drain.hits(), 0, "prefix progress forged a drain proof");
                peer.nonblock(true);
                let started = Instant::now();
                let mut received = 0;
                let mut bytes = [0; 4096];
                loop {
                    assert!(started.elapsed() < GUARD, "peer drain timeout");
                    match peer.read(&mut bytes) {
                        Ok(0) => break,
                        Ok(count) => {
                            assert!(bytes[..count].iter().all(|byte| *byte == 0x6b));
                            received += count;
                        }
                        Err(error) => {
                            assert!(
                                matches!(error, peer::Error::Syscall(error)
                                    if error.is_block() || error.is_intr()),
                                "read peer: {error:?}"
                            );
                            case.try_drive(fibers::Yield::new()).expect("drive peer");
                        }
                    }
                }
                assert_eq!(received, BODY_LEN);
                reached(case.try_wait(&drain, 1), drain.hits(), 1);
                reached(case.try_wait(&second, 1), second.hits(), 1);
                reached(case.try_wait(&closed, 1), closed.hits(), 1);
                assert_eq!(
                    &*seen.values().expect("gated observations"),
                    &if gate {
                        vec![b"A".to_vec(), b"A".to_vec(), b"B".to_vec()]
                    } else {
                        vec![b"A".to_vec(), b"B".to_vec()]
                    }
                );
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

#[test]
fn borrowed_reservation_tracks_native_send() {
    let mut finish = Completion::new();
    let setting = option::Buffer::try_from(1024_usize).expect("socket buffer size");
    let first = Gate::new();
    let second = Gate::new();
    let sent = Gate::new();
    let closed = Gate::new();
    let seen = Rc::new(Trace::new());
    let written = Rc::new(Trace::new());
    let released = Rc::new(Trace::new());
    let reservations = Rc::new(Trace::new());
    let prepared = Listener::<receive::Borrowed>::new::<4>(Default::default())
        .direct(0)
        .stream(tcp::StreamConfig {
            send_buffer_size: Some(setting),
            no_delay: Some(option::Switch::On),
            ..Default::default()
        })
        .prepare::<KeyTag<0>, _, Bundle<tcp::Tcp, BorrowedWire, Throughput>>((
            BorrowedApp {
                first: first.clone(),
                second: second.clone(),
                sent: sent.clone(),
                closed: closed.clone(),
                seen: Rc::clone(&seen),
                written: Rc::clone(&written),
                released: Rc::clone(&released),
            },
            BorrowedWire(Rc::clone(&reservations)),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                let peer = Peer::at(case.addr()).try_connect().expect("connect peer");
                peer.set_nodelay(true).expect("peer nodelay");
                peer.write_wait(Some(GUARD)).expect("peer write timeout");
                peer.write_all(b"A").expect("first input");
                reached(case.try_wait(&first, 1), first.hits(), 1);
                assert_eq!(&*written.values().expect("write result"), &[true]);
                case.try_pause(Duration::from_millis(100))
                    .expect("drive blocked send");
                peer.write_all(b"B").expect("second input");
                reached(case.try_wait(&second, 1), second.hits(), 1);
                assert_eq!(
                    &*reservations.values().expect("receive reservations"),
                    &[true, false],
                    "native send storage was exposed during its retained flight"
                );
                assert_eq!(sent.hits(), 0, "peer has not drained the output");
                peer.nonblock(true);
                let started = Instant::now();
                let mut received = 0;
                let mut bytes = [0; 4096];
                while received < BODY_LEN {
                    assert!(started.elapsed() < GUARD, "peer drain timeout");
                    let remaining = bytes.len().min(BODY_LEN - received);
                    match peer.read(&mut bytes[..remaining]) {
                        Ok(count) => {
                            assert_ne!(count, 0, "truncated reply");
                            assert!(bytes[..count].iter().all(|byte| *byte == 0x6b));
                            received += count;
                        }
                        Err(error) => {
                            assert!(
                                matches!(error, peer::Error::Syscall(error)
                                    if error.is_block() || error.is_intr()),
                                "read peer: {error:?}"
                            );
                            case.try_drive(fibers::Yield::new()).expect("drive peer");
                        }
                    }
                }
                reached(case.try_wait(&sent, 1), sent.hits(), 1);
                assert_eq!(
                    &*released.values().expect("send release"),
                    &[true],
                    "only the exact completed payload may release the next input"
                );
                peer.nonblock(false);
                peer.write_all(b"C").expect("completed input");
                peer.shutdown(Shutdown::Write).expect("peer half-close");
                reached(case.try_wait(&closed, 1), closed.hits(), 1);
                peer.nonblock(true);
                let started = Instant::now();
                loop {
                    assert!(started.elapsed() < GUARD, "peer EOF timeout");
                    match peer.read(&mut bytes) {
                        Ok(count) => {
                            assert_eq!(count, 0, "unexpected reply tail");
                            break;
                        }
                        Err(error) => {
                            assert!(
                                matches!(error, peer::Error::Syscall(error)
                                    if error.is_block() || error.is_intr()),
                                "read peer EOF: {error:?}"
                            );
                            case.try_drive(fibers::Yield::new())
                                .expect("drive peer EOF");
                        }
                    }
                }
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert_eq!(&*seen.values().expect("borrowed input"), b"ABC");
    assert_eq!(
        &*reservations.values().expect("receive reservations"),
        &[true, false, true]
    );
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
