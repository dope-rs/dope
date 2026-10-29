pub mod common;

use std::{
    convert::Infallible,
    io::{self, Read as _, Write as _},
    iter,
    time::Instant,
};

use common::{
    manifold::listener::defer::preamble::{self, FRAMES, HANDSHAKE, PREFIX},
    scenario::case::listener::Listener,
};
use dope_core::driver::{
    life::recovery,
    route::{FrameworkTag, KeyTag, Tag, kind},
};
use dope_manifold::{
    app::{env::Bundle, receive},
    network::listener::conn::write,
    timing::Throughput,
};
use dope_net::tcp::Tcp;
use dope_runtime::executor::session::Completion;
use dope_test::{
    fibers::{self, Wait},
    harness::{
        GUARD,
        peer::Peer,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

#[test]
fn eof_resumes_early_frame() {
    let mut finish = Completion::new();
    let admitted = Gate::new();
    let wrote = Gate::new();
    let eof = Gate::new();
    let closed = Gate::new();
    let observe = {
        let admitted = admitted.clone();
        let wrote = wrote.clone();
        let eof = eof.clone();
        move |event: preamble::Event<'_, '_>| match event {
            preamble::Event::Write(result) => {
                result.expect("preamble wire write");
                wrote.hit();
            }
            preamble::Event::Input { expected, actual } => assert_eq!(actual, expected),
            preamble::Event::Consumed { expected, actual } => assert_eq!(actual, expected),
            preamble::Event::Enqueued(result) => {
                assert!(matches!(result, Ok(write::Outcome::Committed(()))));
                admitted.hit();
            }
            preamble::Event::Chunk(bytes) => assert!(!bytes.is_empty()),
            preamble::Event::Eof(end) => {
                assert!(matches!(end, receive::End::Read));
                eof.hit();
            }
        }
    };
    let prepared = Listener::new::<1>(Default::default())
        .direct(1)
        .prepare::<KeyTag<0>, _, Bundle<Tcp, preamble::Preamble<_>, Throughput>>((
            preamble::App {
                frames: &FRAMES[..1],
                gate: closed.clone(),
                observe: observe.clone(),
            },
            (observe, true),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                let mut peer = Peer::at(case.addr()).try_connect().expect("connect peer");
                assert_eq!(
                    case.try_wait(&admitted, 1).expect("admitted early frame"),
                    Wait::Reached
                );
                assert_eq!(wrote.hits(), 0);
                peer.write_all(HANDSHAKE).expect("final wire input");
                assert_eq!(
                    case.try_wait(&closed, 1).expect("flushed EOF close"),
                    Wait::Reached
                );
                assert_eq!(eof.hits(), 1);
                let want: Vec<_> = iter::once(PREFIX)
                    .chain(FRAMES[0].iter().copied())
                    .collect();
                peer.set_nonblocking(true).expect("nonblocking peer");
                let started = Instant::now();
                let mut reply = Vec::new();
                let mut bytes = [0; 64];
                loop {
                    assert!(started.elapsed() < GUARD, "peer did not observe EOF");
                    match peer.read(&mut bytes) {
                        Ok(0) => break,
                        Ok(count) => reply.extend_from_slice(&bytes[..count]),
                        Err(error) => {
                            assert!(
                                matches!(
                                    error.kind(),
                                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                                ),
                                "read peer EOF: {error}"
                            );
                            case.try_drive(fibers::Yield::new())
                                .expect("drive peer EOF");
                        }
                    }
                }
                assert_eq!(reply, want);
            },
            recovery::Policy::new(
                |event| reports.push(event),
                |failure: recovery::Recovery<'_, '_>| -> Infallible {
                    panic!("native recovery: {:?}", failure.cause())
                },
            ),
            |outcome| outcome.expect("native lifecycle"),
        )
        .expect("listener run");
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
