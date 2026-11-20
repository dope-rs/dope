pub mod common;

use std::{iter, time::Instant};

use common::{
    manifold::listener::defer::preamble::{self, FRAMES, HANDSHAKE, PREFIX},
    scenario::case::listener::Listener,
};
use dope_core::driver::route::KeyTag;
use dope_manifold::{
    app::{env::Bundle, receive},
    timing::Throughput,
};
use dope_net::tcp::Tcp;
use dope_test::{
    fibers::{self, Wait},
    harness::{
        GUARD,
        peer::{self, Peer},
        signals::{Gate, Progress as _},
    },
};

#[test]
fn eof_resumes_early_frame() {
    let admitted = Gate::new();
    let wrote = Gate::new();
    let eof = Gate::new();
    let closed = Gate::new();
    let observe = {
        let admitted = admitted.clone();
        let wrote = wrote.clone();
        let eof = eof.clone();
        move |event: preamble::Event<'_>| match event {
            preamble::Event::Write(result) => {
                result.expect("preamble wire write");
                wrote.hit();
            }
            preamble::Event::Input { expected, actual } => assert_eq!(actual, expected),
            preamble::Event::Consumed { expected, actual } => assert_eq!(actual, expected),
            preamble::Event::Enqueued(result) => {
                assert!(matches!(result, Ok(())));
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
            preamble::Runtime(observe, true),
        ))
        .expect("listener layout");
    prepared.run(|case| {
        let peer = Peer::at(case.addr()).try_connect().expect("connect peer");
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
        peer.nonblock(true);
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
                        matches!(error, peer::Error::Syscall(error)
                            if error.is_block() || error.is_intr()),
                        "read peer EOF: {error:?}"
                    );
                    case.try_drive(fibers::Yield::new())
                        .expect("drive peer EOF");
                }
            }
        }
        assert_eq!(reply, want);
    });
}
