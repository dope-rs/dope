use std::{io, mem};

use dope_test::harness::peer::udp::Script;

#[test]
fn receive_after_stop() {
    Script::with(
        |socket| {
            for _ in 0..2 {
                let error = socket.recv_from(&mut [0]).expect_err("cancelled receive");
                assert_eq!(error.kind(), io::ErrorKind::Interrupted);
            }
        },
        |peer| peer.stop().expect("cancelled script completes"),
    )
    .expect("UDP script");
}

#[test]
fn forgotten_view() {
    Script::with(
        |socket| {
            let error = socket.recv_from(&mut [0]).expect_err("cancelled receive");
            assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        },
        |peer| mem::forget(peer),
    )
    .expect("forgotten peer remains owned");
}
