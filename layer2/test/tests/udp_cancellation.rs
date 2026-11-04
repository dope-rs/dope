use std::{mem, net::Ipv4Addr, time::Duration};

use dope_test::harness::peer::{
    Error,
    udp::{Script, Socket},
};

#[test]
fn receive_after_stop_or_forgotten_view() {
    for (forgotten, reads) in [(false, 2), (true, 1)] {
        Script::with(
            |socket| {
                for _ in 0..reads {
                    let error = socket.recv_from(&mut [0]).expect_err("cancelled receive");
                    assert!(matches!(error, Error::Cancelled));
                }
            },
            |peer| {
                if forgotten {
                    mem::forget(peer);
                } else {
                    peer.stop().expect("cancelled script completes");
                }
            },
        )
        .expect("UDP script");
    }
}

#[test]
fn standalone_datagram_and_timeout() {
    let sender = Socket::bind((Ipv4Addr::LOCALHOST, 0).into()).expect("sender");
    let receiver = Socket::bind((Ipv4Addr::LOCALHOST, 0).into()).expect("receiver");
    let address = receiver.local_addr().expect("destination");
    assert_eq!(sender.send_to(b"a", address).expect("send"), 1);
    let mut byte = [0];
    let (count, source) = receiver.recv_from(&mut byte).expect("receive");
    assert_eq!(
        (count, source, byte),
        (1, sender.local_addr().expect("source"), [b'a'])
    );
    sender.send_to(b"b", address).expect("second send");
    assert_eq!(receiver.recv(&mut byte).expect("receive payload"), 1);
    assert_eq!(byte, [b'b']);
    receiver.nonblock(true);
    assert!(matches!(receiver.recv(&mut byte), Err(Error::Syscall(error)) if error.is_block()));
    receiver.nonblock(false);
    receiver
        .read_wait(Some(Duration::from_millis(1)))
        .expect("timeout");
    assert!(matches!(receiver.recv(&mut byte), Err(Error::TimedOut)));
}
