pub mod common;

use common::native;

use rustls::StreamOwned;
use std::{
    io::{self, Read as _, Write as _},
    os::unix::net::UnixStream,
    time::Duration,
};

#[test]
fn close_keeps_reads() {
    let (client, server) = native::try_half_closed().expect("native half-close");
    let (left, right) = UnixStream::pair().expect("native socket pair");
    for socket in [&left, &right] {
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .expect("read timeout");
        socket
            .set_write_timeout(Some(Duration::from_secs(3)))
            .expect("write timeout");
    }
    let mut client = StreamOwned::new(client, left);
    let mut server = StreamOwned::new(server, right);
    server.write_all(b"after close").expect("peer response");
    server.flush().expect("response output");
    let mut response = [0; 11];
    client.read_exact(&mut response).expect("response input");
    assert_eq!(&response, b"after close");
    assert!(client.conn.wants_read());
}

#[test]
#[ignore = "Rustls 0.23.46 asserts !sent_fatal_alert after local close_notify; run with --release --include-ignored"]
fn malformed_after_close() {
    let (client, server) = native::try_half_closed().expect("native half-close");
    let (left, right) = UnixStream::pair().expect("native socket pair");
    for socket in [&left, &right] {
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .expect("read timeout");
        socket
            .set_write_timeout(Some(Duration::from_secs(3)))
            .expect("write timeout");
    }
    let mut client = StreamOwned::new(client, left);
    let mut server = StreamOwned::new(server, right);
    server
        .sock
        .write_all(&[0xff, 3, 3, 0, 0])
        .expect("invalid peer record");
    let error = client.read(&mut [0; 1]).expect_err("invalid TLS record");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(matches!(
        error
            .get_ref()
            .and_then(|cause| cause.downcast_ref::<rustls::Error>()),
        Some(rustls::Error::InvalidMessage(
            rustls::InvalidMessage::InvalidContentType
        ))
    ));
}
