use std::{net, time::Duration};

use dope_core::platform::wake;
use dope_test::harness::peer::{AcceptScript, Error, Peer, Stream, TcpAcceptor, TcpScript, udp};
use dope_test::harness::support::file;

trait MaybeSend<A> {}
impl<T: ?Sized> MaybeSend<()> for T {}
impl<T: ?Sized + Send> MaybeSend<u8> for T {}

trait MaybeSync<A> {}
impl<T: ?Sized> MaybeSync<()> for T {}
impl<T: ?Sized + Sync> MaybeSync<u8> for T {}

const _: fn() = || {
    fn confined<T: ?Sized + MaybeSend<A> + MaybeSync<B>, A, B>() {}
    confined::<Stream<'static>, _, _>();
    confined::<TcpAcceptor<'static>, _, _>();
    confined::<udp::Socket<'static>, _, _>();
    confined::<file::Directory<fn(Result<(), file::Error>)>, _, _>();
};

#[test]
fn cancel_precedes_ready() {
    for nowait in [false, true] {
        let (wait, release) = wake::Ends::blocking().expect("gate").split();
        AcceptScript::with(
            move |listener| {
                let stream = listener.accept().expect("accept");
                stream.write_all(&[1]).expect("ready");
                wait.wait_timeout(Duration::from_secs(1)).expect("release");
                stream.nonblock(nowait);
                let mut byte = [0];
                for result in [
                    stream.read(&mut byte),
                    stream.write(&[1]),
                    stream.write_vectored(&[&[1]]),
                ] {
                    assert!(matches!(result, Err(Error::Cancelled)));
                }
                assert_eq!(stream.read(&mut []).expect("empty read"), 0);
                assert_eq!(stream.write(&[]).expect("empty write"), 0);
                assert_eq!(
                    stream.write_vectored(&[&[], &[]]).expect("empty vectors"),
                    0
                );
            },
            |peer| {
                let socket = Peer::at(peer.addr()).try_connect().expect("connect");
                socket.read_exact(&mut [0]).expect("worker ready");
                socket.write_all(&[1]).expect("readable data");
                socket.shutdown(net::Shutdown::Write).expect("readable EOF");
                drop(peer);
                release.notify().expect("release");
            },
        )
        .expect("script");
    }
}

#[test]
fn native_is_not_cancelled() {
    TcpScript::with(
        |stream| {
            stream.nonblock(true);
            assert!(
                matches!(stream.read(&mut [0]), Err(Error::Syscall(error)) if error.is_block())
            );
            stream.nonblock(false);
            stream
                .read_wait(Some(Duration::from_millis(1)))
                .expect("timeout");
            assert!(matches!(stream.read(&mut [0]), Err(Error::TimedOut)));
            stream.write_all(&[1]).expect("ready for EOF");
            stream
                .read_wait(Some(Duration::from_secs(1)))
                .expect("EOF timeout");
            assert_eq!(stream.read(&mut [0]).expect("EOF"), 0);
        },
        |peer| {
            let socket = Peer::at(peer.addr()).try_connect().expect("connect");
            socket.read_exact(&mut [0]).expect("worker ready");
            socket.shutdown(net::Shutdown::Write).expect("EOF");
            peer.finish().expect("finish");
        },
    )
    .expect("script");
}

#[test]
fn standalone_accept_and_peek() {
    let listener = TcpAcceptor::bind((net::Ipv4Addr::LOCALHOST, 0).into()).expect("bind");
    assert!(listener.try_accept().expect("empty listener").is_none());
    let client = Peer::at(listener.local_addr().expect("address"))
        .try_connect()
        .expect("connect");
    let server = listener
        .accept_for(Duration::from_secs(1))
        .expect("accept")
        .expect("connected client");
    assert_eq!(
        client.write_vectored(&[b"pe", b"", b"ek"]).expect("write"),
        4
    );
    let mut first = [0];
    assert_eq!(server.peek(&mut first).expect("peek"), 1);
    assert_eq!(first, [b'p']);
    let mut bytes = [0; 4];
    server.read_exact(&mut bytes).expect("unconsumed input");
    assert_eq!(&bytes, b"peek");
}
