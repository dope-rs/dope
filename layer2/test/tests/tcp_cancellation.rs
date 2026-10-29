use std::{
    io::{self, Read as _, Write as _},
    net,
    time::Duration,
};

use dope_core::platform::wake;
use dope_test::harness::peer::{AcceptScript, Peer, Stream, TcpAcceptor, TcpScript, udp};

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
};

#[test]
fn cancel_precedes_ready() {
    for nowait in [false, true] {
        let (wait, release) = wake::Ends::blocking().expect("gate").split();
        AcceptScript::with(
            move |listener| {
                let mut stream = listener.accept().expect("accept");
                stream.write_all(&[1]).expect("ready");
                wait.wait_timeout(Duration::from_secs(1)).expect("release");
                stream.nonblock(nowait);
                let mut byte = [0];
                for result in [
                    stream.read(&mut byte),
                    stream.read_vectored(&mut [io::IoSliceMut::new(&mut byte)]),
                    stream.write(&[1]),
                    stream.write_vectored(&[io::IoSlice::new(&[1])]),
                ] {
                    assert_eq!(
                        result.expect_err("cancelled").kind(),
                        io::ErrorKind::ConnectionAborted
                    );
                    assert!(stream.is_stopped());
                }
                assert_eq!(stream.read(&mut []).expect("empty read"), 0);
                assert_eq!(stream.write(&[]).expect("empty write"), 0);
            },
            |peer| {
                let mut socket = Peer::at(peer.addr()).try_connect().expect("connect");
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
            assert_eq!(
                stream.read(&mut [0]).expect_err("not ready").kind(),
                io::ErrorKind::WouldBlock
            );
            assert!(!stream.is_stopped());
            stream.nonblock(false);
            stream
                .read_wait(Some(Duration::from_millis(1)))
                .expect("timeout");
            assert_eq!(
                stream.read(&mut [0]).expect_err("deadline").kind(),
                io::ErrorKind::TimedOut
            );
            assert!(!stream.is_stopped());
            stream.write_all(&[1]).expect("ready for EOF");
            stream
                .read_wait(Some(Duration::from_secs(1)))
                .expect("EOF timeout");
            assert_eq!(stream.read(&mut [0]).expect("EOF"), 0);
            assert!(!stream.is_stopped());
        },
        |peer| {
            let mut socket = Peer::at(peer.addr()).try_connect().expect("connect");
            socket.read_exact(&mut [0]).expect("worker ready");
            socket.shutdown(net::Shutdown::Write).expect("EOF");
            peer.finish().expect("finish");
        },
    )
    .expect("script");
}
