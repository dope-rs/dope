use std::{convert::Infallible, net, task::Poll, time::Duration};

use dope_core::driver::{
    life::{creation::Attempt, recovery},
    settings,
};
use dope_fiber::{BlockOn as _, abi};
use dope_runtime::executor::{Build as _, drive, session::Completion};
use dope_test::harness::{
    observe::Trace,
    peer::{AcceptScript, Error, Peer, TcpAcceptor, TcpScript},
    scenario::process::Deadline,
};

#[test]
fn cancel_precedes_ready() {
    for nowait in [false, true] {
        AcceptScript::with(
            move |listener| {
                let stream = listener.accept().expect("accept");
                stream.write_all(&[1]).expect("ready");
                assert!(matches!(
                    listener.accept_for(Duration::from_secs(1)),
                    Err(Error::Cancelled)
                ));
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
            },
        )
        .expect("script");
    }
}

#[test]
fn native_is_not_cancelled() {
    Deadline::with(Duration::from_secs(5), |_| {
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
                let mut create = Attempt::new();
                let executor = peer
                    .completion()
                    .try_build(&mut create, settings::Config::default())
                    .map_err(|error| format!("{:?}", error.kind()))
                    .expect("executor");
                let mut finish = Completion::new();
                executor.run(|executor| {
                    executor
                        .enter_with(
                            |mut session| {
                                let trace = Trace::<_, 0>::new();
                                session
                                    .with_app(&mut finish, &trace, |mut app| {
                                        let result = app.block_on(abi::poll_fn(|_| {
                                            socket.shutdown(net::Shutdown::Write).expect("EOF");
                                            Poll::<()>::Pending
                                        }));
                                        assert!(matches!(result, Err(drive::Error::Shutdown)));
                                    })
                                    .expect("application cleanup");
                            },
                            recovery::Policy::new(
                                drop,
                                |failure: recovery::Recovery<'_, '_>| -> Infallible {
                                    panic!("native recovery: {:?}", failure.cause());
                                },
                            ),
                        )
                        .as_ref()
                        .map_err(|error| error.cause())
                        .expect("executor completion");
                });
                peer.finish().expect("finish");
            },
        )
    })
    .expect("watchdog")
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
