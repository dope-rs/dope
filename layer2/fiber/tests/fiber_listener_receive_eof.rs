use std::{
    convert::Infallible,
    io::{self, Read as _, Write as _},
    net::Shutdown,
};

use dope::{
    core::{
        driver::{
            life::recovery,
            route::{KeyTag, table::conn::Capacity},
        },
        io::socket::Backlog,
        platform::wake,
    },
    manifold::{network::listener::Domain, timing::Balanced},
    net::{
        link::event::SendFailure,
        tcp::Tcp,
        wire::{Identity, domain},
    },
    runtime::executor::session::Completion,
};
use dope_fiber::{
    BlockOn as _,
    net::{
        error::{
            Write,
            read::{Read, Transmit},
        },
        server::{Config, Listener, Plan},
    },
};
use dope_test::harness::{peer::Peer, scenario};

pub mod common;

use common::{
    network::{
        reserve::Reservation,
        rounds::{client::Collect as _, listener::App},
    },
    runtime::Setup,
};

#[test]
fn writes_after_eof() {
    let held = Reservation::bind().expect("listener address");
    let addr = held.addr();
    let plan = Plan::<Identity>::new(Capacity::fixed::<1>(), Identity);
    let mut report = None;
    Setup::try_for_plan::<Balanced, _>(&plan)
        .expect("driver configuration")
        .with_input(
            plan,
            |mut sess| {
                let mut finish = Completion::new();
                let hash = sess.hash_state(Domain::DEFAULT);
                let storage = sess
                    .storage()
                    .try_build(sess.driver().storage)
                    .expect("listener port");
                let result = Listener::<KeyTag<0>, Tcp, Identity, _>::adopt(
                    &storage,
                    Config {
                        direct_flights: 0.into(),
                        bind: addr,
                        backlog: Backlog::fixed::<1>(),
                        stream: Default::default(),
                        transport: Default::default(),
                        egress: Default::default(),
                        bound: held.into_tcp(),
                        wire: domain::Local::new(Identity),
                    },
                    |cause| cause.eliminate(),
                    hash,
                    &mut sess,
                    |sess| sess.driver(),
                    |listener, sess| {
                        let (reset, release) = wake::Ends::blocking().expect("reset gate").split();
                        Peer::at(addr)
                            .with(
                                move |peer| {
                                    peer.write_all(b"tail").expect("peer tail");
                                    peer.shutdown(Shutdown::Write).expect("peer EOF");
                                    let mut response = [0; 2];
                                    peer.read_exact(&mut response).expect("reply after EOF");
                                    assert_eq!(&response, b"ok");
                                    reset.wait_timeout(scenario::TIMEOUT).expect("reply observed");
                                    peer.reset_on_close().expect("reset on close");
                                },
                                |peer| {
                                    let handle = storage.handle();
                                    sess.with_quiesce(
                                        &mut finish,
                                        App { pool: listener },
                                        scenario::TIMEOUT,
                                        |mut app| {
                                            app.block_on(dope_fiber::fiber!('_ => async move {
                                                let mut io = handle.accept().await.ok().expect("accept");
                                                let mut tail = Vec::new();
                                                while let Some(chunk) = io.read().await.ok().expect("receive") {
                                                    tail.extend_from_slice(&chunk.try_into_vec().expect("consume tail"));
                                                }
                                                assert_eq!(tail, b"tail");
                                                assert!(matches!(io.read().await, Ok(None)));
                                                io.write_all(b"o").await.ok().expect("first write after EOF");
                                                io.write_all(b"k").await.ok().expect("second write after EOF");
                                                release.notify().expect("release reset");
                                                peer.finish().expect("peer reset completed");
                                                assert!(matches!(io.write_all(b"x").await,
                                                    Err(Write::ConnectionFailed { payload: Some(payload) })
                                                        if payload.as_ref() == b"x"
                                                ));
                                                assert!(matches!(io.read().await,
                                                    Err(Read::Transmit(Transmit::Io(SendFailure::Queued(error))))
                                                        if matches!(io::Error::from(error).kind(),
                                                            io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset)
                                                ));
                                                assert!(matches!(io.read().await, Ok(None)));
                                            }))
                                        },
                                    )
                                    .expect("listener lifecycle")
                                    .expect("listener progress");
                                },
                            )
                            .expect("peer worker");
                    },
                );
                assert!(result.is_ok(), "listener installation");
            },
            recovery::Policy::new(
                |event| {
                    report.get_or_insert(event);
                },
                |failure: recovery::Recovery<'_, '_>| -> Infallible {
                    panic!("native recovery: {:?}", failure.cause())
                },
            ),
            |result| result.expect("driver lifecycle"),
        );
    assert!(report.is_none(), "unexpected reclamation: {report:?}");
}
