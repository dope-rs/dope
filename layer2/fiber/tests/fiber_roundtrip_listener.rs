use dope::{
    core::driver::{life::recovery, storage::Value},
    manifold::network::listener::Domain,
    runtime::executor::{Executor, session::Completion},
};
use dope_core::{driver::route::KeyTag, platform::wake};
use dope_test::harness::scenario;
pub mod common;

use std::{
    convert::Infallible,
    io::{Error, ErrorKind, Read as _, Write as _},
    time::Duration,
};

use dope::{
    core::{
        driver::{
            route::{FrameworkTag, Tag, kind, table::conn::Capacity},
            settings::claim::Needs,
        },
        io::socket::Backlog,
    },
    manifold::timing::Balanced,
    net::{
        tcp::Tcp,
        wire::{Identity, domain},
    },
    runtime::executor::drive,
};
use dope_fiber::{
    BlockOn as _,
    net::{
        error::read::Read,
        read::Lease,
        server::{Config, Listener, Plan},
    },
};
use dope_test::{fibers, harness::peer::Peer, reclaim::Reports};
use o3::buffer::Source as _;

use crate::common::network::{reserve::Reservation, rounds::listener::*};

#[test]
fn fiber_listener_roundtrip() {
    let mut finish = Completion::new();
    let held = (Reservation::bind()).expect("listener address must be reserved");
    let addr = held.addr();
    let plan = Plan::<Identity>::new(Capacity::fixed::<MAX_CONN>(), Identity);
    let config = (Needs::from_tree(&plan).and_then(|needs| needs.resolve::<Balanced>()))
        .expect("listener driver requirements must resolve");
    use dope::core::driver::life::creation::Attempt;
    let mut create = Attempt::new();
    let result = Executor::new(&mut create, config);
    result
        .as_ref()
        .map_err(|error| error.kind())
        .expect("listener executor must initialize");
    let Ok(exec) = result else {
        return;
    };
    let mut reports = Reports::<_>::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    exec
            .with_plan(Value::new(plan)).run(|executor| {
    let result = executor.enter_with(move |mut sess| {
                let hash = sess.hash_state(Domain::DEFAULT);
                let plan = sess.storage();
                let storage = (plan.try_build(sess.driver().storage)).expect("listener port must initialize");
                let result = Listener::<KeyTag<0>, Tcp, Identity, _>::adopt(
                    &storage,
                    Config {
                        direct_flights: 0.into(),
                        bind: addr,
                        backlog: Backlog::fixed::<16>(),
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
                        let pool = storage.handle();
                        let (sent, reset) = wake::Ends::blocking().expect("reset release").split();
                        Peer::at(addr)
                            .with(move |stream| {
                                stream
                                    .write_all(b"abcdefg")
                                    .expect("client prefix must be written");
                                let mut ack = [0; 1];
                                stream
                                    .read_exact(&mut ack)
                                    .expect("client acknowledgement must be read");
                                stream
                                    .write_all(b"xyz")
                                    .expect("client tail must be written");
                                let mut reply = [0; 4];
                                stream
                                    .read_exact(&mut reply)
                                    .expect("client reply must be read");
                                sent.wait_timeout(scenario::TIMEOUT)
                                    .expect("server send must finish before reset");
                                stream
                                    .reset_on_close()
                                    .expect("client must reset the connection on close");
                                (ack, reply)
                            }, |client| {
                        let app = App { pool: listener };
                        let driven = (sess.with_quiesce(&mut finish, app, scenario::TIMEOUT, |mut app| -> Result<(), drive::Error> {
                                let accepted = app.block_on(pool.accept())?;
                                let stream = (accepted).ok().expect("listener must accept the peer");
                                let mut stream = Some(stream);
                                app.block_on(dope_fiber::fiber!('_ => async move {
                                    let mut stream = stream.take().expect("stream must be owned");
                                    let mut retained: Option<Lease<'_, '_, '_, Identity>> = None;
                                    let mut prefix = Vec::with_capacity(5);
                                    while prefix.len() < 5 {
                                        if retained.as_ref().is_none_or(|lease| lease.is_empty()) {
                                            if let Some(exhausted) = retained.take() {
                                                assert!(exhausted.is_empty());
                                            }
                                            let read = stream.read().await;
                                            retained = (read).ok().expect("prefix read must succeed");
                                        }
                                        let lease = retained
                                            .as_mut()
                                            .expect("prefix read must retain a lease");
                                        lease.try_append(&mut prefix, 5).expect("consume received prefix");
                                    }
                                    let mut tail = Vec::with_capacity(2);
                                    while tail.len() < 2 {
                                        if retained.as_ref().is_none_or(|lease| lease.is_empty()) {
                                            if let Some(exhausted) = retained.take() {
                                                assert!(exhausted.is_empty());
                                            }
                                            let read = stream.read().await;
                                            retained = (read).ok().expect("tail read must succeed");
                                        }
                                        let lease = retained
                                            .as_mut()
                                            .expect("tail read must retain a lease");
                                        lease.try_append(&mut tail, 2).expect("consume received prefix");
                                    }
                                    {
                                        let exhausted = retained
                                            .take()
                                            .expect("prefix and tail must retain the final lease");
                                        assert!(exhausted.is_empty());
                                    }
                                    let output = fibers::TEST.poll_once(stream.write_all(b"discarded")).await;
                                    assert!(output.is_none());
                                    let sent = stream.write_all(b"!").await;
                                    (sent).ok().expect("acknowledgement write must succeed");
                                    let mut retained: Option<Lease<'_, '_, '_, Identity>> = None;
                                    let mut reused = Vec::with_capacity(3);
                                    while reused.len() < 3 {
                                        if retained.as_ref().is_none_or(|lease| lease.is_empty()) {
                                            if let Some(exhausted) = retained.take() {
                                                assert!(exhausted.is_empty());
                                            }
                                            let read = stream.read().await;
                                            retained = (read).ok().expect("reused stream read must succeed");
                                        }
                                        let lease = retained
                                            .as_mut()
                                            .expect("reused stream read must retain a lease");
                                        lease.try_append(&mut reused, 3).expect("consume received prefix");
                                    }
                                    {
                                        let exhausted = retained
                                            .take()
                                            .expect("reused read must retain the final lease");
                                        assert!(exhausted.is_empty());
                                    }
                                    let sent = stream.write_all(b"pong").await;
                                    (sent).ok().expect("reply write must succeed");
                                    reset.notify().expect("client must await reset");
                                    assert!(matches!(
                                        stream.read().await,
                                        Err(Read::Transport(error))
                                            if Error::from(error).kind()
                                                == ErrorKind::ConnectionReset
                                    ));
                                    assert_eq!(prefix, b"abcde");
                                    assert_eq!(tail, b"fg");
                                    assert_eq!(reused, b"xyz");
                                }))
                            })).expect("listener test application must finish");
                        ((driven).expect("listener fiber stalled"));
                        assert_eq!(
                            client.finish().expect("listener peer must finish"),
                            (*b"!", *b"pong")
                        );
                        }).expect("listener peer must start");
                    },
                );
                assert!(result.is_ok(), "listener pool must be adopted");
            }, recovery::Policy::new(|event| reports.push(event), recover));
        assert!(
            reports.events().expect("reclamation observations").is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );

        assert!(result.is_ok(), "listener driver must finish");
        });
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

#[test]
fn cancelled_inflight_reuses_slot() {
    use std::{net::Shutdown, pin::pin};

    use dope::net::{link::event::SendFailure, tcp::StreamConfig};
    use dope_fiber::{abi::Task, net::error::read::Transmit};

    use crate::common::runtime::Setup;

    for drop_first in [false, true] {
        let payload = vec![b'x'; 1 << 20];
        let held = Reservation::bind().expect("listener address");
        let addr = held.addr();
        let plan = Plan::<Identity>::new(Capacity::fixed::<1>(), Identity);
        let mut report = None;
        Setup::try_for_plan::<Balanced, _>(&plan)
            .expect("driver configuration")
            .with_input(
                (plan, payload.as_slice()),
                |mut sess| {
                    let mut finish = Completion::new();
                    let hash = sess.hash_state(Domain::DEFAULT);
                    let (plan, payload) = sess.storage();
                    let payload = *payload;
                    let storage = plan
                        .try_build(sess.driver().storage)
                        .expect("listener port");
                    let result = Listener::<KeyTag<0>, Tcp, Identity, _>::adopt(
                        &storage,
                        Config {
                            direct_flights: 0.into(),
                            bind: addr,
                            backlog: Backlog::fixed::<1>(),
                            stream: StreamConfig {
                                send_buffer_size: Some(4096usize.try_into().expect("send buffer")),
                                ..Default::default()
                            },
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
                            let handle = storage.handle();
                            sess.with_quiesce(
                                &mut finish,
                                App { pool: listener },
                                scenario::TIMEOUT,
                                |mut app| -> Result<(), drive::Error> {
                                    let (entered, sent) = wake::Ends::blocking().expect("send gate").split();
                                    let (reset, release) = wake::Ends::blocking().expect("reset gate").split();
                                    let old = Peer::at(addr).with(
                                        move |peer| {
                                            peer.shutdown(Shutdown::Write).expect("peer EOF");
                                            let mut byte = [0];
                                            peer.read_exact(&mut byte).expect("submitted payload");
                                            assert_eq!(byte, [b'x']);
                                            sent.notify().expect("native send observed");
                                            reset.wait_timeout(scenario::TIMEOUT).expect("write cancelled");
                                            peer.reset_on_close().expect("reset on close");
                                        },
                                        |peer| {
                                            let mut io = Some(app.block_on(handle.accept())?.ok().expect("first accept"));
                                            assert!(matches!(app.block_on(io.as_mut().expect("first owner").read())?, Ok(None)));
                                            {
                                                let mut write = pin!(Task::new(io.as_mut().expect("first owner").write_all(payload)));
                                                assert!(app.block_on(fibers::TEST.poll_once(write.as_mut()))?.is_none());
                                                fibers::TEST.try_run(&mut app, dope_fiber::fiber!('_ => async move {
                                                    loop {
                                                        match entered.wait_timeout(Duration::ZERO) {
                                                            Ok(()) => break,
                                                            Err(wake::WaitError::Timeout { .. }) => fibers::Yield::new().await,
                                                            Err(error) => panic!("native send gate: {error:?}"),
                                                        }
                                                    }
                                                })).expect("native send progress");
                                                assert!(app.block_on(fibers::TEST.poll_once(write.as_mut()))?.is_none(), "the backend must still own the incomplete send");
                                            }
                                            if drop_first {
                                                drop(io.take());
                                            }
                                            release.notify().expect("release peer reset");
                                            peer.finish().expect("peer reset completed");
                                            if let Some(io) = io.as_mut() {
                                                fibers::TEST.try_run(&mut app, dope_fiber::fiber!('_ => async move {
                                                    loop {
                                                        match io.read().await {
                                                            Err(Read::Transmit(Transmit::Io(SendFailure::Queued(error)))) => {
                                                                assert!(matches!(Error::from(error).kind(), ErrorKind::BrokenPipe | ErrorKind::ConnectionReset));
                                                                break;
                                                            }
                                                            Ok(None) => fibers::Yield::new().await,
                                                            _ => panic!("cancelled send must report its native failure"),
                                                        }
                                                    }
                                                })).expect("late send failure");
                                            }
                                            Ok::<_, drive::Error>(io)
                                        },
                                    ).expect("first peer worker")?;
                                    Peer::at(addr).with(
                                        |peer| {
                                            let mut response = [0; 2];
                                            peer.read_exact(&mut response).expect("replacement reply");
                                            assert_eq!(&response, b"ok");
                                        },
                                        |peer| -> Result<(), drive::Error> {
                                            let mut io = app.block_on(handle.accept())?.ok().expect("capacity one must accept again");
                                            drop(old);
                                            app.block_on(io.write_all(b"ok"))?.ok().expect("replacement write");
                                            drop(io);
                                            peer.finish().expect("replacement peer");
                                            Ok(())
                                        },
                                    ).expect("replacement peer worker")?;
                                    Ok(())
                                },
                            )
                            .expect("listener lifecycle")
                            .expect("listener progress");
                        },
                    );
                    assert!(result.is_ok(), "listener installation");
                },
                recovery::Policy::new(
                    |event| {
                        report.get_or_insert(event);
                    },
                    recover,
                ),
                |result| result.expect("driver lifecycle"),
            );
        assert!(report.is_none(), "unexpected reclamation: {report:?}");
    }
}
