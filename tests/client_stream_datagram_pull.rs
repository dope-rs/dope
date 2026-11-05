pub mod common;

use common::{
    peer,
    receive::client::{PREFIX, RESET, Received, STOPPED, SUFFIX, State},
    recovery,
    retry::{
        BIND, ENDPOINT, Events,
        traces::Immediate,
        writable::{Callback, InputControl, Peer, Pull},
    },
    suite::{mux::dispatch::Capture, peers::tls},
};
use dope::{
    core::driver::{
        life::{creation::Attempt, recovery::Policy},
        route::Id,
        schedule::clock,
    },
    runtime::{
        control,
        executor::{Executor, session},
        shutdown,
    },
};
use dope_quic::{
    client::{self, EndpointSpec, build, protocol::Stream},
    conn::{Epoch, life::options::Options},
    proto::param,
    runtime::{
        endpoint::{client::Plan, config::Config},
        receive,
    },
};
use dope_test::harness::scenario::TIMEOUT;
use o3::buffer::Residency as _;
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    pin::Pin,
    rc::Rc,
    time::Duration,
};

#[test]
fn mixed_pull_preserves_fragments_and_independent_readiness() {
    let (signing, public) = peer::raw(0x7e).expect("identity");
    let server = signing.server().expect("server TLS");
    let params = param::Set::builder()
        .idle_ms(30_000)
        .datagram(Some(1200))
        .initial_data(1024)
        .local_bidi(1024)
        .remote_bidi(1024)
        .bidi_count(2)
        .try_build()
        .expect("parameters");
    let mut peers: [Peer; 2] = std::array::from_fn(|_| {
        Peer::new(
            server.clone(),
            Options::try_from(params.clone()).expect("peer options"),
        )
        .expect("peer")
    });
    let addresses = peers
        .each_ref()
        .map(|peer| peer.addr().expect("peer address"));
    let plan = Plan::<1, receive::Adaptive>::new(
        BIND,
        Config {
            max_conns: 2,
            ..ENDPOINT
        },
    )
    .expect("client plan");
    let (source, stop) = shutdown::Pair::new().expect("shutdown pair").split();
    let mut create = Attempt::new();
    let executor = Executor::new(
        &mut create,
        Events::settings(16, plan.as_ref()).expect("driver claims"),
    );
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("driver");
    let executor = executor
        .ok()
        .expect("driver admission")
        .shutdown(source)
        .expect("shutdown source");
    let observed = Rc::new(RefCell::new(State::default()));
    let connected = Cell::new(0u8);
    let completed = Cell::new(false);
    let retry = Immediate::default();
    let mut reports = Vec::new();
    executor.run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    let (client, timeout, next) = {
                        let mut driver = session.driver();
                        let now = driver.driver_ref().clock().now();
                        let timeout = now
                            .try_add(Duration::from_secs(2))
                            .expect("test deadline");
                        let authority = tls::client(public).expect("client TLS");
                        let endpoints = addresses
                            .into_iter()
                            .map(|addr| EndpointSpec {
                                addr,
                                authority: authority.try_clone().expect("authority reference"),
                            })
                            .collect();
                        let client = build::Builder::owned(
                            endpoints,
                            Options::try_from(params).expect("client options"),
                            Received::<_, true> {
                                state: Rc::clone(&observed),
                                active: None,
                                observe: |event: Result<client::SlotId, Callback<'_>>| {
                                    let slot = event.expect("only connect callbacks in pull mode");
                                    let bit = 1 << slot.index();
                                    assert_eq!(connected.get() & bit, 0, "one connect per source");
                                    connected.set(connected.get() | bit);
                                },
                            },
                            retry.clone(),
                            client::Config {
                                event_budget: 1,
                                retry_budget: 1,
                            },
                        )
                        .bind(plan, &mut driver)
                        .expect("client endpoint");
                        (client, timeout, now.into())
                    };
                    let mut phase = 0;
                    let mut handles = None;
                    let mut attempts = [None; 2];
                    let mut source = None;
                    let mut held = None;
                    let mut owner = None;
                    let mut terminals = 0;
                    let mut stop = Some(stop);
                    let mut completion = session::Completion::new();
                    session
                        .with_quiesce(
                            &mut completion,
                            Pull {
                                client,
                                next,
                                poll: |client: &mut InputControl<'_, '_, '_, Received<_, true>>,
                                       clock: clock::Clock<'_, '_>,
                                       next: &mut clock::Deadline<'_>| {
                                    if completed.get() {
                                        return control::Flow::Idle;
                                    }
                                    let now = clock.now();
                                    assert!(!timeout.is_due(now), "mixed pull phase {phase}");
                                    *next = timeout;
                                    if client.protocol().active.is_none() {
                                        assert!(client.at(clock).recv_stream().expect("pending stream").is_none());
                                        assert!(client.at(clock).recv_datagram().expect("pending datagram").is_none());
                                    }
                                    if matches!(phase, 0..=2 | 6 | 7) {
                                        for peer in &mut peers {
                                            peer.poll(now.into_raw()).expect("peer progress");
                                        }
                                    }
                                    match phase {
                                        0 => {
                                            let current = peers.each_ref().map(|peer| {
                                                peer.observations().borrow().established.first().copied()
                                            });
                                            if let [Some(readable), Some(datagram)] = current
                                                && connected.get() == 3
                                                && peers.iter().zip([readable, datagram]).all(|(peer, handle)| {
                                                    peer.router.inspect(handle, |_, _, conn| conn.inspection().tx_queued()) == Some(0)
                                                        && peer.router.conn(handle).expect("peer retained").view().recovery().unacked(Epoch::Application) == 0
                                                })
                                            {
                                                for _ in 0..4 {
                                                    match client.at(clock).recv_stream().expect("initial stream credit") {
                                                        Some((_, Stream::Writable)) => {}
                                                        None => break,
                                                        Some(_) => panic!("payload before admission"),
                                                    }
                                                }
                                                assert!(client.at(clock).recv_stream().expect("drained stream credit").is_none());
                                                assert!(client.at(clock).recv_datagram().expect("initial datagram queue").is_none());
                                                for data in [b"first".as_slice(), b"queued".as_slice()] {
                                                    Capture::send(&mut peers[1].router, datagram, data.to_vec(), now.into_raw()).expect("datagram admission");
                                                }
                                                handles = Some([readable, datagram]);
                                                phase = 1;
                                            }
                                        }
                                        1 => {
                                            let [handle, datagram] = handles.expect("source owners");
                                            if peers[1].router.inspect(datagram, |_, _, conn| conn.inspection().tx_queued()) == Some(0)
                                                && peers[1].router.conn(datagram).expect("datagram source").view().recovery().unacked(Epoch::Application) == 0
                                            {
                                                // Queue the datagram-only source before the stream-only source.
                                                let peer = &mut peers[0];
                                                let stream = Pin::new(&mut peer.router).observe(now.into_raw(), |mut turn| {
                                                    turn.protocol().with_conn(handle, |mut conn| {
                                                        let mut streams = conn.streams();
                                                        let mut send = streams.send();
                                                        let stream = send.open_bidi().expect("stream admission");
                                                        send.send(stream.send(), PREFIX).expect("prefix send");
                                                        stream.id()
                                                    }).expect("peer connection")
                                                });
                                                for flight in Capture::output(&mut peer.router, now.into_raw()).expect("prefix output") {
                                                    peer.send(flight).expect("prefix packet");
                                                }
                                                // A second output turn keeps the two native receive fragments distinct.
                                                Pin::new(&mut peer.router).observe(now.into_raw(), |mut turn| {
                                                    turn.protocol().with_conn(handle, |mut conn| {
                                                        let mut streams = conn.streams();
                                                        let stream = streams.view().resolve_send(stream).expect("same stream");
                                                        let mut send = streams.send();
                                                        send.send(stream, SUFFIX).expect("suffix send");
                                                        send.finish(stream).expect("receive EOF");
                                                    }).expect("peer connection");
                                                });
                                                for flight in Capture::output(&mut peer.router, now.into_raw()).expect("suffix output") {
                                                    peer.send(flight).expect("suffix packet");
                                                }
                                                source = Some((handle, stream));
                                                phase = 2;
                                            }
                                        }
                                        2 | 7 => {
                                            if peers.iter().zip(handles.expect("source owners")).all(|(peer, handle)| {
                                                peer.router.inspect(handle, |_, _, conn| conn.inspection().tx_queued()) == Some(0)
                                                    && peer.router.conn(handle).expect("peer retained").view().recovery().unacked(Epoch::Application) == 0
                                            }) {
                                                // Subsequent pulls have no new peer packets or periodic wakeup.
                                                phase += 1;
                                            }
                                        }
                                        3 => {
                                            for _ in 0..4 {
                                                if held.is_none() {
                                                    match client.at(clock).recv_stream().expect("stream access") {
                                                        Some((attempt, Stream::Readable { id, data: Some(data), end })) => {
                                                            assert_eq!(attempt.slot().index(), 0);
                                                            assert_eq!(id, source.expect("source owner").1);
                                                            assert_eq!(data.as_ref(), PREFIX);
                                                            assert!(!end, "suffix remains in the native queue");
                                                            attempts[0] = Some(attempt);
                                                            owner = Some((data.as_ref().as_ptr(), data.resident()));
                                                            held = Some(data);
                                                        }
                                                        Some((_, Stream::Writable)) | None => {}
                                                        Some(_) => panic!("expected prefix"),
                                                    }
                                                }
                                                if attempts[1].is_none()
                                                    && let Some((attempt, data)) = client.at(clock).recv_datagram().expect("datagram access")
                                                {
                                                    assert_eq!(attempt.slot().index(), 1);
                                                    assert_eq!(data.as_ref(), b"first");
                                                    attempts[1] = Some(attempt);
                                                }
                                                if held.is_some() && attempts[1].is_some() {
                                                    break;
                                                }
                                            }
                                            assert!(held.is_some() && attempts[1].is_some(), "opposite-kind sources must progress within four pull pairs");
                                            phase = 4;
                                        }
                                        4 => {
                                            let mut suffix = false;
                                            for _ in 0..4 {
                                                match client.at(clock).recv_stream().expect("independent stream access") {
                                                    Some((attempt, Stream::Readable { id, data: Some(data), end })) => {
                                                        assert_eq!(Some(attempt), attempts[0]);
                                                        assert_eq!(id, source.expect("source owner").1);
                                                        assert_eq!(data.as_ref(), SUFFIX);
                                                        assert!(end, "last fragment carries receive EOF");
                                                        suffix = true;
                                                        break;
                                                    }
                                                    Some((_, Stream::Writable)) | None => {}
                                                    Some(_) => panic!("expected suffix"),
                                                }
                                            }
                                            assert!(suffix, "stream must progress while the other source's datagram remains unconsumed");
                                            phase = 5;
                                        }
                                        5 => {
                                            let (attempt, data) = client.at(clock).recv_datagram().expect("datagram access").expect("retained second datagram");
                                            assert_eq!(Some(attempt), attempts[1]);
                                            assert_eq!(data.as_ref(), b"queued");
                                            assert!(client.at(clock).recv_datagram().expect("empty datagram queue").is_none());
                                            phase = 6;
                                        }
                                        6 => {
                                            let (handle, _) = source.expect("source owner");
                                            let peer = &mut peers[0];
                                            let terminal = Pin::new(&mut peer.router).observe(now.into_raw(), |mut turn| {
                                                turn.protocol().with_conn(handle, |mut conn| {
                                                    let mut streams = conn.streams();
                                                    let mut send = streams.send();
                                                    let terminal = send.open_bidi().expect("terminal stream");
                                                    send.reset(terminal.send(), RESET[0]).expect("reset send");
                                                    streams.receive().stop(terminal.receive(), STOPPED[0]).expect("stop send");
                                                    terminal.id()
                                                }).expect("peer connection")
                                            });
                                            Capture::send(&mut peer.router, handle, b"last".to_vec(), now.into_raw()).expect("last datagram");
                                            source = Some((handle, terminal));
                                            phase = 7;
                                        }
                                        8 => {
                                            let (attempt, input) = client.at(clock).recv_stream().expect("terminal access").expect("retained terminal");
                                            assert_eq!(Some(attempt), attempts[0]);
                                            let bit = match input {
                                                Stream::Reset { id, error_code } => {
                                                    assert_eq!(id, source.expect("source owner").1);
                                                    assert_eq!(error_code, RESET[0]);
                                                    1
                                                }
                                                Stream::Stopped { id, error_code } => {
                                                    assert_eq!(id, source.expect("source owner").1);
                                                    assert_eq!(error_code, STOPPED[0]);
                                                    2
                                                }
                                                Stream::Writable => return control::Flow::Idle,
                                                Stream::Readable { .. } => panic!("terminal preserves its exact kind"),
                                            };
                                            assert_eq!(terminals & bit, 0, "one event for each terminal");
                                            terminals |= bit;
                                            if terminals == 3 {
                                                phase = 9;
                                            }
                                        }
                                        9 => {
                                            assert!(client.at(clock).recv_stream().expect("empty stream queue").is_none());
                                            phase = 10;
                                        }
                                        10 => {
                                            let (attempt, data) = client.at(clock).recv_datagram().expect("last datagram access").expect("stream drain preserves datagram readiness");
                                            assert_eq!(Some(attempt), attempts[0]);
                                            assert_eq!(data.as_ref(), b"last");
                                            client.seal();
                                            client.at(clock).close_all();
                                            let prefix = held.take().expect("original retained owner");
                                            assert_eq!(prefix.as_ref(), PREFIX);
                                            assert_eq!(Some((prefix.as_ref().as_ptr(), prefix.resident())), owner);
                                            drop(prefix);
                                            completed.set(true);
                                            stop.take().expect("shutdown owner").fire().expect("shutdown signal");
                                        }
                                        _ => unreachable!("test phase"),
                                    }
                                    if matches!(phase, 0..=2 | 6 | 7) {
                                        *next = now.try_add(Duration::from_millis(1)).expect("peer poll");
                                    }
                                    control::Flow::Idle
                                },
                            },
                            TIMEOUT,
                            |app| app.run_for(TIMEOUT).map(|_| ()),
                        )
                        .expect("session completion")
                        .expect("application completion");
                },
                Policy::new(
                    |event| reports.push(event),
                    |failure: dope::core::driver::life::recovery::Recovery<'_, '_>| -> Infallible {
                        panic!("native recovery: {:?}", failure.cause())
                    },
                ),
            )
            .as_ref()
            .map_err(|cause| cause.cause())
            .expect("driver retirement");
    });
    assert!(completed.get());
    assert!(retry.0.get().is_none());
    let observed = observed.borrow();
    assert_eq!(connected.get(), 3);
    assert!(observed.bytes.is_empty() && observed.ends.is_empty() && observed.terminals.is_empty());
    recovery::Teardown::new([Id::fixed::<1>()])
        .verify(reports)
        .expect("receive retirement");
}
