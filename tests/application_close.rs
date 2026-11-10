use common::peer::Receiver;
pub mod common;

use common::peer::Pair;
use common::suite::mux::paths;
use common::suite::peers::path;
use dope::core::driver::schedule::clock;
use dope_quic::conn::life::status::phase::Close;
use dope_quic::conn::{Mutation, State, stream, wire::packet::Batch};
use dope_quic::errors::{Code, SendFailure};
use dope_quic::security::qkdf::Initial;
use dope_quic::wire::{
    frame::{Frame, View, close},
    packet::long::{Parsed, initial::Prefix},
};
use dope_test::alloc::Tracker;
use std::{
    pin::Pin,
    time::{Duration, Instant},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn terminal_releases_queued_payloads_and_preserves_held_credit() {
    use common::{peer::Scope as _, suite::peers::packet};
    use dope_quic::{
        proto::param,
        wire::{
            frame::{Borrowed, Boundary, Length},
            stream::Id,
            varint::VarInt,
        },
    };

    for remote in [false, true] {
        let now = Instant::now();
        let params = param::Set::builder()
            .initial_data(4096)
            .stream_uni(4096)
            .uni_count(3)
            .datagram(Some(2048))
            .try_build()
            .expect("bounded receive queues");
        let mut pair =
            packet::Pair::try_params(4, params, now, |_| {}).expect("authenticated packet fixture");
        let id = Id::new(2).expect("peer stream");
        pair.receive(&[0x0a, 2, 4, b'h', b'e', b'l', b'd'], now)
            .expect("held stream payload");
        let held = pair
            .server
            .recv()
            .hold(id)
            .expect("receive stream")
            .expect("held payload");
        let credit_id = Id::new(6).expect("peer credit stream");
        pair.receive(&[0x0a, 6, 6, b'c', b'r', b'e', b'd', b'i', b't'], now)
            .expect("credit payload");
        let (owned, credit) = pair
            .server
            .recv()
            .hold(credit_id)
            .expect("credit stream")
            .expect("credit payload")
            .into_parts();
        let credit = pair
            .server
            .release_conn(credit)
            .map_err(|error| error.kind())
            .expect("deferred stream credit");
        let payload = [0x53; 1024];
        let cached_id = Id::new(10).expect("peer completed stream");
        pair.receive(&[0x0b, 10, 1, b'c'], now)
            .expect("completed stream payload");
        let mut cached = Vec::with_capacity(payload.len());
        assert_eq!(pair.server.recv().recv(cached_id, &mut cached), Ok(1));
        for offset in [4u64, 2052] {
            let mut frames = Vec::new();
            Borrowed::stream(
                id,
                VarInt::try_from(offset).expect("stream offset"),
                Boundary::More,
                Length::Explicit,
                &payload,
            )
            .expect("bounded stream frame")
            .encode(&mut frames)
            .expect("encoded stream frame");
            pair.receive(&frames, now).expect("queued stream payload");
        }
        let mut datagram = vec![0x30];
        datagram.extend_from_slice(&payload);
        pair.receive(&datagram, now).expect("unread datagram");
        pair.server.with_scope(|mut scope| {
            scope
                .datagrams()
                .try_send(payload.to_vec())
                .expect("queued outbound datagram");
        });
        let mut close = pair.peer.packet(&[0x1d, 0, 0]).expect("peer CLOSE");
        let ((), activity) = Tracker::activity(|| {
            pair.server.with_scope(|mut scope| {
                if remote {
                    scope
                        .input(&mut pair.work)
                        .recv_packet(&mut close, now, |_| {})
                        .expect("authenticated CLOSE");
                } else {
                    scope.lifecycle().close(Code::ZERO, Vec::new());
                }
            });
        });
        assert!(
            activity.deallocated_bytes >= 5 * payload.len(),
            "{activity:?}"
        );
        assert_eq!(held.as_ref(), b"held");
        assert_eq!(owned, b"credit");
        assert_eq!(
            pair.server
                .release_held(held)
                .map_err(|error| error.kind())
                .expect("held epoch survives terminal cleanup"),
            b"held",
        );
        pair.server
            .release(credit)
            .map_err(|error| error.kind())
            .expect("detached credit survives terminal cleanup");
    }
}

#[test]
fn initial_close_floor() {
    let mut client = path::try_initial().expect("initial client");
    let now = Instant::now();
    let mut batch = Batch::default();
    client
        .with_scope(|mut scope| {
            scope
                .lifecycle()
                .close(Code::ZERO, b"application detail".to_vec());
            scope.output().send_batch(&mut batch, now, 1, 1_199)
        })
        .expect("insufficient capacity defers close");
    assert!(batch.is_empty());
    assert_eq!(batch.byte_len(), 0);
    assert!(client.view().lifecycle().is_closing());
    assert!(!client.view().lifecycle().is_closed());

    client
        .with_scope(|mut scope| scope.output().send_batch(&mut batch, now, 1, 1_200))
        .expect("minimum Initial capacity admits close");
    assert_eq!(batch.packets(), 1);
    let packet = batch.iter_mut().next().expect("Initial close packet");
    assert_eq!(packet.len(), 1_200);
    let prefix = Prefix::decode(packet).expect("Initial close header");
    let open = Initial::try_new(prefix.dcid.into_slice())
        .expect("initial derivation")
        .try_client()
        .expect("client key admission");
    let packet = match Parsed::parse(packet).expect("protected close packet") {
        Parsed::Initial(packet) => Some(packet),
        _ => None,
    }
    .expect("close retains Initial protection");
    let opened = packet.decrypt(&open, 0).expect("Initial close protection");
    let (frame, _) = Frame::decode_ref(opened.body()).expect("close frame");
    assert!(matches!(
        frame.view(),
        View::ConnectionClose {
            code: close::Code::Transport { error_code, .. },
            reason,
        } if error_code.get() == 0x0c && reason.is_empty()
    ));
    assert_eq!(client.view().lifecycle().state(), State::Closing);
    let deadline = client
        .view()
        .recovery()
        .next_timer()
        .expect("terminal deadline");
    client
        .with_scope(|mut scope| scope.recovery().try_loss(deadline))
        .expect("terminal expiry");
    assert!(client.view().lifecycle().is_closed());
}

#[test]
fn closes_application() {
    let (mut server, mut client, mut wire) =
        Pair::try_default().expect("connected peers").into_parts();
    clock::Scope::run(|clock| {
        let now = clock.now();
        let due = now.try_add(Duration::from_secs(1)).expect("deadline");
        client.with_scope(|mut scope| {
            assert!(matches!(
                scope.application().sync(Some(due)),
                Mutation::Replaced
            ));
            scope.application().ready();
            let stream = scope.streams().send().open_bidi().expect("open stream");
            scope
                .lifecycle()
                .close(Code::from_u16(401), b"expired".to_vec());
            assert!(!scope.view().lifecycle().is_closed());
            assert!(matches!(
                scope.streams().send().open_bidi(),
                Err(stream::Error::NotEstablished)
            ));
            assert_eq!(
                scope.streams().send().send(stream.send(), b"late"),
                Err(stream::Error::NotEstablished)
            );
            let data = b"late".to_vec();
            let storage = (data.as_ptr(), data.capacity());
            assert!(matches!(
                scope.datagrams().try_send(data),
                Err(SendFailure::Closed(data))
                    if (data.as_ptr(), data.capacity()) == storage && data == b"late"
            ));
            assert!(matches!(
                scope.application().sync(None),
                Mutation::Unchanged
            ));
            scope.application().ready();
            assert!(matches!(
                scope.application().sync(Some(due)),
                Mutation::Unchanged
            ));
        });
        Receiver::receive_from(&mut server, &mut wire, &mut client, now.into_raw())
            .expect("peer packet transfer");
        assert_eq!(client.view().lifecycle().state(), State::Closing);
        assert_eq!(server.view().lifecycle().state(), State::Draining);
        server.with_scope(|mut scope| scope.lifecycle().close(Code::ZERO, Vec::new()));
        assert!(
            matches!(server.view().lifecycle().close(), Some(Close::PeerApplication { error_code, reason: Ok(reason) }) if error_code.get() == 401 && reason == b"expired")
        );
        for conn in [&mut client, &mut server] {
            let deadline = conn
                .view()
                .recovery()
                .next_timer()
                .expect("terminal deadline");
            conn.with_scope(|mut scope| scope.recovery().try_loss(deadline))
                .expect("terminal expiry");
            assert!(conn.view().lifecycle().is_closed());
            assert!(conn.view().lifecycle().close().is_some());
        }
    });
}

#[test]
fn peer_close_retains_first_cause_even_when_storage_fails() {
    use common::suite::peers::packet;
    for (malformed, fail) in [(true, false), (false, true), (false, false)] {
        let now = Instant::now();
        let mut pair = packet::Pair::try_new(1, now, |_| {}).expect("connected peer");
        let mut frames = vec![0x1c, 7, 6, 0x50, 0];
        frames.resize(5 + 4096, 0x61);
        frames.extend_from_slice(&[0x1d, 9, 0]);
        if malformed {
            frames.push(0xff);
        }
        let mut packet = pair.peer.packet(&frames).expect("protected close");
        let mut receive = || {
            pair.server.with_scope(|mut scope| {
                scope
                    .input(&mut pair.work)
                    .recv_packet(&mut packet, now, |_| {})
            })
        };
        let received = if fail {
            Tracker::fail_after(0, receive)
        } else {
            receive()
        };
        if malformed {
            assert!(received.is_err());
            assert!(matches!(
                pair.server.view().lifecycle().close(),
                Some(Close::LocalTransport(_))
            ));
            continue;
        }
        received.expect("peer close commits despite retention failure");
        let Some(Close::PeerTransport {
            error_code,
            frame_type,
            reason,
        }) = pair.server.view().lifecycle().close()
        else {
            panic!("peer transport close")
        };
        assert_eq!((error_code.get(), frame_type.get()), (7, 6));
        assert_eq!(
            reason.as_deref(),
            if fail {
                Err(&o3::alloc::Error::Exhausted)
            } else {
                Ok(&frames[5..4101])
            }
        );
        assert_eq!(pair.server.view().lifecycle().state(), State::Draining);
    }
}

#[test]
fn closing_repeats_original_cause_until_fixed_expiry() {
    use common::suite::mux::dispatch;
    use dope_quic::wire::frame;

    let (mut peer, mut server, handle, now) = paths::try_wire().expect("wire fixture");
    for mut flight in dispatch::Capture::output(&mut server, now).expect("handshake output") {
        for segment in flight.segments_mut() {
            if let Some(mut packet) = peer.receive(segment).expect("handshake packet") {
                peer.open(&mut packet).expect("handshake protection");
            }
        }
        Pin::new(&mut server).output().recycle(flight);
    }
    let mut queued = peer
        .packet(&[0x0a, 2, 1, b's', 0x30, b'q'])
        .expect("queued stream and datagram");
    paths::Pair::receive(&mut server, paths::CLIENT, &mut queued, now).expect("queue delivery");
    paths::Pair::access(&mut server, handle, now, |mut conn| {
        conn.lifecycle()
            .close(Code::from_u8(7), b"first cause".to_vec());
        conn.lifecycle()
            .close(Code::from_u8(9), b"later cause".to_vec());
        assert_eq!(conn.view().lifecycle().state(), State::Closing);
        assert!(!conn.view().lifecycle().is_ready());
    })
    .expect("close application");

    let mut deadline = None;
    for input in 0..=7 {
        if input != 0 {
            let mut packet = peer.packet(&[0x30, b'l']).expect("late datagram");
            paths::Pair::receive(&mut server, paths::CLIENT, &mut packet, now)
                .expect("terminal input");
        }
        let flight = dispatch::Capture::output(&mut server, now).expect("terminal output");
        let expected = matches!(input, 0 | 1 | 3 | 7);
        assert_eq!(!flight.is_empty(), expected, "reply after {input} packets");
        for mut flight in flight {
            for segment in flight.segments_mut() {
                let mut packet = peer
                    .receive(segment)
                    .expect("close packet")
                    .expect("one RTT close");
                // Authenticate the first response without delivering it to a
                // connection: the peer keeps sending as if CLOSE were lost.
                let (_, body) = peer.open(&mut packet).expect("close protection");
                let frames = frame::Frame::decode_all(&body).expect("close frames");
                assert_eq!(frames.len(), 1);
                assert!(matches!(frames[0].view(), View::ConnectionClose {
                    code: close::Code::Application(code), reason,
                } if code == Code::from_u8(7) && reason == b"first cause"));
            }
            Pin::new(&mut server).output().recycle(flight);
        }
        assert!(server.handler().datagrams.is_empty());
        assert!(server.handler().streams.is_empty());
        let due = Pin::new(&mut server)
            .observe(now, |turn| turn.deadline())
            .expect("retention deadline");
        assert_eq!(*deadline.get_or_insert(due), due);
        assert_eq!(server.active_conns(), 1);
    }
    let deadline = deadline.expect("fixed deadline");
    let before = deadline - Duration::from_nanos(1);
    assert!(
        dispatch::Capture::output(&mut server, before)
            .expect("retention turn")
            .is_empty()
    );
    assert!(server.conn(handle).is_some());
    assert!(
        dispatch::Capture::output(&mut server, deadline)
            .expect("expiry turn")
            .is_empty()
    );
    assert!(server.conn(handle).is_none());
    assert_eq!(server.active_conns(), 0);
}

#[test]
fn unavailable_close_output_still_expires() {
    let mut client = path::try_initial().expect("initial client");
    let now = Instant::now();
    let mut batch = Batch::default();
    client
        .with_scope(|mut scope| {
            scope.lifecycle().close(Code::ZERO, Vec::new());
            scope.output().send_batch(&mut batch, now, 1, 1199)
        })
        .expect("deferred output");
    assert!(batch.is_empty());
    let deadline = client
        .view()
        .recovery()
        .next_timer()
        .expect("output independent deadline");
    assert_eq!(deadline.duration_since(now), Duration::from_millis(2_997));
    client
        .with_scope(|mut scope| {
            scope
                .recovery()
                .try_loss(deadline - Duration::from_nanos(1))
        })
        .expect("retention turn");
    assert_eq!(client.view().lifecycle().state(), State::Closing);
    client
        .with_scope(|mut scope| scope.recovery().try_loss(deadline))
        .expect("terminal expiry");
    assert!(client.view().lifecycle().is_closed());
}

#[test]
fn rejects_closing() {
    let mut pair = paths::Pending::try_new(19, 21)
        .expect("router pair")
        .establish()
        .expect("path fixture operation");
    Pin::new(&mut pair.client).observe(pair.start, |mut turn| {
        turn.with_app(pair.client_id, |mut conn| {
            conn.ready();
            conn.transport().lifecycle().close(Code::ZERO, Vec::new());
            conn.ready();
            assert!(!conn.transport().view().lifecycle().is_closed());
        })
        .expect("open application");
        assert_eq!(
            turn.with_app(pair.client_id, |_| ()),
            Err(dope_quic::errors::Access::Closed)
        );
    });
    for router in [&pair.client, &pair.server] {
        assert!(
            router.handler().errors.is_empty(),
            "{:?}",
            router.handler().errors
        );
    }
}
