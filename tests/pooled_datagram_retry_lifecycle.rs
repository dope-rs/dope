pub mod common;

use common::peer::Config;
use common::runtime::gso::{self, Capture, Pair};
use common::suite::cid::Count;
use common::suite::owners;
use dope_quic::conn::State;
use dope_quic::errors::{Code, SendFailure};
use dope_quic::proto::datagram::Arena;
use dope_test::harness::observe::VecCellExt as _;
use std::pin::Pin;

#[test]
fn zero_keeps_retry_idle() {
    let Pair {
        mut client,
        handle,
        now,
        ..
    } = Pair::new(|| Config::default().try_build().expect("datagram options"))
        .expect("fixture handshake");
    assert!(
        Capture::output(&mut client, now)
            .expect("fixture output")
            .is_empty()
    );
    client.handler().events.clear_stream();
    let deadline = Pin::new(&mut client).observe(now, |turn| turn.deadline());

    let arena = Arena::try_new(1, 1200).expect("one packet owner");
    let packet = arena.try_acquire(1).expect("packet owner");
    let result = Pin::new(&mut client)
        .observe(now, |mut turn| {
            turn.with_app(handle, |mut conn| {
                conn.transport().output().try_packet(packet, now, 0)
            })
        })
        .expect("live managed connection");
    let packet = match result
        .err()
        .expect("zero maximum retains the packet as Full")
    {
        SendFailure::Full(packet) => Some(packet),
        _ => None,
    }
    .expect("zero maximum returns the full packet");
    assert_eq!(
        Pin::new(&mut client).observe(now, |turn| turn.deadline()),
        deadline,
        "fixed input cannot create a retry deadline"
    );
    assert!(
        Capture::output(&mut client, now)
            .expect("fixture output")
            .is_empty()
    );
    assert!(client.handler().events.streams.len() == 0);

    let result = Pin::new(&mut client)
        .observe(now, |mut turn| {
            turn.with_app(handle, |mut conn| {
                conn.transport().output().try_packet(packet, now, 1200)
            })
        })
        .expect("live managed connection");
    result
        .as_ref()
        .map_err(owners::rejection)
        .expect("positive maximum admits the retained packet");
}

#[test]
fn close_reclaims_retry() {
    let options = || {
        Config::default()
            .try_build()
            .expect("datagram options")
            .into_builder()
            .transport(|path| path.max_pmtu(1200))
            .try_build()
            .expect("fixed path MTU")
    };
    let Pair {
        mut client,
        handle,
        mut now,
        ..
    } = Pair::new(options).expect("fixture handshake");
    assert!(
        Capture::output(&mut client, now)
            .expect("fixture output")
            .is_empty()
    );
    client.handler().events.clear_stream();
    let arena = Arena::try_new(1, 1200).expect("one retained packet owner");
    let mut blocked = None;
    for _ in 0..32 {
        let packet = arena.try_acquire(1150).expect("recycled packet owner");
        let result = Pin::new(&mut client)
            .observe(now, |mut turn| {
                turn.with_app(handle, |mut conn| {
                    conn.transport().output().try_packet(packet, now, 1200)
                })
            })
            .expect("live managed connection");
        assert!(
            matches!(&result, Ok(_) | Err(SendFailure::Full(_))),
            "pooled burst must send or become full: {:?}",
            result.as_ref().err().map(owners::rejection)
        );
        if let Err(SendFailure::Full(packet)) = result {
            blocked = Some(packet);
            break;
        }
    }
    let mut packet = blocked.expect("same-instant burst reaches pacing");
    packet.payload_mut().fill(7);
    let payload = packet.payload().as_ptr();
    let due = client
        .conn(handle)
        .expect("live connection")
        .view()
        .recovery()
        .send_at(now);
    assert!(due > now);
    assert_eq!(
        Pin::new(&mut client).observe(now, |turn| turn.deadline()),
        Some(due)
    );

    let result = Pin::new(&mut client)
        .observe(now, |mut turn| {
            turn.with_app(handle, |mut conn| {
                conn.transport().lifecycle().close(Code::ZERO, Vec::new());
                conn.transport().output().try_packet(packet, now, 1200)
            })
        })
        .expect("close the armed connection");
    let Err(SendFailure::Closed(packet)) = result else {
        panic!("closing connection must return Closed");
    };
    assert_eq!(packet.payload().as_ptr(), payload);
    assert_eq!(packet.payload(), &[7; 1150]);
    assert_eq!(arena.available(), 0);
    Capture::output(&mut client, now).expect("fixture output");
    let conn = client.conn(handle).expect("closing connection retained");
    assert_eq!(conn.view().lifecycle().state(), State::Closing);
    let deadline = conn
        .view()
        .recovery()
        .next_timer()
        .expect("terminal deadline");
    assert!(deadline > due);
    assert_eq!(
        Pin::new(&mut client).observe(now, |turn| turn.deadline()),
        Some(deadline)
    );
    assert!(
        Capture::output(&mut client, due)
            .expect("fixture output")
            .is_empty()
    );
    assert!(client.handler().events.streams.len() == 0);
    assert_eq!(
        Pin::new(&mut client).observe(due, |turn| turn.deadline()),
        Some(deadline),
        "the old pacing deadline does not rearm the retry"
    );
    assert!(
        Capture::output(&mut client, deadline)
            .expect("terminal expiry")
            .is_empty()
    );
    assert!(client.conn(handle).is_none());
    assert_eq!(
        Pin::new(&mut client).observe(deadline, |turn| turn.deadline()),
        None
    );

    now = deadline;
    let (key, config) = Count::try_keys().expect("next peer TLS configuration");
    let mut server = Capture::server(Capture::default(), config, options()).expect("next peer");
    let next = Capture::connect(
        &mut client,
        gso::SERVER,
        key,
        options(),
        gso::CID.to_vec(),
        now,
    )
    .expect("reuse the only connection slot");
    assert_eq!(next.index(), handle.index());
    assert_ne!(next.generation(), handle.generation());
    Capture::settle_pair(&mut client, &mut server, gso::CLIENT, gso::SERVER, &mut now)
        .expect("fixture pair settlement");
    let ready = client.handler().events.established.take();
    assert!(ready.contains(&next));
    client.handler().events.established.set(ready);
    assert!(client.handler().events.streams.len() == 0);
    assert!(
        Pin::new(&mut client)
            .observe(now, |turn| turn.deadline())
            .is_none_or(|deadline| deadline > now),
        "next does not inherit an armed retry"
    );
    drop(packet);
    assert_eq!(arena.available(), 1);
}
