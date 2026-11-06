pub mod common;
use crate::common::peer;
use common::suite::peers::config;
use std::time::{Duration, Instant};

use dope_quic::conn::stream;
use dope_quic::conn::wire::packet::Batch;
use dope_quic::wire::stream::Id;
use peer::Receiver as _;
use peer::{Config, Event, Pair, Scope as _, flight::Flight};

#[test]
fn later_stream_progresses() {
    let Pair {
        mut client,
        mut server,
        mut work,
    } = Pair::try_new(
        config::try_bounds().expect("server stream limits"),
        config::try_bounds().expect("client stream limits"),
    )
    .expect("establish peers with stream limits");
    let now = Instant::now() + Duration::from_millis(20);
    assert!(
        client
            .with_scope(|mut scope| {
                let mut streams = scope.streams();
                let mut send = streams.send();
                for index in 0..300u64 {
                    let stream = send.open_bidi()?;
                    assert_eq!(stream.get(), index * 4);
                    if index < 256 {
                        send.send(stream.send(), &[0x31; 1 << 15])?;
                    } else {
                        send.send(stream.send(), &[index as u8])?;
                    }
                }
                Ok::<_, stream::Error>(())
            })
            .is_ok(),
        "configured streams must admit their payloads"
    );

    assert!(server.receive_from(&mut work, &mut client, now).is_ok());
    let ack_at = now + Duration::from_millis(20);
    assert!(client.receive_from(&mut work, &mut server, ack_at).is_ok());
    let next = ack_at + Duration::from_millis(20);
    assert!(server.receive_from(&mut work, &mut client, next).is_ok());

    let target = (Id::new(256 * 4)).expect("target wire ID must fit");
    let mut received = Vec::new();
    let result = server.with_scope(|mut scope| {
        let mut streams = scope.streams();
        let target = streams.view().resolve_recv(target)?;
        Ok::<_, stream::Error>(streams.receive().recv(target, &mut received))
    });
    assert!(matches!(result, Ok(Ok(1))));
    assert_eq!(received, vec![0]);
}

#[test]
fn replacements_do_not_extend_sweep() {
    let mut config = Config::new(4_096, 1 << 20, 0, 513);
    config.local = Some([0, 513]);
    let Pair {
        mut client,
        mut server,
        mut work,
    } = Pair::try_new(
        config.try_build().expect("server limits"),
        config.try_build().expect("client limits"),
    )
    .expect("connected peers");
    for index in 0..513 {
        let stream = client.send_streams().open_uni().expect("initial stream");
        if index < 256 {
            client
                .send_streams()
                .send(stream, &[0x31; 4_096])
                .expect("pending prefix");
        } else {
            client.send_streams().send(stream, &[0x32]).expect("tail");
            client.send_streams().finish(stream).expect("tail FIN");
        }
    }
    let mut batch = Batch::default();
    let flight = Flight::emit_due(&mut client, &mut batch, Instant::now(), 1, 1_200)
        .expect("prime first window");
    let mut now = flight.sent_at() + Duration::from_millis(100);
    flight
        .deliver(&mut work, &mut server)
        .expect("first packet");
    let first = match server.poll().poll_event() {
        Some(Event::Readable { stream_id }) => stream_id,
        _ => unreachable!("readable"),
    };
    server
        .recv()
        .recv_owned(first)
        .expect("consume first prefix");
    let target = Id::new(2 + 4 * ((first.get() / 4 + 1) % 256)).expect("pending neighbor");
    client
        .receive_from(&mut work, &mut server, now)
        .expect("first ACK");
    let mut completed = 0;
    let mut progressed = false;
    for round in 0..3 {
        let flight =
            Flight::emit_due(&mut client, &mut batch, now, 256, 1_200).expect("next window");
        now = flight.sent_at() + Duration::from_millis(100);
        flight
            .deliver(&mut work, &mut server)
            .expect("window delivery");
        while let Some(event) = server.poll().poll_event() {
            if let Event::Readable { stream_id } = event {
                server.recv().recv_owned(stream_id).expect("consume stream");
                progressed |= stream_id.id() == target;
                completed += usize::from(stream_id.get() / 4 >= 256);
            }
        }
        client
            .receive_from(&mut work, &mut server, now)
            .expect("ACK and MAX_STREAMS");
        if round == 0 {
            assert_eq!(completed, 256, "replace a complete discovery window");
        }
        if completed >= 256 {
            completed -= 256;
            for _ in 0..256 {
                let stream = client.send_streams().open_uni().expect("replacement");
                client.send_streams().send(stream, &[0x32]).expect("tail");
                client.send_streams().finish(stream).expect("tail FIN");
            }
        }
    }
    assert!(
        progressed,
        "the pending neighbor must survive tail replacement"
    );
}
