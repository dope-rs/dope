pub mod common;

use std::time::{Duration, Instant};

use common::peer;
use common::suite::peers::config;
use dope_quic::conn::{self, output::transmit, wire::packet::Batch};
use dope_test::alloc::Tracker;
use o3::alloc;
use peer::{Pair, Receiver as _};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn full_journal_sends_before_retry() {
    use dope_quic::errors::Code;
    use peer::Scope as _;

    let config = peer::Config::new(1024, 65536, 0, 33);
    let server = config
        .try_build()
        .expect("server options")
        .into_builder()
        .journals(|journals| journals.control::<32>())
        .try_build()
        .expect("bounded control journal");
    let mut pair = Pair::try_new(server, config.try_build().expect("client options"))
        .expect("connected peers");
    let ids: Vec<_> = (0..33)
        .map(|_| {
            let mut send = pair.client.send_streams();
            let id = send.open_uni().expect("stream credit");
            send.send(id, b"x").expect("stream payload");
            id
        })
        .collect();
    let mut now = Instant::now() + Duration::from_secs(1);
    pair.exchange(now, 4).expect("stream delivery");
    for id in ids {
        pair.server.recv().stop(id.id(), Code::ZERO).expect("stop");
    }
    let reset = pair
        .server
        .send_streams()
        .open_uni()
        .expect("reverse stream");
    pair.server
        .send_streams()
        .reset(reset, Code::ZERO)
        .expect("reset");
    for _ in 0..4 {
        now += Duration::from_millis(30);
        pair.exchange(now, 1).expect("send queued stops and retry");
    }
    let mut events = pair.client.poll();
    assert_eq!(
        std::iter::from_fn(|| events.poll_event())
            .filter(|event| matches!(event, peer::Event::Stopped { error_code, .. } | peer::Event::Reset { error_code, .. } if *error_code == Code::ZERO))
            .count(),
        34
    );
}

#[test]
fn control_stream_retry() {
    use o3::depot::capacity;
    use peer::Scope as _;

    const TOKEN: [u8; 8] = [0x7b; 8];
    let mut observed = false;
    for budget in 0..32 {
        let Pair {
            mut client,
            mut server,
            mut work,
        } = config::try_packet().expect("established packet pair");
        let stream = {
            let mut send = client.send_streams();
            let stream = send.open_uni().expect("stream credit");
            send.send(stream, &[0x5a; 32]).expect("payload credit");
            send.finish(stream).expect("stream FIN");
            stream
        };
        client
            .with_scope(|mut scope| scope.path().try_probe(TOKEN))
            .expect("path challenge");
        let now = client
            .view()
            .send_at(Instant::now() + Duration::from_secs(1));
        let before = client.view().recovery().in_flight();
        let mut batch = Batch::default();
        let result = Tracker::fail_after(budget, || {
            client.with_scope(|mut scope| scope.output().send_batch(&mut batch, now, 1, 1200))
        });
        if !matches!(
            result,
            Err(transmit::Error::Admission(transmit::Admission::Packet {
                space: conn::Epoch::Application,
                reason: capacity::Reason::Allocation(alloc::Error::Exhausted),
                controls,
                streams,
                ..
            })) if controls.get() > 0 && streams.get() > 0
        ) {
            continue;
        }
        assert!(batch.is_empty(), "joint admission must publish no packet");
        assert_eq!(client.view().recovery().in_flight(), before);
        client
            .with_scope(|mut scope| scope.output().send_batch(&mut batch, now, 1, 1200))
            .expect("retry retains both selections");
        assert_eq!(batch.packets(), 1);
        let next = client.view().send_at(now);
        assert!(next < client.view().next_timer().expect("retry arms PTO"));
        let mut idle = Batch::default();
        client
            .with_scope(|mut scope| scope.output().send_batch(&mut idle, next, 1, 1200))
            .expect("completed selections leave no stale packet");
        assert!(idle.is_empty());
        for wire in batch.iter_mut() {
            server
                .receive(&mut work, wire, now)
                .expect("retry authenticates");
        }
        assert_eq!(
            server
                .recv()
                .recv_owned(stream.id())
                .expect("stream payload"),
            Some(vec![0x5a; 32])
        );
        assert!(
            server
                .stream_state()
                .recv_eof(stream.id())
                .expect("stream FIN")
        );
        assert!(
            server
                .recv()
                .recv_owned(stream.id())
                .expect("drained stream")
                .is_none()
        );
        client
            .receive_from(&mut work, &mut server, next)
            .expect("challenge response");
        assert!(client.view().path().has_token(&TOKEN));
        observed = true;
        break;
    }
    assert!(observed, "no joint packet allocation failure observed");
}
