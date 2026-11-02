pub mod common;

use common::peer::{Config, Pair, Receiver as _};
use dope_quic::{
    conn::{
        self,
        stream::{Error, abandon, event},
    },
    errors::{Code, detail::Plan},
    stream::{release, send},
    wire::stream::Id,
};
use std::time::{Duration, Instant};

#[test]
fn future_abandon_survives_late_data() {
    let mut pair = Pair::try_default().expect("connected peers");
    let id = pair.client.with_scope(|mut scope| {
        let mut streams = scope.streams();
        streams.send().open_bidi().expect("implicit stream");
        streams.send().open_bidi().expect("future stream").id()
    });
    let code = Code::from_u8(0x95);
    let mut released = 0usize;
    pair.server.with_scope(|mut scope| {
        let mut streams = scope.streams();
        let local = streams
            .view()
            .resolve_send(Id::new(1).expect("local ID"))
            .expect("send direction");
        assert!(matches!(
            streams
                .send()
                .abandon_with(local, code, &mut |_| released += 1),
            Err(abandon::Error::Plan(Plan::Access(conn::Error::StreamState))),
        ));
        let beyond = streams
            .view()
            .resolve_send(Id::new(32).expect("beyond ID"))
            .expect("send direction");
        assert!(matches!(
            streams
                .send()
                .abandon_with(beyond, code, &mut |_| released += 1),
            Err(abandon::Error::Plan(Plan::Access(conn::Error::StreamLimit))),
        ));
        let send = streams
            .view()
            .resolve_send(id)
            .expect("peer send direction");
        streams
            .send()
            .abandon_with(send, code, &mut |_| released += 1)
            .expect("future peer abandonment");
        assert_eq!(streams.view().stopped(send), Some(code));
        assert!(!streams.view().is_send_open(send));
        assert!(
            matches!(streams.events().poll(), Some(event::Item::Stopped { id: stopped, error_code })
            if stopped.id() == id && error_code == code)
        );
    });
    pair.client.with_scope(|mut scope| {
        let mut streams = scope.streams();
        let send = streams
            .view()
            .resolve_send(id)
            .expect("opened client stream");
        streams
            .send()
            .send(send, b"late")
            .expect("inflight request");
        streams.send().finish(send).expect("request FIN");
    });
    let now = Instant::now() + Duration::from_secs(1);
    pair.server
        .receive_from(&mut pair.work, &mut pair.client, now)
        .expect("late request arrival");
    pair.server.with_scope(|mut scope| {
        let mut streams = scope.streams();
        let stream = streams.view().resolve_bidi(id).expect("same peer stream");
        assert_eq!(
            streams
                .receive()
                .recv_owned(stream.receive())
                .expect("late request"),
            Some(b"late".to_vec())
        );
        assert!(streams.view().is_recv_eof(stream.receive()));
        assert!(!streams.view().is_send_open(stream.send()));
        while streams.events().poll().is_some() {}
    });
    pair.client
        .receive_from(
            &mut pair.work,
            &mut pair.server,
            now + Duration::from_millis(1),
        )
        .expect("reset delivery");
    pair.client.with_scope(|mut scope| {
        let mut streams = scope.streams();
        assert!(
            matches!(streams.events().poll(), Some(event::Item::Reset { id: reset, error_code })
            if reset.id() == id && error_code == code)
        );
    });
    pair.exchange(now + Duration::from_millis(40), 2)
        .expect("reset acknowledgement");
    let releases = released;
    pair.server.with_scope(|mut scope| {
        let mut streams = scope.streams();
        while streams.events().poll().is_some() {}
        let send = streams
            .view()
            .resolve_send(id)
            .expect("closed peer identity");
        assert_eq!(streams.view().stopped(send), None);
        assert!(streams.resident().handle(id).is_none());
        streams
            .send()
            .abandon_with(send, code, &mut |_| released += 1)
            .expect("closed abandonment is inert");
        assert!(!streams.view().is_send_open(send));
        assert!(streams.events().poll().is_none());
    });
    assert_eq!(released, releases);
}

#[test]
fn event_pressure_preserves_frontier_and_owners() {
    let server = Config::default()
        .try_build()
        .expect("server options")
        .into_builder()
        .streams(|streams| streams.events(1))
        .try_build()
        .expect("one event slot");
    let mut pair = Pair::try_new(
        server,
        Config::default().try_build().expect("client options"),
    )
    .expect("connected peers");
    let mut released = Vec::new();
    pair.server.with_scope(|mut scope| {
        let mut streams = scope.streams();
        while streams.events().poll().is_some() {}
        let first = streams.view().resolve_send(Id::ZERO).expect("first peer stream");
        streams.send().abandon_with(first, Code::ZERO, &mut |event| released.push(event))
            .expect("occupy event slot");
        let future = streams.view().resolve_send(Id::new(4).expect("future ID")).expect("future peer stream");
        let local = streams.send().open_uni().expect("local stream");
        assert!(matches!(streams.send().try_buffer(local, send::Buffer::Owned(vec![1; 4])),
            conn::stream::SendOutcome::Accepted));
        streams.send().abandon_with(first, Code::ZERO, &mut |event| released.push(event))
            .expect("repeat needs no event slot");
        let before = released.len();
        for stream in [future, local] {
            assert!(matches!(
                streams.send().abandon_with(stream, Code::ZERO, &mut |event| released.push(event)),
                Err(abandon::Error::Events(shortage)) if shortage.requested.get() == 1 && shortage.available == 0,
            ));
        }
        assert!(streams.resident().handle(future.id()).is_none());
        assert_eq!(streams.send().send(future, &[]), Err(Error::InvalidStream));
        assert_eq!(streams.view().buffered(local), 4);
        assert!(streams.view().is_send_open(local));
        assert_eq!(released.len(), before);
        assert!(matches!(streams.events().poll(), Some(event::Item::Stopped { id, .. }) if id == first));
        for stream in [future, local] {
            streams.send().abandon_with(stream, Code::ZERO, &mut |event| released.push(event))
                .expect("retry after event release");
            assert!(!streams.view().is_send_open(stream));
            assert!(matches!(streams.events().poll(), Some(event::Item::Stopped { id, .. }) if id == stream));
        }
        assert_eq!(streams.view().buffered(local), 0);
        assert!(released.iter().any(|event| event.source.id() == local.id()
            && event.cause == release::Cause::Stopped && event.charge.logical_bytes == 4
            && event.charge.owners == 1));
    });
}
