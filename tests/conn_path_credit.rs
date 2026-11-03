pub mod common;

use common::suite::mux::{
    dispatch,
    paths::{self, CLIENT, Pair},
};
use common::suite::peers::packet::bytes;
use dope_quic::wire::frame;
use std::{net::SocketAddr, pin::Pin};

#[test]
fn candidate_credit_parks() {
    let (mut peer, mut server, handle, at) = paths::try_wire().expect("wire path fixture");
    let target = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let (valid, timer) = Pair::access(&mut server, handle, at, |conn| {
        (
            conn.view().path().is_valid(),
            conn.view().recovery().next_timer(),
        )
    })
    .expect("connection state");
    assert!(valid);
    assert!(timer.is_none_or(|due| due > at), "due recovery: {timer:?}");
    let mut notice = peer
        .packet(&bytes::notice(65, 0).expect("spare CID"))
        .expect("spare CID packet");
    Pair::receive(&mut server, CLIENT, &mut notice, at).expect("spare CID announcement");
    let request = [frame::TYPE_PATH_CHALLENGE, 1, 2, 3, 4, 5, 6, 7, 8];
    let mut wire = peer.packet(&request).expect("candidate challenge");
    let credit = wire.len() * 3;
    assert!(credit < 1_200);
    Pair::receive(&mut server, target, &mut wire, at).expect("candidate packet");
    let flight = dispatch::Capture::output(&mut server, at).expect("candidate response");
    let mut count = 0;
    for mut packet in flight {
        let selected = packet.addr() == target;
        for segment in packet.segments_mut() {
            let size = segment.len();
            let Some(mut wire) = peer.receive(segment).expect("receive server packet") else {
                assert!(!selected, "candidate packet must contain 1-RTT frames");
                continue;
            };
            let (_, frames) = peer.open(&mut wire).expect("open server packet");
            if !selected {
                continue;
            }
            count += 1;
            assert_eq!(size, credit);
            let frames = frame::Frame::decode_all(&frames).expect("response frames");
            assert!(frames.iter().any(|frame| matches!(
                frame.view(),
                frame::View::PathResponse { data } if data.as_slice() == &request[1..]
            )));
            assert!(
                !frames
                    .iter()
                    .any(|frame| matches!(frame.view(), frame::View::PathChallenge { .. }))
            );
        }
    }
    assert_eq!(count, 1);
    assert_eq!(
        Pair::peer(&mut server, handle, at).expect("active path"),
        Some(CLIENT)
    );
    for _ in 0..2 {
        assert!(
            dispatch::Capture::output(&mut server, at)
                .expect("blocked candidate")
                .is_empty()
        );
    }
    let due = Pin::new(&mut server).observe(at, |turn| turn.deadline());
    assert!(due.is_none_or(|due| due > at), "blocked deadline: {due:?}");
    let mut wire = peer
        .packet(&[frame::TYPE_PING])
        .expect("fresh candidate credit");
    let credit = wire.len() * 3;
    Pair::receive(&mut server, target, &mut wire, at).expect("candidate padding");
    let flight = dispatch::Capture::output(&mut server, at).expect("resumed candidate");
    let mut count = 0;
    let mut short = None;
    for mut packet in flight {
        let selected = packet.addr() == target;
        for segment in packet.segments_mut() {
            let size = segment.len();
            let Some(mut wire) = peer.receive(segment).expect("receive server packet") else {
                assert!(!selected, "candidate packet must contain 1-RTT frames");
                continue;
            };
            let (_, frames) = peer.open(&mut wire).expect("open server packet");
            if !selected {
                continue;
            }
            count += 1;
            assert_eq!(size, credit);
            let frames = frame::Frame::decode_all(&frames).expect("challenge frames");
            short = frames.iter().find_map(|frame| match frame.view() {
                frame::View::PathChallenge { data } => Some(*data),
                _ => None,
            });
            assert!(short.is_some());
            assert!(
                !frames
                    .iter()
                    .any(|frame| matches!(frame.view(), frame::View::PathResponse { .. }))
            );
        }
    }
    assert_eq!(count, 1);
    let short = short.expect("short candidate challenge");
    assert!(
        !Pair::access(&mut server, handle, at, |mut conn| conn
            .path()
            .cancel_probe(short))
        .expect("live candidate")
    );
    let mut response = vec![frame::TYPE_PATH_RESPONSE];
    response.extend_from_slice(&short);
    let mut wire = peer.packet(&response).expect("short challenge response");
    Pair::receive(&mut server, target, &mut wire, at).expect("address validation");
    let mut expanded = None;
    for mut packet in dispatch::Capture::output(&mut server, at).expect("MTU validation") {
        let selected = packet.addr() == target;
        for segment in packet.segments_mut() {
            let size = segment.len();
            let mut wire = peer.receive(segment).expect("MTU packet").expect("1-RTT");
            let (_, frames) = peer.open(&mut wire).expect("open MTU packet");
            if !selected {
                continue;
            }
            for item in frame::Frame::decode_all(&frames).expect("MTU frames") {
                if let frame::View::PathChallenge { data } = item.view() {
                    assert!(size >= 1_200);
                    expanded = Some(*data);
                }
            }
        }
    }
    let expanded = expanded.expect("expanded candidate challenge");
    assert_ne!(short, expanded);
    let mut wire = peer.packet(&response).expect("delayed short response");
    Pair::receive(&mut server, target, &mut wire, at).expect("duplicate address proof");
    assert_eq!(
        Pair::peer(&mut server, handle, at).expect("active path"),
        Some(CLIENT)
    );
    response[1..].copy_from_slice(&expanded);
    let mut wire = peer.packet(&response).expect("expanded challenge response");
    Pair::receive(&mut server, target, &mut wire, at).expect("MTU validation response");
    assert_eq!(
        Pair::peer(&mut server, handle, at).expect("validated path"),
        Some(target)
    );
    assert!(server.handler().errors.is_empty());
}
