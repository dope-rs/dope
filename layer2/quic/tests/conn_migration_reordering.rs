pub mod common;

use common::suite::mux::{
    dispatch,
    paths::{self, CLIENT, Pair},
};
use common::suite::peers::packet::bytes;
use dope_quic::{
    conn::{Epoch, State},
    wire::frame,
};
use std::{net::SocketAddr, time::Duration};

#[test]
fn nonprobe_orders_path() {
    let target = SocketAddr::from(([10, 0, 0, 1], 50_001));
    for (kind, expected) in [(frame::TYPE_PING, CLIENT), (frame::TYPE_PADDING, target)] {
        let (mut peer, mut server, handle, start) = paths::try_wire().expect("wire path fixture");
        let at = start + Duration::from_millis(20);
        let mut notice = peer
            .packet(&bytes::notice(65, 0).expect("spare CID"))
            .expect("spare CID packet");
        Pair::receive(&mut server, CLIENT, &mut notice, at).expect("spare CID announcement");
        let mut payload = [0; 1_200];
        payload[0] = frame::TYPE_PING;
        let mut older = peer.packet(&payload).expect("older candidate packet");
        payload[0] = kind;
        let mut newer = peer.packet(&payload).expect("newer active packet");
        Pair::receive(&mut server, CLIENT, &mut newer, at).expect("active packet");
        Pair::receive(&mut server, target, &mut older, at).expect("reordered candidate packet");
        let flight = dispatch::Capture::output(&mut server, at).expect("validation challenge");
        let mut response =
            paths::try_response(&mut peer, flight, target).expect("probing response");
        Pair::receive(&mut server, target, &mut response, at).expect("path validation");
        assert_eq!(
            Pair::peer(&mut server, handle, at).expect("active path"),
            Some(expected)
        );
    }
}

#[test]
fn newer_active_wins() {
    let (mut peer, mut server, handle, start) = paths::try_wire().expect("wire path fixture");
    let target = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let at = start + Duration::from_millis(20);
    let mut notice = peer
        .packet(&bytes::notice(65, 0).expect("spare CID"))
        .expect("spare CID packet");
    Pair::receive(&mut server, CLIENT, &mut notice, at).expect("spare CID announcement");
    let mut ping = [0; 1_200];
    ping[0] = frame::TYPE_PING;
    let mut older = peer.packet(&ping).expect("older candidate packet");
    let mut newer = peer.packet(&ping).expect("newer active packet");
    Pair::receive(&mut server, target, &mut older, at).expect("candidate packet");
    Pair::receive(&mut server, CLIENT, &mut newer, at).expect("newer active packet");
    let flight = dispatch::Capture::output(&mut server, at).expect("validation challenge");
    let mut response = paths::try_response(&mut peer, flight, target).expect("probing response");
    Pair::receive(&mut server, target, &mut response, at).expect("path validation");
    assert_eq!(
        Pair::peer(&mut server, handle, at).expect("active path"),
        Some(CLIENT)
    );
}

#[test]
fn duplicate_keeps_pending() {
    let (mut peer, mut server, handle, start) = paths::try_wire().expect("wire path fixture");
    let target = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let other = SocketAddr::from(([10, 0, 0, 1], 50_002));
    let at = start + Duration::from_millis(20);
    for sequence in 65..68 {
        let mut notice = peer
            .packet(&bytes::notice(sequence, 0).expect("spare CID"))
            .expect("spare CID packet");
        Pair::receive(&mut server, CLIENT, &mut notice, at).expect("spare CID announcement");
    }
    let mut ping = [0; 1_200];
    ping[0] = frame::TYPE_PING;
    let mut accepted = peer.packet(&ping).expect("active packet");
    let mut replay = accepted.clone();
    Pair::receive(&mut server, CLIENT, &mut accepted, at).expect("active packet accepted");
    let mut intent = peer.packet(&ping).expect("candidate packet");
    Pair::receive(&mut server, target, &mut intent, at).expect("candidate intent");
    let flight = dispatch::Capture::output(&mut server, at).expect("validation challenge");
    let mut response = paths::try_response(&mut peer, flight, target).expect("probing response");
    Pair::receive(&mut server, other, &mut replay, at).expect("duplicate packet ignored");
    Pair::receive(&mut server, target, &mut response, at).expect("pending validation response");
    assert_eq!(
        Pair::peer(&mut server, handle, at).expect("validated path"),
        Some(target)
    );
}

#[test]
fn previous_probe_keeps_its_cid_across_migrations() {
    let (mut peer, mut server, handle, start) = paths::try_wire().expect("wire path fixture");
    let target = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let third = SocketAddr::from(([10, 0, 0, 9], 50_002));
    let at = start + Duration::from_millis(20);
    for sequence in 65..68 {
        let mut notice = peer
            .packet(&bytes::notice(sequence, 0).expect("spare CID"))
            .expect("spare CID packet");
        Pair::receive(&mut server, CLIENT, &mut notice, at).expect("spare CID announcement");
    }
    let mut ping = [0; 1_200];
    ping[0] = frame::TYPE_PING;
    let mut request = peer.packet(&ping).expect("migration intent");
    Pair::receive(&mut server, target, &mut request, at).expect("new path");
    let flight = dispatch::Capture::output(&mut server, at).expect("both path challenges");
    let previous_cid = Pair::path(&flight, CLIENT)
        .find_map(Pair::short_cid)
        .expect("previous path CID")
        .to_vec();
    let target_cid = Pair::path(&flight, target)
        .find_map(Pair::short_cid)
        .expect("candidate path CID")
        .to_vec();
    assert_ne!(previous_cid, target_cid);
    let [mut late, mut response] = paths::try_responses(&mut peer, flight, [CLIENT, target])
        .expect("both paths have distinct challenges in the emitted packet order");
    Pair::receive(&mut server, third, &mut response, at)
        .expect("a response on another path validates the original challenge");
    assert_eq!(
        Pair::peer(&mut server, handle, at).expect("active path"),
        Some(target)
    );

    let (retried, retry) =
        Pair::retry(&mut server, at, 16, Some(CLIENT)).expect("previous validation remains timed");
    assert!(
        Pair::path(&retry, CLIENT)
            .all(|packet| { Pair::short_cid(packet) == Some(previous_cid.as_slice()) })
    );
    paths::try_response(&mut peer, retry, CLIENT).expect("previous challenge retries on its CID");

    let next = retried + Duration::from_millis(1);
    let mut request = peer.packet(&ping).expect("third path intent");
    Pair::receive(&mut server, third, &mut request, next).expect("next migration");
    let flight = dispatch::Capture::output(&mut server, next).expect("replacement challenges");
    assert!(Pair::path(&flight, CLIENT).next().is_none());
    assert!(
        Pair::path(&flight, target)
            .all(|packet| { Pair::short_cid(packet) == Some(target_cid.as_slice()) })
    );
    let mut response = paths::try_response(&mut peer, flight, third)
        .expect("third path receives a distinct challenge");
    Pair::receive(&mut server, target, &mut late, next).expect("abandoned previous response");
    assert_eq!(
        Pair::peer(&mut server, handle, next).expect("active path"),
        Some(target)
    );
    Pair::receive(&mut server, target, &mut response, next).expect("current challenge response");
    assert_eq!(
        Pair::peer(&mut server, handle, next).expect("active path"),
        Some(third)
    );
    assert!(server.handler().errors.is_empty());
}

#[test]
fn retiring_previous_cid_removes_its_probe_and_reset_route() {
    let (mut peer, mut server, handle, start) = paths::try_wire().expect("wire path fixture");
    let target = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let at = start + Duration::from_millis(20);
    for sequence in 65..68 {
        let mut notice = peer
            .packet(&bytes::notice(sequence, 65).expect("routed CID"))
            .expect("CID packet");
        Pair::receive(&mut server, CLIENT, &mut notice, at).expect("CID announcement");
    }
    let mut ping = [0; 1_200];
    ping[0] = frame::TYPE_PING;
    let mut request = peer.packet(&ping).expect("migration intent");
    Pair::receive(&mut server, target, &mut request, at).expect("new path");
    let flight = dispatch::Capture::output(&mut server, at).expect("both challenges");
    let mut response = paths::try_response(&mut peer, flight, target).expect("candidate response");
    Pair::receive(&mut server, target, &mut response, at).expect("candidate validates first");
    assert_eq!(
        Pair::peer(&mut server, handle, at).expect("active path"),
        Some(target)
    );

    let mut notice = peer
        .packet(&bytes::notice(67, 66).expect("retire previous CID"))
        .expect("retirement packet");
    Pair::receive(&mut server, target, &mut notice, at).expect("retirement admitted");
    let expired = at + Duration::from_secs(5);
    let flight = dispatch::Capture::output(&mut server, expired).expect("retired probe deadline");
    assert!(Pair::path(&flight, CLIENT).next().is_none());

    let mut reset = [0; 30];
    reset[0] = 0x40;
    reset[1..9].copy_from_slice(&[0xa7; 8]);
    reset[14..].copy_from_slice(&[65; 16]);
    Pair::receive(&mut server, CLIENT, &mut reset, expired).expect("retired reset token ignored");
    assert!(
        Pair::access(&mut server, handle, expired, |conn| conn
            .view()
            .lifecycle()
            .is_ready())
        .expect("connection remains owned")
    );
}

#[test]
fn third_path_challenge_cannot_borrow_another_paths_cid() {
    let (mut peer, mut server, handle, start) = paths::try_wire().expect("wire path fixture");
    let target = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let third = SocketAddr::from(([10, 0, 0, 9], 50_002));
    let at = start + Duration::from_millis(20);
    let mut notice = peer
        .packet(&bytes::notice(65, 0).expect("spare CID"))
        .expect("spare CID packet");
    Pair::receive(&mut server, CLIENT, &mut notice, at).expect("spare CID announcement");
    let mut ping = [0; 1_200];
    ping[0] = frame::TYPE_PING;
    let mut request = peer.packet(&ping).expect("migration intent");
    Pair::receive(&mut server, target, &mut request, at).expect("new path");
    let flight = dispatch::Capture::output(&mut server, at).expect("validation challenges");
    let mut token = None;
    for mut packet in flight {
        let selected = packet.addr() == target;
        for segment in packet.segments_mut() {
            let Some(mut wire) = peer.receive(segment).expect("receive challenge packet") else {
                continue;
            };
            let (_, body) = peer.open(&mut wire).expect("open challenge packet");
            for item in frame::Frame::decode_all(&body).expect("challenge frames") {
                if selected && let frame::View::PathChallenge { data } = item.view() {
                    token = Some(*data);
                }
            }
        }
    }
    let token = token.expect("candidate challenge");
    let mut frames = [0; 1_200];
    frames[0] = frame::TYPE_PATH_RESPONSE;
    frames[1..9].copy_from_slice(&token);
    frames[9] = frame::TYPE_PATH_CHALLENGE;
    frames[10..18].copy_from_slice(b"thirdpth");
    let mut response = peer
        .packet(&frames)
        .expect("response with third-path challenge");
    let error = Pair::receive(&mut server, third, &mut response, at);
    assert!(matches!(error, Err(common::runtime::Error::Receive(
        dope_quic::errors::Receive::Packet(dope_quic::conn::Error::Path(
            dope_quic::conn::wire::path::Error::Address(
                dope_quic::conn::wire::path::Address::Cid { address }
            )
        ))
    )) if address.addr() == third));
    assert!(
        Pair::access(&mut server, handle, at, |conn| {
            conn.view().path().has_token(&token) && conn.view().lifecycle().is_ready()
        })
        .expect("receipt survives the exact reply admission failure")
    );
    // The bounded two-CID route set cannot serve this third challenge. Its
    // explicit resource failure must neither undo the receipt nor reuse a CID.
    assert_eq!(
        Pair::peer(&mut server, handle, at).expect("receipt completes without another packet"),
        Some(target)
    );
    let flight = dispatch::Capture::output(&mut server, at).expect("retained output");
    assert!(Pair::path(&flight, third).next().is_none());
}

#[test]
fn previous_reply_keeps_its_cid_when_active_cid_retires() {
    for reply_first in [false, true] {
        let (mut peer, mut server, handle, start) = paths::try_wire().expect("wire path fixture");
        let target = SocketAddr::from(([10, 0, 0, 1], 50_001));
        let at = start + Duration::from_millis(20);
        for sequence in [67, 65] {
            let mut notice = peer
                .packet(&bytes::notice(sequence, 1).expect("routed CID"))
                .expect("CID packet");
            Pair::receive(&mut server, CLIENT, &mut notice, at).expect("CID announcement");
        }
        let mut ping = [0; 1_200];
        ping[0] = frame::TYPE_PING;
        let mut request = peer.packet(&ping).expect("migration intent");
        Pair::receive(&mut server, target, &mut request, at).expect("candidate path");
        let flight = dispatch::Capture::output(&mut server, at).expect("both challenges");
        assert!(Pair::path(&flight, CLIENT).any(|packet| {
            Pair::short_cid(packet) == Some([0x59, 67, 0, 0, 0, 0, 0, 0].as_slice())
        }));
        let [mut old_response, mut response] =
            paths::try_responses(&mut peer, flight, [CLIENT, target]).expect("both challenges");
        let mut challenge = [0; 1_200];
        challenge[0] = frame::TYPE_PATH_CHALLENGE;
        challenge[1..9].copy_from_slice(b"oldreply");
        let mut challenge = peer.packet(&challenge).expect("active-path challenge");
        if reply_first {
            Pair::receive(&mut server, CLIENT, &mut challenge, at).expect("queue old reply");
            Pair::receive(&mut server, CLIENT, &mut old_response, at)
                .expect("previous validation preserves its pending reply");
        } else {
            Pair::receive(&mut server, CLIENT, &mut old_response, at)
                .expect("previous validation completes first");
            Pair::receive(&mut server, CLIENT, &mut challenge, at).expect("queue old reply");
        }
        Pair::receive(&mut server, target, &mut response, at).expect("candidate commits");
        let mut notice = peer
            .packet(&bytes::notice(68, 66).expect("retire active CID65"))
            .expect("retirement packet");
        Pair::receive(&mut server, target, &mut notice, at).expect("active CID replacement");
        dispatch::Capture::send(&mut server, handle, b"new-active-cid".to_vec(), at)
            .expect("active output");
        let flight = dispatch::Capture::output(&mut server, at).expect("owned reply and output");
        assert!(Pair::path(&flight, target).any(|packet| {
            Pair::short_cid(packet) == Some([0x59, 68, 0, 0, 0, 0, 0, 0].as_slice())
        }));
        let mut replied = false;
        for mut packet in flight {
            let old_path = packet.addr() == CLIENT;
            if old_path {
                assert_eq!(
                    Pair::short_cid(&packet),
                    Some([0x59, 67, 0, 0, 0, 0, 0, 0].as_slice())
                );
            }
            for segment in packet.segments_mut() {
                let Some(mut wire) = peer.receive(segment).expect("receive retained output") else {
                    continue;
                };
                let (_, body) = peer.open(&mut wire).expect("open retained output");
                for record in frame::Frame::decode_all(&body).expect("retained frames") {
                    if let frame::View::PathResponse { data } = record.view()
                        && data == b"oldreply"
                    {
                        assert!(old_path);
                        replied = true;
                    }
                }
            }
        }
        assert!(replied, "the old reply keeps its exact address and CID");
        assert_eq!(
            Pair::peer(&mut server, handle, at).expect("active path"),
            Some(target)
        );
    }
}

#[test]
fn unvalidated_candidate_cannot_replace_retired_active_cid() {
    let (mut peer, mut server, handle, start) = paths::try_wire().expect("wire path fixture");
    let target = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let at = start + Duration::from_millis(20);
    let mut notice = peer
        .packet(&bytes::notice(65, 0).expect("candidate CID"))
        .expect("CID packet");
    Pair::receive(&mut server, CLIENT, &mut notice, at).expect("CID announcement");
    let mut ping = [0; 1_200];
    ping[0] = frame::TYPE_PING;
    let mut request = peer.packet(&ping).expect("migration intent");
    Pair::receive(&mut server, target, &mut request, at).expect("candidate path");
    let flight = dispatch::Capture::output(&mut server, at).expect("both challenges");
    let mut response = paths::try_response(&mut peer, flight, target).expect("candidate response");
    let mut notice = peer
        .packet(&bytes::notice(65, 65).expect("retire active CID"))
        .expect("retirement packet");
    let error = Pair::receive(&mut server, CLIENT, &mut notice, at)
        .expect_err("no usable CID for a validated path");
    assert!(matches!(
        error,
        common::runtime::Error::Receive(dope_quic::errors::Receive::Packet(
            dope_quic::conn::Error::ConnectionIdUnavailable
        ))
    ));
    assert!(
        dispatch::Capture::output(&mut server, at)
            .expect("terminal output")
            .is_empty()
    );
    Pair::receive(&mut server, target, &mut response, at).expect("late response discarded");
    assert!(server.conn(handle).is_none());
}

#[test]
fn validated_candidate_keeps_its_cid_when_active_cid_retires() {
    for response_first in [false, true] {
        let (mut peer, mut server, handle, start) = paths::try_wire().expect("wire path fixture");
        let target = SocketAddr::from(([10, 0, 0, 1], 50_001));
        let at = start + Duration::from_millis(20);
        let mut notice = peer
            .packet(&bytes::notice(65, 0).expect("candidate CID"))
            .expect("CID packet");
        Pair::receive(&mut server, CLIENT, &mut notice, at).expect("CID announcement");
        let mut ping = [0; 1_200];
        ping[0] = frame::TYPE_PING;
        let mut request = peer.packet(&ping).expect("migration intent");
        Pair::receive(&mut server, target, &mut request, at).expect("candidate path");
        let flight = dispatch::Capture::output(&mut server, at).expect("both challenges");
        let candidate_cid = [0x59, 65, 0, 0, 0, 0, 0, 0];
        assert_eq!(
            Pair::path(&flight, target).find_map(Pair::short_cid),
            Some(candidate_cid.as_slice())
        );
        assert_ne!(
            Pair::path(&flight, CLIENT)
                .find_map(Pair::short_cid)
                .expect("active path CID"),
            candidate_cid.as_slice()
        );
        let mut token = None;
        for mut packet in flight {
            let candidate = packet.addr() == target;
            for segment in packet.segments_mut() {
                let Some(mut wire) = peer.receive(segment).expect("receive challenge packet")
                else {
                    continue;
                };
                let (_, body) = peer.open(&mut wire).expect("open challenge packet");
                for record in frame::Frame::decode_all(&body).expect("challenge frames") {
                    if candidate && let frame::View::PathChallenge { data } = record.view() {
                        token = Some(*data);
                    }
                }
            }
        }
        let token = token.expect("candidate challenge");
        let notice = bytes::notice(65, 65).expect("retire active CID");
        let (response_offset, notice_offset) = if response_first {
            (0, 9)
        } else {
            (notice.len(), 0)
        };
        let mut frames = [0; 1_200];
        frames[response_offset] = frame::TYPE_PATH_RESPONSE;
        frames[response_offset + 1..response_offset + 9].copy_from_slice(&token);
        frames[notice_offset..notice_offset + notice.len()].copy_from_slice(&notice);
        let mut response = peer.packet(&frames).expect("response and retirement");
        Pair::receive(&mut server, target, &mut response, at)
            .expect("validated candidate survives active CID retirement");
        assert_eq!(
            Pair::peer(&mut server, handle, at).expect("active path"),
            Some(target)
        );
        dispatch::Capture::send(&mut server, handle, b"candidate-owned".to_vec(), at)
            .expect("candidate output");
        let flight = dispatch::Capture::output(&mut server, at).expect("retirement and output");
        assert!(!flight.is_empty());
        let mut retired = false;
        for mut packet in flight {
            assert_eq!(packet.addr(), target);
            assert_eq!(Pair::short_cid(&packet), Some(candidate_cid.as_slice()));
            for segment in packet.segments_mut() {
                let Some(mut wire) = peer.receive(segment).expect("receive migrated output") else {
                    continue;
                };
                let (_, body) = peer.open(&mut wire).expect("open migrated output");
                for record in frame::Frame::decode_all(&body).expect("migrated frames") {
                    if let frame::View::RetireConnectionId { sequence_number } = record.view() {
                        assert_eq!(sequence_number.get(), 0);
                        retired = true;
                    }
                }
            }
        }
        assert!(retired, "the old active CID is retired");
        assert!(
            Pair::access(&mut server, handle, at, |conn| conn
                .view()
                .lifecycle()
                .is_ready())
            .expect("connection remains owned")
        );
    }
}

#[test]
fn candidate_retirement_keeps_migration_when_later_cid_is_invalid() {
    let (mut peer, mut server, handle, start) = paths::try_wire().expect("wire path fixture");
    let target = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let at = start + Duration::from_millis(20);
    let mut notice = peer
        .packet(&bytes::notice(65, 0).expect("candidate CID"))
        .expect("CID packet");
    Pair::receive(&mut server, CLIENT, &mut notice, at).expect("CID announcement");
    let mut ping = [0; 1_200];
    ping[0] = frame::TYPE_PING;
    let mut request = peer.packet(&ping).expect("migration intent");
    Pair::receive(&mut server, target, &mut request, at).expect("candidate path");
    let flight = dispatch::Capture::output(&mut server, at).expect("both challenges");
    let [token] = paths::try_challenges(&mut peer, flight, [target]).expect("candidate challenge");
    assert!(
        Pair::access(&mut server, handle, at, |conn| conn
            .view()
            .recovery()
            .unacked(Epoch::Application))
        .expect("old flight")
            > 0
    );
    let notice = bytes::notice(65, 65).expect("retire active CID");
    let mut invalid = notice.clone();
    *invalid.last_mut().expect("reset token") ^= 1;
    let mut frames = [0; 1_200];
    frames[0] = frame::TYPE_PATH_RESPONSE;
    frames[1..9].copy_from_slice(&token);
    frames[9..9 + notice.len()].copy_from_slice(&notice);
    frames[9 + notice.len()..9 + notice.len() + invalid.len()].copy_from_slice(&invalid);
    let mut response = peer.packet(&frames).expect("response and CID updates");
    let error = Pair::receive(&mut server, target, &mut response, at)
        .expect_err("the later CID conflicts with the committed CID");
    assert!(matches!(
        error,
        common::runtime::Error::Receive(dope_quic::errors::Receive::Packet(
            dope_quic::conn::Error::ProtocolViolation
        ))
    ));
    let view = server
        .conn(handle)
        .expect("closing connection retained")
        .view();
    assert_eq!(view.path().peer_address(), Some(target));
    assert_eq!(view.lifecycle().state(), State::Closing);
    assert_eq!(view.recovery().unacked(Epoch::Application), 0);
    let flight = dispatch::Capture::output(&mut server, at).expect("transport close");
    assert!(!flight.is_empty());
    for mut packet in flight {
        assert_eq!(packet.addr(), target);
        assert_eq!(
            Pair::short_cid(&packet),
            Some([0x59, 65, 0, 0, 0, 0, 0, 0].as_slice())
        );
        for segment in packet.segments_mut() {
            let mut wire = peer
                .receive(segment)
                .expect("receive close packet")
                .expect("one RTT close");
            let (_, body) = peer.open(&mut wire).expect("open close packet");
            let frames = frame::Frame::decode_all(&body).expect("close frames");
            assert_eq!(frames.len(), 1);
            assert!(matches!(
                frames[0].view(),
                frame::View::ConnectionClose {
                    code: frame::close::Code::Transport { error_code, .. },
                    ..
                } if error_code.get() == 0x0a
            ));
        }
    }
}

#[test]
fn candidate_retirement_keeps_old_reply_until_its_own_cid_arrives() {
    let (mut peer, mut server, handle, start) = paths::try_wire().expect("wire path fixture");
    let target = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let at = start + Duration::from_millis(20);
    let mut notice = peer
        .packet(&bytes::notice(65, 0).expect("candidate CID"))
        .expect("CID packet");
    Pair::receive(&mut server, CLIENT, &mut notice, at).expect("CID announcement");
    let mut ping = [0; 1_200];
    ping[0] = frame::TYPE_PING;
    let mut request = peer.packet(&ping).expect("migration intent");
    Pair::receive(&mut server, target, &mut request, at).expect("candidate path");
    let flight = dispatch::Capture::output(&mut server, at).expect("both challenges");
    let [token] = paths::try_challenges(&mut peer, flight, [target]).expect("candidate challenge");
    let mut challenge = [0; 1_200];
    challenge[0] = frame::TYPE_PATH_CHALLENGE;
    challenge[1..9].copy_from_slice(b"oldreply");
    let mut challenge = peer.packet(&challenge).expect("active path challenge");
    Pair::receive(&mut server, CLIENT, &mut challenge, at).expect("queue old path reply");
    let notice = bytes::notice(65, 65).expect("retire active CID");
    let mut frames = [0; 1_200];
    frames[0] = frame::TYPE_PATH_RESPONSE;
    frames[1..9].copy_from_slice(&token);
    frames[9..9 + notice.len()].copy_from_slice(&notice);
    let mut response = peer.packet(&frames).expect("response and retirement");
    Pair::receive(&mut server, target, &mut response, at).expect("candidate commits");
    let flight = dispatch::Capture::output(&mut server, at).expect("CID-blocked old reply");
    for mut packet in flight {
        assert_eq!(packet.addr(), target);
        for segment in packet.segments_mut() {
            let Some(mut wire) = peer.receive(segment).expect("receive migrated output") else {
                continue;
            };
            let (_, body) = peer.open(&mut wire).expect("open migrated output");
            for record in frame::Frame::decode_all(&body).expect("migrated frames") {
                assert!(!matches!(record.view(), frame::View::PathResponse { data }
                    if data == b"oldreply"));
            }
        }
    }
    let mut notice = peer
        .packet(&bytes::notice(66, 65).expect("replacement previous CID"))
        .expect("replacement packet");
    Pair::receive(&mut server, target, &mut notice, at).expect("old reply receives its own CID");
    dispatch::Capture::send(&mut server, handle, b"active-owned".to_vec(), at)
        .expect("active output");
    let flight = dispatch::Capture::output(&mut server, at).expect("owned reply and active output");
    assert!(Pair::path(&flight, target).next().is_some());
    let mut replied = false;
    for mut packet in flight {
        let old_path = packet.addr() == CLIENT;
        let sequence = if old_path { 66 } else { 65 };
        if !old_path {
            assert_eq!(packet.addr(), target);
        }
        assert_eq!(
            Pair::short_cid(&packet),
            Some([0x59, sequence, 0, 0, 0, 0, 0, 0].as_slice())
        );
        for segment in packet.segments_mut() {
            let Some(mut wire) = peer.receive(segment).expect("receive retained output") else {
                continue;
            };
            let (_, body) = peer.open(&mut wire).expect("open retained output");
            for record in frame::Frame::decode_all(&body).expect("retained frames") {
                if let frame::View::PathResponse { data } = record.view()
                    && data == b"oldreply"
                {
                    assert!(old_path);
                    replied = true;
                }
            }
        }
    }
    assert!(replied, "the old reply retains its exact address");
    assert_eq!(
        Pair::peer(&mut server, handle, at).expect("active path"),
        Some(target)
    );
}

#[test]
fn validated_candidate_waits_for_its_own_replacement_cid() {
    let (mut peer, mut server, handle, start) = paths::try_wire().expect("wire path fixture");
    let target = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let at = start + Duration::from_millis(20);
    let mut notice = peer
        .packet(&bytes::notice(65, 0).expect("candidate CID"))
        .expect("CID packet");
    Pair::receive(&mut server, CLIENT, &mut notice, at).expect("CID announcement");
    let mut request = peer.packet(&[0; 1_200]).expect("probing candidate packet");
    Pair::receive(&mut server, target, &mut request, at).expect("candidate reachability");
    let flight = dispatch::Capture::output(&mut server, at).expect("candidate challenge");
    let mut response = paths::try_response(&mut peer, flight, target).expect("candidate response");
    Pair::receive(&mut server, target, &mut response, at).expect("reachability without intent");
    let mut notice = peer
        .packet(&bytes::notice(66, 66).expect("retire candidate CID"))
        .expect("retirement packet");
    Pair::receive(&mut server, CLIENT, &mut notice, at).expect("one remaining active CID");
    dispatch::Capture::send(&mut server, handle, b"active-only".to_vec(), at)
        .expect("active output");
    let flight = dispatch::Capture::output(&mut server, at).expect("use active CID66");
    assert!(Pair::path(&flight, CLIENT).any(|packet| {
        Pair::short_cid(packet) == Some([0x59, 66, 0, 0, 0, 0, 0, 0].as_slice())
    }));
    let mut ping = [0; 1_200];
    ping[0] = frame::TYPE_PING;
    let mut request = peer.packet(&ping).expect("validated candidate intent");
    Pair::receive(&mut server, target, &mut request, at).expect("intent waits for a CID");
    assert_eq!(
        Pair::peer(&mut server, handle, at).expect("active path"),
        Some(CLIENT)
    );
    let flight = dispatch::Capture::output(&mut server, at).expect("CID-blocked candidate");
    assert!(Pair::path(&flight, target).next().is_none());

    let mut notice = peer
        .packet(&bytes::notice(67, 66).expect("replacement candidate CID"))
        .expect("replacement packet");
    Pair::receive(&mut server, CLIENT, &mut notice, at).expect("NEW_CID resumes migration");
    assert_eq!(
        Pair::peer(&mut server, handle, at).expect("active path"),
        Some(target)
    );
    dispatch::Capture::send(&mut server, handle, b"candidate-owned".to_vec(), at)
        .expect("candidate output");
    let flight = dispatch::Capture::output(&mut server, at).expect("use candidate CID67");
    assert!(Pair::path(&flight, target).any(|packet| {
        Pair::short_cid(packet) == Some([0x59, 67, 0, 0, 0, 0, 0, 0].as_slice())
    }));
    assert!(Pair::path(&flight, target).all(|packet| {
        Pair::short_cid(packet) != Some([0x59, 66, 0, 0, 0, 0, 0, 0].as_slice())
    }));
}

#[test]
fn expired_candidate_leaves_the_active_reply_on_its_current_cid() {
    let (mut peer, mut server, handle, start) = paths::try_wire().expect("wire path fixture");
    let expired_path = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let target = SocketAddr::from(([10, 0, 0, 1], 50_002));
    let at = start + Duration::from_millis(20);
    let mut notice = peer
        .packet(&bytes::notice(65, 0).expect("candidate CID"))
        .expect("CID packet");
    Pair::receive(&mut server, CLIENT, &mut notice, at).expect("CID announcement");
    let mut request = peer.packet(&[0; 1_200]).expect("candidate probe");
    Pair::receive(&mut server, expired_path, &mut request, at).expect("candidate admission");
    let flight = dispatch::Capture::output(&mut server, at).expect("both challenges");
    let mut response =
        paths::try_response(&mut peer, flight, CLIENT).expect("previous active path challenge");
    Pair::receive(&mut server, CLIENT, &mut response, at).expect("previous path validates");
    let expired = at + Duration::from_secs(5);
    let expired_flight =
        dispatch::Capture::output(&mut server, expired).expect("unanswered candidate expires");
    for mut packet in expired_flight {
        for segment in packet.segments_mut() {
            if let Some(mut wire) = peer.receive(segment).expect("receive expiry output") {
                peer.open(&mut wire).expect("consume without replying");
            }
        }
    }
    let mut notice = peer
        .packet(&bytes::notice(66, 0).expect("unused CID"))
        .expect("CID packet");
    Pair::receive(&mut server, CLIENT, &mut notice, expired).expect("fresh spare admission");
    let mut challenge = [0; 1_200];
    challenge[0] = frame::TYPE_PATH_CHALLENGE;
    challenge[1..9].copy_from_slice(b"actreply");
    let mut challenge = peer.packet(&challenge).expect("active challenge");
    Pair::receive(&mut server, CLIENT, &mut challenge, expired).expect("active reply queued");
    let mut ping = [0; 1_200];
    ping[0] = frame::TYPE_PING;
    let mut request = peer.packet(&ping).expect("new candidate intent");
    Pair::receive(&mut server, target, &mut request, expired).expect("new candidate admission");
    let flight = dispatch::Capture::output(&mut server, expired).expect("owned path output");
    assert!(Pair::path(&flight, CLIENT).all(|packet| {
        Pair::short_cid(packet) != Some([0x59, 66, 0, 0, 0, 0, 0, 0].as_slice())
    }));
    assert!(Pair::path(&flight, target).any(|packet| {
        Pair::short_cid(packet) == Some([0x59, 66, 0, 0, 0, 0, 0, 0].as_slice())
    }));
    let mut response =
        paths::try_response(&mut peer, flight, target).expect("new candidate challenge");
    Pair::receive(&mut server, target, &mut response, expired).expect("new candidate validates");
    assert_eq!(
        Pair::peer(&mut server, handle, expired).expect("active path"),
        Some(target)
    );
}
