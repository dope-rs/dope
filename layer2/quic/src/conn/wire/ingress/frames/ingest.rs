use crate::conn::wire::ingress::events;
use crate::errors::detail::Diagnostic;
use events::Observe as _;
use o3::buffer;
use o3::mem::slice::Ext as _;

use crate::{conn, stream, wire::frame};
use conn::wire::ingress;
use frame::{ack::ranges, decode};
use ingress::frames;
use ingress::packet;

use frames::sources;

pub(in crate::conn::wire::ingress) fn try_process<
    const DOMAIN: u8,
    B: stream::Recv,
    const RECONCILE: bool,
    D: buffer::raw::Segment,
    S,
    E,
>(
    input: &mut ingress::Input<'_, DOMAIN, B, RECONCILE, D>,
    meta: frames::PacketMeta,
    cid: Option<frames::PacketCid<'_>>,
    bytes: &[u8],
    source: S,
    hooks: &mut E,
) -> Result<(), conn::Error>
where
    S: sources::Source<B>,
    E: events::Sink,
{
    use frames::{Disposition, commit, plan};

    if input.connection.is_closing() {
        if input.connection.egress.lifecycle.received() {
            input
                .context
                .record(conn::Effects::from_bits(conn::Effects::TRANSMIT));
        }
        return Ok(());
    }
    let epoch = meta.epoch();
    let local = input.connection.peer.local;
    let Some(mut packet) = packet::Packet::begin(input.connection, epoch, meta.number()) else {
        return Ok(());
    };
    let (slots, max) = {
        let (conn, _) = packet.state();
        (
            conn.receive
                .datagram_capacity
                .get()
                .saturating_sub(conn.receive.datagrams.len()),
            conn.receive.local_max_datagram_frame_size,
        )
    };
    let mut elicit = false;
    let result = frame::Body::with(bytes, |body| {
        let work = &mut input.workspace.storage;
        let buffers = &mut work.buffers;
        buffers.segments.clear();
        buffers.parts.clear();
        work.records.with(|mut records| {
            let mut build = plan::Build::new(slots, max);
            let mut rest = bytes;
            let mut count = 0;
            let mut saw = false;
            let mut parse = None;
            let mut issue = None;
            while let Some(&kind) = rest.first() {
                if kind == frame::TYPE_PADDING {
                    saw = true;
                    rest = rest.split_while(|&byte| byte == frame::TYPE_PADDING).1;
                    continue;
                }
                let slot = if issue.is_none() {
                    let Some(slot) = records.vacancy() else {
                        parse = Some(conn::Error::FrameLimit {
                            actual: count + 1,
                            maximum: conn::MAX_FRAMES_PER_PACKET,
                        });
                        break;
                    };
                    Some(slot)
                } else {
                    if count == conn::MAX_FRAMES_PER_PACKET {
                        parse = Some(conn::Error::FrameLimit {
                            actual: count + 1,
                            maximum: conn::MAX_FRAMES_PER_PACKET,
                        });
                        break;
                    }
                    None
                };
                let item = decode::Decoder::new(
                    |data: &[u8]| Ok(body.locate(data)),
                    |valid: ranges::Validated<'_>| valid.record(&body),
                    decode::Notice::into_owned,
                )
                .decode(&mut rest);
                let (frame, used) = match item {
                    Ok(item) => item,
                    Err(error) => {
                        parse = Some(conn::Error::Frame(error));
                        break;
                    }
                };
                saw = true;
                if !matches!(
                    frame.view(),
                    frame::View::Ack { .. }
                        | frame::View::Padding
                        | frame::View::ConnectionClose { .. }
                ) {
                    elicit = true;
                }
                if let Some(slot) = slot {
                    issue = build
                        .try_record(slot, &body, meta.kind(), local, frame, used)
                        .err();
                }
                count += 1;
            }
            if !saw && parse.is_none() {
                parse = Some(conn::Error::ProtocolViolation);
            }
            match (parse, issue) {
                (Some(error), _) => Err(error),
                (None, Some(error)) => {
                    let code = error.close_code();
                    if let Some(observer) = hooks.observer() {
                        observer.observe(&Diagnostic::Plan(error));
                    }
                    code.map_or(Ok(frames::Disposition::Drop), Err)
                }
                (None, None) => {
                    let plan = plan::Plan::new(buffers, body, records, build);
                    commit::try_process(&mut packet, meta, cid, plan, source, hooks)
                }
            }
        })
    });
    let error = match result {
        Ok(Disposition::Commit) => match packet.try_commit(elicit, meta.now) {
            Ok(delta) => {
                input.context.record(delta);
                return Ok(());
            }
            Err((retained, error)) => {
                packet = retained;
                conn::Error::Timing(error)
            }
        },
        Ok(Disposition::Drop) => match packet.try_drop() {
            Ok(delta) => {
                input.context.record(delta);
                return Ok(());
            }
            Err(error) => conn::Error::Timing(error),
        },
        Err(error) => error,
    };
    let delta = packet.close(&error);
    input.context.record(delta);
    Err(error)
}
