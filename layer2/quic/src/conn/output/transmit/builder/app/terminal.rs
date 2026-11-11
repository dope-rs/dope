use crate::conn::output::transmit::builder::ack;
use crate::conn::stream::state::transmit::log::journal;
use o3::buffer::{self, write};
use std::{mem, num::NonZero, time};

use crate::conn::output::transmit::{
    self,
    builder::{self, pending},
    packet::{plan::Plan, seals},
    permit::{self, fit},
};
use crate::conn::wire::path;
use crate::conn::{self, life::status::phase};
use crate::proto::recovery::{numbers, pmtud};
use crate::security::crypto::output::Draft;
use crate::wire::frame::close;
use crate::{
    stream,
    wire::{
        frame,
        packet::{kind, short},
        stream::Initiator,
        varint,
    },
};

pub(in crate::conn::output::transmit) fn probe<
    'connection,
    const DOMAIN: u8,
    B: stream::Recv,
    D: buffer::raw::Segment,
>(
    build: builder::Builder<'connection, DOMAIN, B, D>,
    now: time::Instant,
    dst: &mut write::Append<'_>,
    probe: pmtud::Probe,
    max: usize,
) -> transmit::Built<'connection, DOMAIN, B, D> {
    let Some(write) = build.connection.handshake.keys.write() else {
        return Ok(None);
    };
    let phase = write.is_key_phase();
    if !build.can_track() {
        return Ok(None);
    }
    let target = probe.get();
    if target > max {
        return Ok(None);
    }
    let Some(number) = conn::Epoch::Application
        .bind_mut(&mut build.connection.egress.recovery.spaces)
        .next()
    else {
        return Ok(None);
    };

    let overhead = 1 + build.connection.path.peer.id().len() + usize::from(number.encoded_len());
    let Some(payload) = target.checked_sub(overhead + conn::TAG_LEN) else {
        return Ok(None);
    };
    let Some(space) = payload.checked_sub(1) else {
        return Ok(None);
    };

    let dcid = *build.connection.path.peer.id();
    let target = build.destination;
    target.prefix(dst)?;
    let mut draft = Draft::try_new(
        dst.append(),
        short::Ref::new(dcid.as_ref_id(), number.encoding(), phase),
    )
    .map_err(transmit::Admission::Protection)?;
    let ack = {
        let mut frames = draft.append();
        let ack = ack::append::<kind::OneRtt, false>(
            &build.connection.receive.packet_numbers,
            now,
            &mut frames,
            space,
        )
        .map_err(transmit::Admission::into_error)?;
        let padding = space - frames.len();
        frames
            .try_tail::<0, 1>([&[frame::TYPE_PING]], padding, frame::TYPE_PADDING)
            .map_err(|rejected| transmit::Admission::Payload {
                error: rejected.into_parts().1,
                requested: 1 + padding,
            })?;
        ack
    };
    let mut commit = Plan::new(conn::Epoch::Application, number);
    commit.flags.ack = ack;
    commit.flags.set_elicit(true);
    commit.flags.set_flight(true);
    commit.path.pmtud_probe = Some(probe);
    let prepared = pending::Packet::try_new(journal::Schedule::new(build).finish(), commit, target)
        .map_err(|(_, error)| error.into_error())?;
    let ((), prepared) = match prepared.protect(draft) {
        Ok(output) => output,
        Err(failure) => {
            return failure.into_error().map_or(Ok(None), Err);
        }
    };
    Ok(Some(prepared))
}

enum Protection {
    Initial,
    Handshake,
    OneRtt(bool),
}

pub(in crate::conn::output::transmit) struct Close {
    seal: Protection,
    epoch: conn::Epoch,
    number: numbers::Packet,
    code: close::Code,
    limit: NonZero<usize>,
    fixed: usize,
}

pub(in crate::conn::output::transmit) fn admit<
    const DOMAIN: u8,
    B: stream::Recv,
    D: buffer::raw::Segment,
>(
    conn: &conn::Machine<DOMAIN, B, D>,
    close: &phase::Close,
    max: usize,
) -> Option<Close> {
    let seal = if let Some(write) = conn.handshake.keys.write() {
        Protection::OneRtt(write.is_key_phase())
    } else if conn.handshake.keys.long.handshake().is_some() {
        Protection::Handshake
    } else {
        conn.handshake.keys.long.initial()?;
        Protection::Initial
    };
    let epoch = match &seal {
        Protection::Initial => conn::Epoch::Initial,
        Protection::Handshake => conn::Epoch::Handshake,
        Protection::OneRtt(_) => conn::Epoch::Application,
    };
    let permit = permit::Permit::new(conn);
    let (number, limit) = match epoch {
        conn::Epoch::Initial => {
            let (number, limit, _) = fit::crypto::<kind::Initial, DOMAIN, B, D>(&permit, max)?;
            (number, limit)
        }
        conn::Epoch::Handshake => {
            let (number, limit, _) = fit::crypto::<kind::Handshake, DOMAIN, B, D>(&permit, max)?;
            (number, limit)
        }
        conn::Epoch::Application => {
            let number = epoch.bind(&conn.egress.recovery.spaces).next()?;
            let limit = builder::Builder::<'_, DOMAIN, B, D>::short_for(
                &conn.path,
                max,
                number.encoding(),
                path::OutputRoute::ACTIVE,
            );
            (number, NonZero::new(limit)?)
        }
    };
    let code = match close {
        phase::Close::LocalApplication {
            error_code: value, ..
        } if epoch == conn::Epoch::Application => close::Code::Application(*value),
        phase::Close::LocalApplication { .. } => close::Code::Transport {
            error_code: conn::APPLICATION_ERROR,
            frame_type: varint::VarInt::ZERO,
        },
        phase::Close::LocalTransport(value) => close::Code::Transport {
            error_code: *value,
            frame_type: varint::VarInt::ZERO,
        },
        phase::Close::PeerApplication { .. } | phase::Close::PeerTransport { .. } => {
            unreachable!("close")
        }
    };
    let fixed = 1 + usize::from(match code {
        close::Code::Application(value) => value.as_varint().encoded_len(),
        close::Code::Transport {
            error_code: value,
            frame_type: frame,
        } => value.encoded_len() + frame.encoded_len(),
    });
    (fixed < limit.get()).then_some(Close {
        seal,
        epoch,
        number,
        code,
        limit,
        fixed,
    })
}

pub(in crate::conn::output::transmit) fn close<
    'connection,
    const DOMAIN: u8,
    B: stream::Recv,
    D: buffer::raw::Segment,
>(
    build: builder::Builder<'connection, DOMAIN, B, D>,
    dst: &mut write::Append<'_>,
    max: usize,
) -> transmit::Built<'connection, DOMAIN, B, D> {
    let Some(close) = build.connection.egress.lifecycle.close() else {
        return Ok(None);
    };
    let Some(Close {
        seal,
        epoch,
        number,
        code,
        limit,
        fixed,
    }) = admit(build.connection, close, max)
    else {
        return Ok(None);
    };
    let limit = limit.get();
    let mut reason: &[u8] = match close {
        phase::Close::LocalApplication { reason, .. } if epoch == conn::Epoch::Application => {
            reason
        }
        _ => &[],
    };
    while fixed + varint::VarInt::width(reason.len()) + reason.len() > limit {
        let len = reason.len();
        let encoded = fixed + varint::VarInt::width(len) + len;
        reason = &reason[..len.saturating_sub((encoded - limit).max(1))];
    }
    let len = reason.len();
    let close = frame::Borrowed::try_close(code, reason)
        .map_err(|error| transmit::Admission::Encoding { epoch, error })?;
    let mut commit = Plan::new(epoch, number);
    commit.flags.set_close(true);
    let target = build.destination;
    if let Protection::OneRtt(phase) = seal {
        let dcid = *build.connection.path.peer.id();
        target.prefix(dst)?;
        let mut draft = Draft::try_new(
            dst.append(),
            short::Ref::new(dcid.as_ref_id(), commit.packet_number.encoding(), phase),
        )
        .map_err(transmit::Admission::Protection)?;
        close
            .encode(&mut draft.append())
            .map_err(|error| transmit::Admission::Payload {
                error,
                requested: fixed + varint::VarInt::width(len) + len,
            })?;
        let prepared =
            pending::Packet::try_new(journal::Schedule::new(build).finish(), commit, target)
                .map_err(|(_, error)| error.into_error())?;
        return match prepared.protect(draft) {
            Ok(((), prepared)) => Ok(Some(prepared)),
            Err(failure) => failure.into_error().map_or(Ok(None), Err),
        };
    }
    let frames = &mut build.connection.scratch.frames;
    frames.clear();
    close
        .encode(frames)
        .map_err(|error| transmit::Admission::Payload {
            error,
            requested: fixed + varint::VarInt::width(len) + len,
        })?;

    if epoch == conn::Epoch::Initial
        && matches!(build.connection.peer.local, Initiator::Client)
        && frames.len() < limit
    {
        let padding = limit - frames.len();
        write::Append::new(frames)
            .try_fill(padding, frame::TYPE_PADDING)
            .map_err(|error| transmit::Admission::Payload {
                error,
                requested: padding,
            })?;
    }
    let mut frames = mem::take(&mut build.connection.scratch.frames);
    target.prefix(dst).inspect_err(|_| {
        build.connection.scratch.frames = mem::take(&mut frames);
    })?;
    let prepared =
        match pending::Packet::try_new(journal::Schedule::new(build).finish(), commit, target) {
            Ok(prepared) => prepared,
            Err((scratch, error)) => {
                scratch.frames = frames;
                return Err(error.into_error());
            }
        };
    let sealed = match seal {
        Protection::Initial => seals::long::<DOMAIN, B, D, kind::Initial>(dst, &frames, prepared),
        Protection::Handshake => {
            seals::long::<DOMAIN, B, D, kind::Handshake>(dst, &frames, prepared)
        }
        Protection::OneRtt(_) => unreachable!("epoch"),
    };
    let ((), mut prepared) = match sealed {
        Ok(output) => output,
        Err(mut failure) => {
            frames.clear();
            failure.scratch().frames = frames;
            return failure.into_error().map_or(Ok(None), Err);
        }
    };
    frames.clear();
    prepared.scratch().frames = frames;
    Ok(Some(prepared))
}
