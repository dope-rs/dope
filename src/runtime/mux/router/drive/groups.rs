use std::{num::NonZero, time};

use o3::depot::array::copy;

use crate::{
    conn, errors,
    runtime::{brand, mux, routing::registry},
    stream,
};
use conn::wire::packet::Fresh as _;
use drive::{MIN_GSO_PACKETS, Step};
use mux::{auth, output, router};
use router::drive;

#[derive(Clone, Copy, PartialEq, Eq)]
struct Class {
    max_packet_bytes: NonZero<usize>,
    encoded_packet_bytes: usize,
}

pub(super) fn flush_group<
    'storage,
    'id,
    A: auth::Mode<DOMAIN>,
    C,
    const DOMAIN: u8,
    B: stream::Recv,
>(
    scope: brand::Token<'_>,
    slots: &mut registry::Scope<'storage, 'id, C, DOMAIN, B, A::Send, A::Session>,
    anchor: registry::Key<'id>,
    binding: &mut router::Binding<A, DOMAIN>,
    outgoing: &mut output::Storage,
    now: time::Instant,
) -> Result<Step, errors::Transmit<A::Identity, DOMAIN>> {
    let queued = slots.len_in::<{ mux::FLUSH_QUEUE }>();
    if queued < MIN_GSO_PACKETS - 1 || outgoing.cache.batch.is_none() {
        return Ok(Step::Idle);
    }
    let classify = |slot: &registry::Slot<C, DOMAIN, B, A::Send, A::Session>| {
        if slot.is_first() {
            return None;
        }
        let limit = drive::slot_max_packet_bytes(slot.is_first(), slot.max_packet_bytes);
        let bytes = slot.conn.view().permit().gso_len(now, limit.get())?;
        Some(Class {
            max_packet_bytes: limit,
            encoded_packet_bytes: bytes,
        })
    };
    let slot = slots.get(anchor);
    let Some(class) = classify(slot) else {
        return Ok(Step::Idle);
    };
    let destination = slot.peer_addr;
    let room = outgoing.gso_room(
        class
            .max_packet_bytes
            .saturating_add(destination.overhead()),
    );
    if room < MIN_GSO_PACKETS {
        return Ok(Step::Idle);
    }
    let take = room.min(conn::MAX_BATCH_PACKETS);
    let mut picks = copy::Vec::<registry::Key<'id>, { conn::MAX_BATCH_PACKETS }>::from_one(anchor);
    for (key, slot) in slots
        .iter::<{ mux::FLUSH_QUEUE }>()
        .take(conn::MAX_BATCH_PACKETS * 2 - 1)
    {
        let Some(candidate) = classify(slot) else {
            continue;
        };
        if slot.peer_addr != destination || candidate != class {
            continue;
        }
        let Some(pick) = picks.vacancy() else {
            break;
        };
        pick.insert(key);
        if picks.len() == take {
            break;
        }
    }
    let picks = picks.as_slice();
    let len = picks.len();
    if len < MIN_GSO_PACKETS {
        return Ok(Step::Idle);
    }
    let bytes = outgoing
        .bytes_capacity
        .saturating_sub(outgoing.pending.bytes());
    let (_, result) = router::State::<A, C, DOMAIN, B>::output_group(
        scope.reborrow(),
        slots,
        picks,
        binding,
        outgoing,
        now,
        |fatal, slots, output| {
            output.emit_gso(len, bytes, destination, |batch| {
                batch.prepare();
                for &key in picks {
                    let mut entry = slots.entry(key);
                    let mut slot = entry.access::<false>(fatal.reborrow());
                    let before = batch.packets();
                    let result = slot.conn.output().append_gso(
                        batch,
                        now,
                        class.max_packet_bytes.get(),
                        *slot.peer_addr,
                    );
                    if batch.packets() != before {
                        *slot.first_flush = false;
                    }
                    if let Err(error) = result {
                        return (None, Err((key, error)));
                    }
                }
                (None, Ok(()))
            })
        },
    );
    match result {
        Ok(()) => Ok(Step::Driven),
        Err((key, cause)) => Err(errors::Transmit::new(
            slots.handle(binding.domain, key),
            cause,
        )),
    }
}
