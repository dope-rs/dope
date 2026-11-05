use crate::proto::path::Peer;
use crate::runtime::brand;
use o3::buffer::{self, storage, write::Sink as _};

use std::{num::NonZero, time};

use crate::runtime::routing::registry;
use crate::{conn, errors, runtime::mux, stream};
use conn::{machine, wire};
use issuer::Source as _;
use mux::{auth, output, router};
use path::reset;
use router::routing;
use routing::{issuer, packets};
use wire::path;

pub(in crate::runtime::mux) fn receive<
    A: auth::Mode<DOMAIN>,
    C,
    const DOMAIN: u8,
    B: stream::Recv,
>(
    state: &mut router::State<A, C, DOMAIN, B>,
    from: Peer,
    packet: &[u8],
    now: time::Instant,
) -> Result<bool, errors::Receive> {
    let trigger = packets::Header::new(packet);
    if let Some(token) = reset::Token::from_tail(packet) {
        let matched = brand::Token::with(|scope| {
            registry::Scope::with(&mut state.registry, |mut slots| {
                detect(&mut slots, scope, from, token, now)
            })
        });
        if matched {
            return Ok(true);
        }
    }
    if !trigger.is_short() {
        return Ok(false);
    }
    let Some(cfg) = state.server.as_ref().map(A::config) else {
        return Ok(false);
    };
    let cfg = cfg.0;
    let Some(secret) = cfg.security.stateless_reset_secret.as_ref() else {
        return Ok(false);
    };
    let Some(dcid) = trigger.dcid::<A::LocalRoute>() else {
        return Ok(false);
    };
    if trigger.len() < 23 {
        return Ok(false);
    }
    let Some(amp) = trigger.len().checked_mul(3) else {
        return Ok(false);
    };
    let overhead = from.overhead();
    let limit = cfg
        .packet_max()
        .min(usize::from(from.max_udp_payload().get()) - overhead)
        .min(state.outgoing.bytes_capacity.saturating_sub(overhead))
        .min(amp);
    if limit < 22 {
        return Ok(false);
    }
    let len = NonZero::<usize>::MIN.saturating_add((trigger.len() - 1).min(limit) - 1);
    let prefix = NonZero::<usize>::MIN.saturating_add(len.get() - reset::Token::LEN - 1);
    let (outgoing, issuer) = (&mut state.outgoing, &mut state.binding.issuer);
    let emission = outgoing.emit_owned(len.get() + overhead, len.get() + overhead, |reset| {
        let mut reset =
            match storage::NonEmpty::try_resize(reset, prefix.saturating_add(overhead), 0) {
                Ok(reset) => reset,
                Err(rejected) => {
                    let (reset, error) = rejected.into_parts();
                    return Err((reset, Some(error)));
                }
            };
        if let Err(issuer::Exhausted::Exhausted) = issuer.reset(&mut reset[overhead..]) {
            return Err((reset.into_storage(), None));
        }
        if let Some(ticket) = from.ticket() {
            reset[..overhead].copy_from_slice(ticket.as_bytes());
        }
        let first = &mut reset[overhead];
        *first = (*first & 0x3f) | 0x40;
        if let Err(error) = reset.append().write_slice(&secret.token_for(dcid)) {
            return Err((reset.into_storage(), Some(error)));
        }
        Ok(output::Flight::plain(from, reset))
    });
    match emission {
        output::Emission::Allocation {
            error,
            requested: need,
        } => Err(errors::Receive::Allocation {
            error,
            requested: need,
        }),
        output::Emission::Sent | output::Emission::Skipped => Ok(false),
    }
}

pub(in crate::runtime::mux) fn detect<C, const DOMAIN: u8, B, D, T>(
    slots: &mut registry::Scope<'_, '_, C, DOMAIN, B, D, T>,
    scope: brand::Token<'_>,
    from: Peer,
    token: reset::Token,
    now: time::Instant,
) -> bool
where
    B: stream::Recv,
    D: buffer::raw::Segment,
{
    let (entries, side, reset) = slots.resets();
    let mut matched = false;
    for (key, value) in side.bucket(reset.hash(&token)) {
        if !value.matches(token) {
            continue;
        }
        let Some(key) = entries.admit(key) else {
            break;
        };
        let mut slot = entries.access::<false>(key, scope.reborrow());
        let accepted = match slot.conn.routing().reset(from, token) {
            machine::Reset::Ignore => false,
            machine::Reset::Accept => {
                slot.conn.events().mark(conn::Effects::LIFECYCLE);
                true
            }
        };
        if accepted {
            entries.refresh(key, now, conn::DeadlineMode::Exact, None);
            matched = true;
        }
    }
    matched
}
