use crate::conn::wire::ingress::events;
use o3::buffer;
use std::time;

use crate::conn::wire::{ingress, path};
use crate::conn::{self, machine};
use crate::proto::recovery::pmtud;
use crate::wire::packet::{self, long, short};
use crate::wire::stream::Initiator;
use crate::{security::crypto, stream};
use ingress::bodies;
use long::sequence::{self, Sequence as _};
use path::reset;

enum Reset {
    Received,
    Absent,
}

pub(in crate::conn) struct Receiver<
    'a,
    'b,
    const DOMAIN: u8,
    B: stream::Recv,
    const RECONCILE: bool,
    D: buffer::raw::Segment,
    E,
> {
    input: &'a mut ingress::Input<'b, DOMAIN, B, RECONCILE, D>,
    now: time::Instant,
    sink: &'a mut E,
}

impl<'a, 'b, const DOMAIN: u8, B: stream::Recv, const RECONCILE: bool, D: buffer::raw::Segment, E>
    Receiver<'a, 'b, DOMAIN, B, RECONCILE, D, E>
{
    pub(in crate::conn) fn new(
        input: &'a mut ingress::Input<'b, DOMAIN, B, RECONCILE, D>,
        now: time::Instant,
        sink: &'a mut E,
    ) -> Self {
        Self { input, now, sink }
    }

    pub(in crate::conn) fn run<P, S>(
        self,
        mut packet: long::Datagram<P>,
        policy: S,
    ) -> ingress::Received
    where
        P: long::Buffer,
        for<'p> &'p mut P: sequence::Sequence,
        S: bodies::Policy<P, DOMAIN, B, RECONCILE, D>,
        for<'p> S:
            bodies::Policy<<&'p mut P as sequence::Sequence>::First, DOMAIN, B, RECONCILE, D>,
        E: events::Sink,
    {
        let mut token = None;
        let result = (|| {
            let Self { input, now, sink } = self;
            let terminal = input.connection.try_terminal(now)?;
            token = reset::Token::from_tail(packet.as_ref());
            if terminal {
                input
                    .context
                    .record(conn::Effects::from_bits(conn::Effects::LIFECYCLE));
                if input.connection.egress.lifecycle.state != conn::State::Closing
                    || !input.connection.path.ingress().is_active()
                {
                    return Ok(());
                }
            }
            let bytes = packet.as_ref();
            let Some(&first) = bytes.first() else {
                return Ok(());
            };
            let len = bytes.len();
            if len > pmtud::MAX_PMTU as usize {
                token = None;
                return Err(conn::Error::PacketCeiling);
            }
            if !input.connection.egress.activity.peer_address_validated
                && let Some(credit) = input.connection.path.ingress().credit(len)
            {
                let received = input
                    .connection
                    .egress
                    .activity
                    .amplification_received
                    .checked_add(credit.bytes() as u64);
                let outer_received = input
                    .connection
                    .egress
                    .activity
                    .outer_amplification_received
                    .checked_add(credit.outer_bytes() as u64);
                if let (Some(received), Some(outer_received)) = (received, outer_received) {
                    input.connection.egress.activity.amplification_received = received;
                    input
                        .connection
                        .egress
                        .activity
                        .outer_amplification_received = outer_received;
                }
                input
                    .context
                    .record(conn::Effects::from_bits(conn::Effects::TRANSMIT));
            }
            if first & packet::FIXED_BIT == 0 {
                Self::reset(input, &mut token);
                return Ok(());
            }
            let accept = matches!(input.connection.peer.local, Initiator::Client)
                || long::is_initial_size(len);
            if let Some(initial) = packet.initial() {
                token = None;
                let initial = initial.map_err(conn::Error::Long)?;
                policy.try_process(input, bodies::Packet::long(initial, now), sink)?;
            }
            let mut packet = packet.into_inner();
            let mut failure = None;
            while let Some(&first) = packet.as_ref().first() {
                if matches!(
                    input.connection.egress.lifecycle.state,
                    conn::State::Draining | conn::State::Closed
                ) {
                    break;
                }
                let closing = input.connection.is_closing();
                if first & 0x80 == 0 {
                    if first & 0x40 == 0 {
                        break;
                    }
                    let pto = match input.connection.try_pto() {
                        Ok((_, pto)) => pto,
                        Err(error) => return Self::fail(input, &mut token, error.into()),
                    };
                    let cid = input.connection.path.local.id();
                    let width = cid.as_ref_id().length();
                    let next = conn::Epoch::Application
                        .bind(&input.connection.receive.packet_numbers.spaces)
                        .expected_pn();
                    let Some(key) = input.connection.handshake.keys.read() else {
                        break;
                    };
                    let sealed = match short::Protected::parse(packet, width) {
                        Ok(sealed) => sealed,
                        Err(fail) => {
                            let (_, error) = fail.into_parts();
                            return Self::fail(input, &mut token, conn::Error::Short(error));
                        }
                    };
                    let opened = match key.open(sealed, next, now, pto, !closing) {
                        Ok(opened) => opened,
                        Err(fail) => {
                            let (_, error) = fail.into_parts();
                            return Self::fail(input, &mut token, error);
                        }
                    };
                    if !closing {
                        input.connection.handshake.keys.drop_early();
                    }
                    token = None;
                    policy.try_process(input, bodies::Packet::one_rtt(opened, now, pto), sink)?;
                    break;
                }
                if first & 0x30 == 0x30 {
                    let (result, authenticated) = if closing {
                        (Ok(()), false)
                    } else {
                        input.receive_retry(packet.as_ref(), now)
                    };
                    if authenticated {
                        token = None;
                    }
                    return match Self::reset(input, &mut token) {
                        Reset::Received => Ok(()),
                        Reset::Absent => result.and_then(|()| failure.map_or(Ok(()), Err)),
                    };
                }
                let long = match (&mut packet).first() {
                    Ok(long) => long,
                    Err(error) => return Self::fail(input, &mut token, conn::Error::Long(error)),
                };
                if (matches!(input.connection.peer.local, Initiator::Client)
                    || matches!(&long, long::Parsed::Initial(_)))
                    && input
                        .connection
                        .path
                        .origin
                        .peer_first_scid
                        .as_ref()
                        .is_some_and(|peer| peer.as_ref_id() != long.scid_ref())
                {
                    continue;
                }
                match long {
                    long::Parsed::Initial(long) if accept => {
                        if let Some(key) = input.connection.handshake.keys.initial() {
                            let peer = (!closing
                                && matches!(input.connection.peer.local, Initiator::Client)
                                && input.connection.path.origin.peer_first_scid.is_none())
                            .then(|| long.scid());
                            let next = conn::Epoch::Initial
                                .bind(&input.connection.receive.packet_numbers.spaces)
                                .expected_pn();
                            let open = match key.decrypt(long, next) {
                                Ok(open) => open,
                                Err(
                                    error @ conn::Error::Crypto(
                                        crypto::Error::Decrypt(_)
                                        | crypto::Error::Sample { .. }
                                        | crypto::Error::Number { .. },
                                    ),
                                ) => {
                                    failure.get_or_insert(error);
                                    continue;
                                }
                                Err(error) => return Self::fail(input, &mut token, error),
                            };
                            token = None;
                            if let Some(cid) = peer {
                                input.connection.path.set_peer(cid)?;
                                input
                                    .context
                                    .record(conn::Effects::from_bits(conn::Effects::TRANSMIT));
                            }
                            policy.try_process(input, bodies::Packet::long(open, now), sink)?;
                        }
                    }
                    long::Parsed::Initial(_) => {}
                    long::Parsed::ZeroRtt(long) => {
                        if let Some(key) = input.connection.handshake.keys.early() {
                            let next = conn::Epoch::Application
                                .bind(&input.connection.receive.packet_numbers.spaces)
                                .expected_pn();
                            let open = match key.decrypt(long, next) {
                                Ok(open) => open,
                                Err(
                                    error @ conn::Error::Crypto(
                                        crypto::Error::Decrypt(_)
                                        | crypto::Error::Sample { .. }
                                        | crypto::Error::Number { .. },
                                    ),
                                ) => {
                                    failure.get_or_insert(error);
                                    continue;
                                }
                                Err(error) => return Self::fail(input, &mut token, error),
                            };
                            token = None;
                            policy.try_process(input, bodies::Packet::long(open, now), sink)?;
                        }
                    }
                    long::Parsed::Handshake(long) => {
                        if let Some(key) = input.connection.handshake.keys.handshake() {
                            let next = conn::Epoch::Handshake
                                .bind(&input.connection.receive.packet_numbers.spaces)
                                .expected_pn();
                            let open = match key.decrypt(long, next) {
                                Ok(open) => open,
                                Err(
                                    error @ conn::Error::Crypto(
                                        crypto::Error::Decrypt(_)
                                        | crypto::Error::Sample { .. }
                                        | crypto::Error::Number { .. },
                                    ),
                                ) => {
                                    failure.get_or_insert(error);
                                    continue;
                                }
                                Err(error) => return Self::fail(input, &mut token, error),
                            };
                            token = None;
                            if !closing && !input.connection.egress.activity.peer_address_validated
                            {
                                input.connection.egress.activity.peer_address_validated = true;
                                input
                                    .context
                                    .record(conn::Effects::from_bits(conn::Effects::TRANSMIT));
                            }
                            policy.try_process(input, bodies::Packet::long(open, now), sink)?;
                        }
                    }
                }
            }
            match Self::reset(input, &mut token) {
                Reset::Received => Ok(()),
                Reset::Absent => failure.map_or(Ok(()), Err),
            }
        })();
        (result, token)
    }

    fn reset(
        input: &mut ingress::Input<'_, DOMAIN, B, RECONCILE, D>,
        token: &mut Option<reset::Token>,
    ) -> Reset {
        let Some(candidate) = *token else {
            return Reset::Absent;
        };
        match input.connection.reset(None, candidate) {
            machine::Reset::Accept => {
                *token = None;
                input
                    .context
                    .record(conn::Effects::from_bits(conn::Effects::LIFECYCLE));
                Reset::Received
            }
            machine::Reset::Ignore => Reset::Absent,
        }
    }

    /// Authenticated protocol failures retain their exact error, not the reset tail.
    fn fail(
        input: &mut ingress::Input<'_, DOMAIN, B, RECONCILE, D>,
        token: &mut Option<reset::Token>,
        error: conn::Error,
    ) -> Result<(), conn::Error> {
        if !matches!(
            error,
            conn::Error::Short(_)
                | conn::Error::Long(_)
                | conn::Error::AeadLimitReached
                | conn::Error::Crypto(
                    crypto::Error::Decrypt(_)
                        | crypto::Error::HeaderDecrypt(_)
                        | crypto::Error::Sample { .. }
                        | crypto::Error::Number { .. }
                )
        ) {
            *token = None;
            return input.reject(error);
        }
        match Self::reset(input, token) {
            Reset::Received => Ok(()),
            Reset::Absent => input.reject(error),
        }
    }
}
