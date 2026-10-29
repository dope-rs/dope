use std::{net, time};

use o3::base::cell::region;

use crate::{
    lookup::error,
    storage::queries::{Queries, StreamStatus, index, ledger, state, tx},
    wire::response,
};

/// Applies authenticated replies independently of server cookie cache generations.
pub(crate) fn udp<'d, const LANES: usize, const N: usize>(
    queries: &Queries<'_, 'd, LANES, N>,
    token: &mut region::Token<region::Identity<'d, 1>>,
    source: net::SocketAddr,
    packet: &[u8],
    now: time::Instant,
) {
    let msg = match response::Decoder::try_new(packet) {
        Ok(msg) => msg,
        Err(
            error::Packet::Length { .. }
            | error::Packet::Truncated { .. }
            | error::Packet::Pointer { .. }
            | error::Packet::Label { .. }
            | error::Packet::NameLength { .. }
            | error::Packet::PointerHops { .. }
            | error::Packet::RecordName { .. },
        ) => return,
    };
    let id = msg.transaction();
    let retry = state::Retry::from_config(queries.config);
    let inner = queries.get_mut(token);
    let Some(branch) = inner.reg.ids.udp_owner(id) else {
        return;
    };
    let lane = branch.lane();
    if queries.is_cancelled(lane) {
        return;
    }
    let family = branch.family();
    let Some((mut wait, (data, cookie, server))) = inner.accept(
        lane,
        family,
        ledger::Filter::Live { id, now },
        |req, lease| {
            let tx::Lease::Udp { cookie, .. } = lease else {
                return None;
            };
            if req.stream().is_some() || source != req.address() {
                return None;
            }
            let data = match msg.udp::<N>(req.name(), family, cookie.client()) {
                Ok(data) => data,
                Err(
                    error::Malformed::Packet(_)
                    | error::Malformed::Flags { .. }
                    | error::Malformed::QuestionCount { .. }
                    | error::Malformed::QuestionName(_)
                    | error::Malformed::QuestionKind { .. }
                    | error::Malformed::QuestionClass { .. }
                    | error::Malformed::OptDuplicate { .. }
                    | error::Malformed::OptName(_)
                    | error::Malformed::Cookie(_)
                    | error::Malformed::Address(_)
                    | error::Malformed::Trailing { .. }
                    | error::Malformed::AliasRecords { .. }
                    | error::Malformed::AliasConflict
                    | error::Malformed::AliasHops { .. }
                    | error::Malformed::Name(_),
                ) => return None,
            };
            Some((data, *cookie, req.server()))
        },
    ) else {
        return;
    };
    match data {
        response::Datagram::Authenticated {
            reply,
            server_cookie: value,
        } => {
            wait.cache(cookie, server, value);
            wait.apply(reply, retry, now);
        }
        response::Datagram::BadCookie {
            server_cookie: value,
        } => {
            if wait.cache(cookie, server, value) {
                wait.cookie();
            } else {
                wait.stream();
            }
        }
        response::Datagram::StreamRequired => wait.stream(),
    }
    inner.complete(lane, now);
}

pub(in crate::storage) fn stream<'d, const LANES: usize, const N: usize>(
    queries: &Queries<'_, 'd, LANES, N>,
    token: &mut region::Token<region::Identity<'d, 1>>,
    server: index::Server,
    msg: response::Decoder<'_>,
    now: time::Instant,
) -> StreamStatus {
    let id = msg.transaction();
    let retry = state::Retry::from_config(queries.config);
    let inner = queries.get_mut(token);
    if let Some(branch) = inner.reg.ids.stream_owner(id) {
        let lane = branch.lane();
        if queries.is_cancelled(lane) {
            return StreamStatus::read(queries, token, server);
        }
        let family = branch.family();
        let applied = match inner.accept(
            lane,
            family,
            ledger::Filter::Live { id, now },
            |req, lease| {
                if !matches!(lease, tx::Lease::Tcp { .. }) || req.stream() != Some(server) {
                    return None;
                }
                Some(msg.decode::<N>(req.name(), family))
            },
        ) {
            Some((wait, decoded)) => {
                match decoded {
                    Ok(reply) => wait.apply(reply, retry, now),
                    Err(error) => wait.failed(retry, now, error::Kind::Malformed(error)),
                }
                true
            }
            None => false,
        };
        if applied {
            inner.complete(lane, now);
        }
    }
    StreamStatus::read(queries, token, server)
}
