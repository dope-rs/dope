use std::{ops, pin, time};

use dope::runtime::civil;
use o3::buffer::{self, write::Sink as _};

use crate::{
    conn, errors,
    proto::{epoch, path::Peer, recovery::pmtud},
    runtime::{brand, mux, routing::registry},
    security::{qkdf, relay::Ticket, secrets},
    stream,
    wire::packet::{
        self,
        long::{self, retry},
    },
};
use conn::{id, life::accept, wire::path};
use mux::{auth, handler, output, router};
use registry::{Directory, Scope};
use router::routing;
use routing::{state, txn};
use state::cids;
use txn::{install, plan};

pub(crate) struct Turn<
    'a,
    A: auth::Mode<DOMAIN>,
    H: handler::Handler<DOMAIN, B, A::Send, A::Identity>,
    const DOMAIN: u8,
    B: stream::Recv,
> {
    pub(in crate::runtime::mux) state: &'a mut router::State<A, H::Connection, DOMAIN, B>,
    pub(in crate::runtime::mux) handler: pin::Pin<&'a mut H>,
}

impl<
    'a,
    A: auth::Mode<DOMAIN>,
    H: handler::Handler<DOMAIN, B, A::Send, A::Identity>,
    const DOMAIN: u8,
    B: stream::Recv,
> Turn<'a, A, H, DOMAIN, B>
{
    pub(crate) fn new(mux: pin::Pin<&'a mut router::Router<H, DOMAIN, B, A>>) -> Self {
        let mux = mux.project();
        Self {
            state: mux.state,
            handler: mux.handler,
        }
    }

    pub(in crate::runtime::mux) fn route<I: buffer::raw::Mutable, S>(
        &mut self,
        from: Peer,
        input: I,
        now: time::Instant,
        epoch: epoch::Sample<S>,
        apply: impl for<'storage, 'id, 'scope> FnOnce(
            brand::Branded<'scope, Option<path::LocalCidKey>>,
            pin::Pin<&'scope mut H>,
            &'scope mut registry::Scope<
                'storage,
                'id,
                H::Connection,
                DOMAIN,
                B,
                A::Send,
                A::Session,
            >,
            registry::Key<'id>,
            &'scope mut router::Binding<A, DOMAIN>,
            &'scope mut conn::Workspace,
            long::Datagram<I>,
        ) -> Result<(), errors::Receive>,
    ) -> Result<(), errors::Receive>
    where
        S: civil::Source,
    {
        brand::Token::with_mut(self, |scope, turn| {
            let state = &mut *turn.state;
            let mut sink = turn.handler.as_mut();
            let routed = Directory::with(&mut state.registry, |owner| {
                let header = routing::packets::Header::new(input.as_ref());
                let Some(value) = header.dcid::<A::LocalRoute>() else {
                    return Err((input, apply));
                };
                let hit = match cids::Source::new(owner).find(value) {
                    Ok(hit) => hit,
                    Err(_) => return Err((input, apply)),
                };
                let (mut slots, key, local) = hit.into_scope();
                Ok(apply(
                    brand::Branded::new(scope.reborrow(), local),
                    sink.as_mut(),
                    &mut slots,
                    key,
                    &mut state.binding,
                    &mut state.receive_workspace,
                    long::Datagram::protected(input),
                ))
            });
            let (input, apply) = match routed {
                Ok(result) => return result,
                Err(unrouted) => unrouted,
            };
            if routing::reset::receive(state, from, input.as_ref(), now)?
                || !routing::packets::Header::new(input.as_ref()).is_initial()
                || state.server.is_none()
            {
                return Ok(());
            }
            let init = match long::Admission::try_new(input) {
                Ok(packet) => packet,
                Err(_) => return Ok(()),
            };
            Directory::with(&mut state.registry, |owner| {
                match cids::Source::new(owner).lookup(init.dcid()) {
                    cids::Lookup::Routed(hit) => {
                        let (mut slots, key, local) = hit.into_scope();
                        apply(
                            brand::Branded::new(scope.reborrow(), local),
                            sink.as_mut(),
                            &mut slots,
                            key,
                            &mut state.binding,
                            &mut state.receive_workspace,
                            long::Datagram::protected(init.into_inner()),
                        )
                    }
                    cids::Lookup::Vacant(mut alias) => {
                        // Existing CIDs were matched above; this endpoint's Initial
                        // and Retry CIDs are always at least eight bytes long.
                        if init.dcid().len() < 8
                            || state.lifecycle.admission_closed
                            || alias.is_full()
                        {
                            return Ok(());
                        }
                        let server = state
                            .server
                            .as_ref()
                            .ok_or(errors::Receive::Packet(conn::Error::HeaderDecode))?;
                        let cfg = A::config(server).0;
                        if init.datagram_len() > pmtud::MAX_PMTU as usize {
                            return Err(errors::Receive::Packet(conn::Error::PacketCeiling));
                        }
                        let dcid = alias.value();
                        let gate = |dcid: packet::cid::Ref<'_>,
                                    scid: packet::cid::Ref<'_>,
                                    token: &[u8],
                                    datagram_len: usize| {
                            let admitted = 'gate: {
                                let Some(secret) = cfg.security.retry_token_secret.as_ref() else {
                                    if !token.is_empty()
                                        && token.len() != secrets::Address::WIRE_LEN
                                    {
                                        return Ok(None);
                                    }
                                    if !cfg.security.require_address_validation {
                                        break 'gate (
                                            brand::Branded::new(
                                                scope.reborrow(),
                                                router::AddressProof::Unvalidated,
                                            ),
                                            None,
                                        );
                                    }
                                    return Ok(None);
                                };
                                let seen = epoch
                                    .observe(scope.reborrow())
                                    .map_err(errors::Receive::Civil)?;
                                if let Some(odcid) = secret.validate(&from, token, dcid, &seen) {
                                    break 'gate (
                                        brand::Branded::new(
                                            scope.reborrow(),
                                            router::AddressProof::Retry { odcid },
                                        ),
                                        Some(seen),
                                    );
                                }
                                if secrets::Address(&secret.0).is_valid(&from, token, &seen) {
                                    break 'gate (
                                        brand::Branded::new(
                                            scope.reborrow(),
                                            router::AddressProof::NewToken,
                                        ),
                                        Some(seen),
                                    );
                                }
                                if !token.is_empty() && token.len() != secrets::Address::WIRE_LEN {
                                    return Ok(None);
                                }
                                let Some(key) = state.retry.as_ref() else {
                                    break 'gate (
                                        brand::Branded::new(
                                            scope.reborrow(),
                                            router::AddressProof::Unvalidated,
                                        ),
                                        Some(seen),
                                    );
                                };
                                let expiry = seen
                                    .expire(secrets::RETRY_TOKEN_TTL)
                                    .map_err(errors::Receive::Expiry)?;
                                let Some(new_scid) =
                                    alias.new_away(&state.binding.route, &mut state.binding.issuer)
                                else {
                                    return Ok(None);
                                };
                                let Some(amp) = datagram_len.checked_mul(3) else {
                                    return Ok(None);
                                };
                                let overhead = from.overhead();
                                let Some(outer_amp) = datagram_len
                                    .checked_add(overhead)
                                    .and_then(|bytes| bytes.checked_mul(3))
                                else {
                                    return Ok(None);
                                };
                                let limit = cfg.packet_max().min(amp);
                                let len = retry::Suffix::prefix_len(scid, new_scid.as_ref_id())
                                    + secrets::Retry::encoded_len(dcid)
                                    + retry::INTEGRITY_TAG_LEN;
                                let wire_len = len + overhead;
                                if len > limit
                                    || wire_len > state.outgoing.bytes_capacity
                                    || wire_len > outer_amp
                                {
                                    return Ok(None);
                                }
                                let prefix = 1 + dcid.len();
                                let mut failure = None;
                                let emission = state.outgoing.emit_owned(
                                    wire_len,
                                    prefix + wire_len,
                                    |mut storage| {
                                        if from.is_relayed()
                                            && let Err(error) =
                                                storage.write_slice(&[0; Ticket::LEN])
                                        {
                                            return Err((storage, Some(error)));
                                        }
                                        if let Err(error) = storage.write_slices([
                                            &[dcid.length().get()],
                                            dcid.into_slice(),
                                        ]) {
                                            return Err((storage, Some(error)));
                                        }
                                        let mut storage = match retry::Suffix::encode_into(
                                            storage,
                                            packet::QUIC_V1,
                                            scid,
                                            new_scid.as_ref_id(),
                                            &[],
                                        ) {
                                            Ok(storage) => storage,
                                            Err(rejected) => {
                                                let (storage, error) = rejected.into_parts();
                                                return Err((storage, Some(error)));
                                            }
                                        };
                                        if let Err(error) = secret.issue_into(
                                            &mut storage.append(),
                                            &from,
                                            dcid,
                                            new_scid.as_ref_id(),
                                            expiry,
                                        ) {
                                            return Err((storage.into_storage(), Some(error)));
                                        }
                                        let tag = match key
                                            .tag_from_aad(&storage.storage()[overhead..])
                                        {
                                            Ok(tag) => tag,
                                            Err(error) => {
                                                failure = Some(error);
                                                return Err((storage.into_storage(), None));
                                            }
                                        };
                                        if let Err(error) = storage.append().write_slice(&tag) {
                                            return Err((storage.into_storage(), Some(error)));
                                        }
                                        if let Some(ticket) = from.ticket()
                                            && let Err(error) =
                                                storage.try_prepend(ticket.as_bytes())
                                        {
                                            return Err((storage.into_storage(), Some(error)));
                                        }
                                        Ok(output::Flight::suffix(from, storage))
                                    },
                                );
                                if let Some(error) = failure {
                                    return Err(errors::Receive::Retry(error));
                                }
                                return match emission {
                                    output::Emission::Sent | output::Emission::Skipped => Ok(None),
                                    output::Emission::Allocation { error, requested } => {
                                        Err(errors::Receive::Allocation { error, requested })
                                    }
                                };
                            };
                            Ok(Some(admitted))
                        };
                        let gate = if init.is_single() {
                            let Some(admitted) =
                                gate(init.dcid(), init.scid(), init.token(), init.datagram_len())?
                            else {
                                return Ok(());
                            };
                            ops::ControlFlow::Break(admitted)
                        } else {
                            ops::ControlFlow::Continue(gate)
                        };
                        let keys = qkdf::Initial::try_new(dcid.as_ref())
                            .and_then(qkdf::Pair::try_server)
                            .map_err(|error| {
                                errors::Receive::Accept(errors::Connect::Protocol(error.into()))
                            })?;
                        let (init, failure) =
                            init.decrypt(&keys.peer).map_err(errors::Receive::Packet)?;
                        let (proof, seen) = match gate {
                            ops::ControlFlow::Break(admitted) => admitted,
                            ops::ControlFlow::Continue(gate) => {
                                let Some(admitted) = gate(
                                    init.dcid(),
                                    init.scid(),
                                    init.token(),
                                    init.datagram_len(),
                                )?
                                else {
                                    return Ok(());
                                };
                                admitted
                            }
                        };
                        let remote = init.scid().into_owned();
                        let cap = state
                            .binding
                            .packet_cap(cfg, state.outgoing.bytes_capacity)
                            .ok_or(errors::Receive::Packet(conn::Error::PacketCeiling))?;
                        let input = init.into_inner();
                        let local = alias
                            .reserve_away(&state.binding.route, &mut state.binding.issuer)
                            .ok_or(errors::Receive::Packet(conn::Error::ConnectionIdLimit))?;
                        let cid = local.value();
                        let ids = match proof.value() {
                            router::AddressProof::Retry { odcid } => {
                                accept::Ids::retry(dcid, cid, remote, *odcid, dcid)
                            }
                            router::AddressProof::NewToken => {
                                accept::Ids::validated(dcid, cid, remote)
                            }
                            router::AddressProof::Unvalidated => {
                                accept::Ids::initial(dcid, cid, remote)
                            }
                        };
                        let token = match cfg.security.retry_token_secret.as_ref() {
                            Some(secret) => match seen.as_ref() {
                                Some(seen) => {
                                    let expiry = seen
                                        .expire(secrets::ADDRESS_TOKEN_TTL)
                                        .map_err(errors::Receive::Expiry)?;
                                    Some(secrets::Address(&secret.0).issue(&from, expiry))
                                }
                                None => None,
                            },
                            None => None,
                        };
                        let ids = ids.with_token(token);
                        let (conn, tls) = A::accept(server, ids, auth::ServerConfig(cfg, keys))
                            .map_err(errors::Receive::Accept)?;
                        match install::try_attach(
                            sink.as_mut(),
                            &mut state.binding,
                            proof.brand(),
                            plan::Plan::new(conn, tls, from, cap, now, local),
                            |sink, binding, slots, key, _| {
                                apply(
                                    brand::Branded::new(scope.reborrow(), None),
                                    sink,
                                    slots,
                                    key,
                                    binding,
                                    &mut state.receive_workspace,
                                    input,
                                )
                            },
                        )
                        .map_err(|error| {
                            errors::Receive::Accept(errors::Connect::Protocol(error.into()))
                        })? {
                            install::Outcome::Installed(value) => {
                                value?;
                                match failure {
                                    Some(error) => Err(errors::Receive::Packet(error.into())),
                                    None => Ok(()),
                                }
                            }
                            install::Outcome::Rejected => Err(errors::Receive::ApplicationRejected),
                            install::Outcome::Unavailable => {
                                Err(errors::Receive::Packet(conn::Error::EventCapacity))
                            }
                        }
                    }
                }
            })
        })
    }

    pub(in crate::runtime::mux) fn ready<R, const PLANE: usize>(
        &mut self,
        now: time::Instant,
        f: impl for<'scope> FnOnce(
            id::Handle<A::Identity, DOMAIN>,
            pin::Pin<&'scope mut H>,
            registry::Access<'scope, H::Connection, DOMAIN, B, A::Send, A::Session, false>,
        ) -> R,
    ) -> Option<R> {
        brand::Token::with_mut(self, |scope, turn| {
            let state = &mut *turn.state;
            let binding = &mut state.binding;
            let mut sink = turn.handler.as_mut();
            Scope::with(&mut state.registry, |mut slots| {
                let key = slots.pop_front::<PLANE>()?;
                let handle = slots.handle(binding.domain, key);
                let result = {
                    let mut entry = slots.entry(key);
                    f(
                        handle,
                        sink.as_mut(),
                        entry.access::<false>(scope.reborrow()),
                    )
                };
                router::State::<A, H::Connection, DOMAIN, B>::reconcile(
                    scope, &mut slots, key, binding, now, None,
                );
                Some(result)
            })
        })
    }

    pub(in crate::runtime::mux) fn try_remove(
        &mut self,
        handle: id::Handle<A::Identity, DOMAIN>,
        now: time::Instant,
    ) -> Result<(), errors::Access> {
        brand::Token::with_mut(self, |scope, turn| {
            let domain = turn.state.binding.domain;
            Scope::with(&mut turn.state.registry, |slots| {
                let key = slots
                    .bind(domain, handle)
                    .ok_or(errors::Access::InvalidHandle)?;
                slots.close(key, domain, turn.handler.as_mut(), scope, now);
                Ok(())
            })
        })
    }

    pub(in crate::runtime::mux) fn remove_at(&mut self, index: usize, now: time::Instant) -> bool {
        brand::Token::with_mut(self, |scope, turn| {
            let domain = turn.state.binding.domain;
            Scope::with(&mut turn.state.registry, |slots| {
                let Some(key) = slots.resolve(index) else {
                    return false;
                };
                slots.close(key, domain, turn.handler.as_mut(), scope, now);
                true
            })
        })
    }

    pub(in crate::runtime::mux) fn clear(&mut self, now: time::Instant) -> usize {
        let len = self.registry.capacity();
        let mut removed = 0;
        for index in 0..len {
            if self.remove_at(index, now) {
                removed += 1;
            }
        }
        removed
    }
}

impl<
    A: auth::Mode<DOMAIN>,
    H: handler::Handler<DOMAIN, B, A::Send, A::Identity>,
    const DOMAIN: u8,
    B: stream::Recv,
> ops::Deref for Turn<'_, A, H, DOMAIN, B>
{
    type Target = router::State<A, H::Connection, DOMAIN, B>;

    fn deref(&self) -> &Self::Target {
        self.state
    }
}

impl<
    A: auth::Mode<DOMAIN>,
    H: handler::Handler<DOMAIN, B, A::Send, A::Identity>,
    const DOMAIN: u8,
    B: stream::Recv,
> ops::DerefMut for Turn<'_, A, H, DOMAIN, B>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.state
    }
}
