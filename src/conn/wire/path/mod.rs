use std::{num, time};

use o3::depot::capacity;

pub(crate) mod address;
pub(crate) mod local;
pub(crate) mod origin;
pub(crate) mod peer;
pub mod probe;
pub(crate) mod reset;
pub(crate) mod setup;
pub(crate) mod view;

use crate::conn;
use crate::conn::output::control;
use crate::proto::path::Peer;
use crate::wire::frame::decode;
use crate::wire::packet::{cid, header};

use address::state;
pub(crate) use local::{LocalCidKey, RouteKey};

pub(in crate::conn) struct State {
    pub(in crate::conn) peer: peer::Peer,
    pub(in crate::conn) local: local::State,
    pub(in crate::conn) origin: origin::State,
    address: state::State,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(in crate::conn) struct Key(num::NonZero<u64>);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(in crate::conn) struct ControlKey(num::NonZero<u64>);

#[must_use]
pub(in crate::conn) struct OutputPermit {
    route: OutputRoute,
    ceiling: usize,
}

impl OutputPermit {
    pub(in crate::conn) const fn ceiling(&self) -> usize {
        self.ceiling
    }

    pub(in crate::conn) fn into_route(self) -> OutputRoute {
        self.route
    }

    pub(in crate::conn) const fn route(&self) -> OutputRoute {
        self.route
    }
}

#[derive(Clone, Copy)]
pub(in crate::conn) struct OutputRoute {
    path: Option<Key>,
}

impl OutputRoute {
    pub(in crate::conn) const ACTIVE: Self = Self { path: None };

    pub(in crate::conn) const fn path(self) -> Option<Key> {
        self.path
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Address {
    Capacity {
        address: Peer,
        reason: capacity::Reason,
    },
    Keys {
        address: Peer,
        next: u64,
    },
    Bytes {
        address: Peer,
        received: u64,
        bytes: usize,
    },
    OuterBytes {
        address: Peer,
        received: u64,
        bytes: usize,
    },
    Deadline {
        now: time::Instant,
        period: time::Duration,
    },
    Cid {
        address: Peer,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum Error {
    Policy { active: Peer, source: Peer },
    Address(Address),
    Peer(capacity::Reason),
    Probe(probe::Error),
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::conn) enum Migration {
    Pending,
    Committed,
}

pub(in crate::conn) enum Admission {
    Validate,
    Respond,
    Preserve,
}

/// A newer non-probing packet selected under the original packet owner.
pub(in crate::conn) struct Intent(header::Number);

impl State {
    pub(in crate::conn) fn new(setup: setup::Setup<'_>) -> Self {
        let setup::Setup {
            identities:
                setup::Identities {
                    local_cid: local,
                    original_dcid: original,
                    peer_cid: peer,
                    peer_first_scid: first,
                },
            tokens,
            peer_shard: shard,
            stateless_reset_secret: reset,
            options,
        } = setup;
        let side = tokens.initiator();
        let limit = options.cid_limit();
        let fixed = options.is_fixed();
        Self {
            peer: peer::Peer::new(peer, shard),
            local: local::State::new(local, reset, limit),
            origin: origin::State::new(original, first, tokens),
            address: state::State::new(side, fixed),
        }
    }

    pub(in crate::conn) const fn ingress(&self) -> view::Ingress<'_> {
        view::Ingress::new(self)
    }

    pub(in crate::conn) const fn output(&self) -> view::Output<'_> {
        view::Output::new(self)
    }

    pub(in crate::conn) fn bind(&mut self, address: Peer) {
        if self.address.active.is_none() {
            self.address.active = Some(address);
        }
    }

    pub(in crate::conn) fn begin(
        &mut self,
        address: Peer,
        bytes: usize,
        outer_bytes: usize,
        token: Option<[[u8; 8]; 2]>,
    ) {
        self.address.ingress = Some(state::Ingress {
            address,
            bytes,
            outer_bytes,
            challenge: token,
        });
    }

    pub(in crate::conn) fn end(&mut self, control: &control::Pending) {
        let seen = self
            .address
            .ingress
            .and_then(|source| self.address.key_for(source.address));
        self.address.ingress = None;
        if let Some(path) = seen {
            self.sweep(path, control);
        }
    }

    pub(in crate::conn) fn try_admit(
        &mut self,
        control: &mut control::Pending,
        now: time::Instant,
        pto: time::Duration,
        admission: Admission,
        response: bool,
    ) -> conn::Ingress<Result<Option<Key>, Error>> {
        let mut effects = conn::Effects::none();
        let result = (|| {
            let Some(source) = self.address.ingress else {
                return Ok(None);
            };
            let active = match self.address.active {
                Some(active) => active,
                None => {
                    self.address.active = Some(source.address);
                    effects.merge(conn::Effects::from_bits(conn::Effects::ROUTING));
                    source.address
                }
            };
            if source.address == active {
                return if response {
                    self.address
                        .try_observe(active, 0, 0)
                        .map(Some)
                        .map_err(Error::Address)
                } else {
                    Ok(None)
                };
            }
            if !self.address.allows(active, source.address) {
                return Err(Error::Policy {
                    active,
                    source: source.address,
                });
            }
            if matches!(admission, Admission::Preserve) {
                return Ok(None);
            }
            let candidate = self
                .address
                .candidate
                .and_then(|item| self.address.get(item.path()))
                .is_some_and(|path| path.address == source.address)
                && self.address.candidate.is_some_and(|item| {
                    !item.is_stopped() || control.probe().has_response(item.path())
                });
            if matches!(admission, Admission::Respond) {
                let previous = self
                    .address
                    .previous
                    .and_then(|item| self.address.get(item.path()))
                    .is_some_and(|path| path.address == source.address);
                if !candidate && !previous {
                    // A response cannot borrow a CID owned by another path.
                    // The matched receipt was already settled before this
                    // bounded route admission; this rejects only the reply.
                    return Err(Error::Address(Address::Cid {
                        address: source.address,
                    }));
                }
            }
            if candidate || matches!(admission, Admission::Respond) {
                let path = self
                    .address
                    .try_observe(source.address, source.bytes, source.outer_bytes)
                    .map_err(Error::Address)?;
                effects.merge(conn::Effects::from_bits(conn::Effects::TRANSMIT));
                return Ok(Some(path));
            }
            let Some(data) = source.challenge else {
                return if response {
                    Err(Error::Address(Address::Cid {
                        address: source.address,
                    }))
                } else {
                    Ok(None)
                };
            };
            let deadline = now
                .checked_add(pto)
                .ok_or(Error::Address(Address::Deadline { now, period: pto }))?;
            let old = self.address.candidate;
            if old.is_some_and(|item| control.probe().has_response(item.path())) {
                return Err(Error::Address(Address::Cid {
                    address: source.address,
                }));
            }
            if let Some(previous) = self.address.previous {
                if control.probe().has_response(previous.path())
                    && self
                        .address
                        .get(previous.path())
                        .is_some_and(|path| path.address != active)
                {
                    return Err(Error::Address(Address::Cid {
                        address: source.address,
                    }));
                }
                self.try_expire_previous(previous.path(), control)
                    .map_err(Error::Peer)?;
                effects.merge(conn::Effects::from_bits(conn::Effects::TRANSMIT));
            }
            if let Some(old) = old {
                self.peer.try_abandon(control).map_err(Error::Peer)?;
                control.challenges().abandon(old.path());
                self.address.candidate = None;
                self.sweep(old.path(), control);
                effects.merge(conn::Effects::from_bits(conn::Effects::TRANSMIT));
            }
            let path = self
                .address
                .try_observe(source.address, source.bytes, source.outer_bytes)
                .map_err(Error::Address)?;
            let previous = match self.address.try_observe(active, 0, 0) {
                Ok(previous) => previous,
                Err(error) => {
                    self.sweep(path, control);
                    return Err(Error::Address(error));
                }
            };
            if let Err(error) = control.queue().try_pair(data, [path, previous]) {
                self.sweep(path, control);
                self.sweep(previous, control);
                return Err(Error::Probe(error));
            }
            self.peer.reserve();
            self.address.candidate = Some(state::Candidate::new(path, deadline));
            self.address.previous = Some(state::Candidate::new(previous, deadline));
            effects.merge(conn::Effects::from_bits(conn::Effects::TRANSMIT));
            Ok(Some(path))
        })();
        conn::Ingress::new(result, effects)
    }

    pub(in crate::conn) fn try_service(
        &mut self,
        now: time::Instant,
        pto: time::Duration,
        token: Option<[u8; 8]>,
        control: &mut control::Pending,
    ) -> Result<conn::Effects, capacity::Reason> {
        let Some((kind, item)) = self.address.next_validation() else {
            return Ok(conn::Effects::none());
        };
        let Some(deadline) = item.deadline() else {
            return Ok(conn::Effects::none());
        };
        if now < deadline {
            return Ok(conn::Effects::none());
        }
        if item.is_expired(now, pto) {
            return self.try_expire(kind, item.path(), control);
        }
        let Some(data) = token else {
            return self.try_expire(kind, item.path(), control);
        };
        let item = match kind {
            state::Validation::Candidate => self.address.candidate.as_mut(),
            state::Validation::Previous => self.address.previous.as_mut(),
        };
        let Some(item) = item else {
            return Ok(conn::Effects::none());
        };
        let path = item.path();
        match control.queue().try_path(data, Some(path)) {
            Ok(()) => {
                if item.retry(pto).is_some() {
                    Ok(conn::Effects::from_bits(conn::Effects::TRANSMIT))
                } else {
                    self.try_expire(kind, path, control)
                }
            }
            Err(
                probe::Error::Duplicate { .. }
                | probe::Error::Capacity { .. }
                | probe::Error::Keys { .. }
                | probe::Error::State { .. },
            ) => self.try_expire(kind, path, control),
        }
    }

    fn try_expire(
        &mut self,
        kind: state::Validation,
        path: Key,
        control: &mut control::Pending,
    ) -> Result<conn::Effects, capacity::Reason> {
        match kind {
            state::Validation::Candidate => {
                control.challenges().abandon(path);
                if control.probe().has_response(path) {
                    if let Some(item) = self.address.candidate.as_mut() {
                        item.stop();
                    }
                } else {
                    self.peer.try_abandon(control)?;
                    self.address.candidate = None;
                    self.sweep(path, control);
                }
            }
            state::Validation::Previous => self.try_expire_previous(path, control)?,
        }
        Ok(conn::Effects::from_bits(conn::Effects::TRANSMIT))
    }

    fn try_expire_previous(
        &mut self,
        path: Key,
        control: &mut control::Pending,
    ) -> Result<(), capacity::Reason> {
        // A candidate may still commit after the active path validates or
        // expires. Retain that route for replies admitted before its commit.
        if self.address.candidate.is_some() || control.probe().has_response(path) {
            control.challenges().abandon(path);
            if let Some(item) = self.address.previous.as_mut() {
                item.stop();
            }
            return Ok(());
        }
        self.peer.try_release_previous(control)?;
        control.challenges().abandon(path);
        self.address.previous = None;
        self.sweep(path, control);
        Ok(())
    }

    pub(in crate::conn) fn transmit(
        &mut self,
        path: Option<Key>,
        bytes: usize,
        outer_bytes: usize,
        control: &control::Pending,
    ) -> conn::Effects {
        'record: {
            let Some(path) = path else {
                self.peer.use_active();
                break 'record;
            };
            let active = self.address.active;
            let Some(item) = self.address.get_mut(path) else {
                break 'record;
            };
            if Some(item.address) == active {
                self.peer.use_active();
            } else {
                self.peer.use_selected();
                let Some(sent) = item.sent.checked_add(bytes as u64) else {
                    break 'record;
                };
                let Some(outer_sent) = item.outer_sent.checked_add(outer_bytes as u64) else {
                    break 'record;
                };
                item.sent = sent;
                item.outer_sent = outer_sent;
            }
            self.sweep(path, control);
        }
        if self.peer.is_dirty() {
            conn::Effects::from_bits(conn::Effects::ROUTING)
        } else {
            conn::Effects::none()
        }
    }

    pub(in crate::conn) fn try_respond(
        &mut self,
        token: [u8; 8],
        now: time::Instant,
        control: &mut control::Pending,
    ) -> Result<probe::Completion, capacity::Reason> {
        let result = control.queue().try_respond(token)?;
        let probe::Completion::Accepted {
            path: Some(key),
            expanded,
        } = result
        else {
            return Ok(result);
        };
        if let Some(item) = self
            .address
            .candidate
            .as_mut()
            .filter(|item| item.path() == key)
        {
            if expanded {
                item.validate();
            } else {
                item.expand(now);
            }
            control.challenges().abandon(key);
        }
        if let Some(path) = self.address.get_mut(key) {
            path.validated = true;
        }
        if self.address.previous.is_some_and(|item| item.path() == key) {
            self.try_expire_previous(key, control)?;
        }
        Ok(result)
    }

    /// Only the newest authenticated non-probing 1-RTT packet carries intent.
    pub(in crate::conn) fn intent(&self, number: header::Number) -> Option<Intent> {
        if self
            .address
            .highest_non_probing
            .is_some_and(|prior| number <= prior)
        {
            return None;
        }
        Some(Intent(number))
    }

    pub(in crate::conn) fn observe(&mut self, intent: Intent) {
        self.address.highest_non_probing = Some(intent.0);
        let ingress = self.ingress().response();
        if let Some(item) = self.address.candidate.as_mut() {
            item.observe(ingress == Some(item.path()));
        }
    }

    /// Reachability and the latest migration intent may arrive in either order.
    pub(in crate::conn) fn try_migrate(
        &mut self,
        control: &mut control::Pending,
    ) -> Result<Migration, capacity::Reason> {
        let Some((path, address)) = self.candidate() else {
            return Ok(Migration::Pending);
        };
        if self
            .peer
            .try_activate(control, self.address.previous.is_some())?
            == Migration::Pending
        {
            return Ok(Migration::Pending);
        }
        self.migrate(path, address, control);
        Ok(Migration::Committed)
    }

    fn candidate(&self) -> Option<(Key, Peer)> {
        let item = self.address.candidate.filter(|item| item.is_complete())?;
        let address = self.address.get(item.path())?.address;
        Some((item.path(), address))
    }

    fn migrate(&mut self, path: Key, address: Peer, control: &mut control::Pending) {
        control.challenges().abandon(path);
        self.address.active = Some(address);
        self.address.candidate = None;
        self.sweep(path, control);
    }

    fn sweep(&mut self, path: Key, control: &control::Pending) {
        if self
            .address
            .candidate
            .is_some_and(|item| item.path() == path)
            || self
                .address
                .previous
                .is_some_and(|item| item.path() == path)
            || control.probe().has_path(path)
        {
            return;
        }
        self.address.remove(path);
    }

    pub(in crate::conn) fn set_peer(&mut self, id: cid::Id) -> Result<(), conn::Error> {
        self.peer.set_id(id)?;
        self.origin.peer_first_scid = Some(id);
        Ok(())
    }

    pub(in crate::conn) fn accept(
        &mut self,
        update: decode::Notice<cid::Id>,
        retire: decode::Retire,
        control: &mut control::Pending,
    ) -> Result<Migration, peer::Error> {
        let previous = self
            .address
            .previous
            .and_then(|item| self.address.get(item.path()))
            .map(|route| {
                (
                    route.address,
                    *self.peer.previous().unwrap_or_else(|| self.peer.id()),
                )
            });
        let candidate = self.candidate();
        let migration = self.peer.accept_cid(
            control,
            self.local.active_connection_id_limit,
            update,
            retire,
            candidate.is_some(),
        )?;
        if migration == Migration::Committed {
            let Some((path, address)) = candidate else {
                unreachable!("candidate")
            };
            self.migrate(path, address, control);
        }
        let previous = previous
            .filter(|(address, _)| Some(*address) != self.address.active)
            .map(|(_, cid)| cid);
        if let Some(previous) = previous
            && self.peer.previous().unwrap_or_else(|| self.peer.id()) != &previous
        {
            if let Some(path) = self.address.previous.as_mut() {
                path.stop();
                let key = path.path();
                control.challenges().abandon(key);
                if !control.probe().has_response(key) {
                    self.address.previous = None;
                    self.sweep(key, control);
                }
            }
        }
        if self.address.candidate.is_some() {
            self.peer.reserve();
        } else if previous.is_some() && self.address.previous.is_some() {
            self.peer.reserve_previous();
        }
        Ok(migration)
    }
}

const _: () = {
    assert!(std::mem::size_of::<LocalCidKey>() == 8);
    assert!(std::mem::size_of::<RouteKey>() == 8);
    assert!(std::mem::size_of::<Key>() == 8);
    assert!(std::mem::size_of::<ControlKey>() == 8);
    assert!(std::mem::size_of::<OutputRoute>() == 8);
    assert!(std::mem::size_of::<OutputPermit>() == 16);
};
