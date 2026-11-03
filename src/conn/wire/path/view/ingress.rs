use std::{marker, mem};

use crate::conn::wire::path::{self, reset};
use crate::proto::path::Peer;

/// Active-path amplification credit tied to the borrowed routing state.
pub(in crate::conn) struct Credit<'path> {
    bytes: usize,
    outer_bytes: usize,
    path: marker::PhantomData<&'path path::State>,
}

impl Credit<'_> {
    pub(in crate::conn) const fn bytes(&self) -> usize {
        self.bytes
    }

    pub(in crate::conn) const fn outer_bytes(&self) -> usize {
        self.outer_bytes
    }
}

#[derive(Clone, Copy)]
pub(in crate::conn) struct Ingress<'a> {
    path: &'a path::State,
}

impl<'a> Ingress<'a> {
    pub(in crate::conn::wire::path) const fn new(path: &'a path::State) -> Self {
        Self { path }
    }

    pub(in crate::conn) fn is_new(&self, address: Peer) -> bool {
        let Some(active) = self.path.address.active else {
            return false;
        };
        address != active
            && self
                .path
                .address
                .candidate
                .filter(|route| !route.is_stopped())
                .and_then(|route| self.path.address.get(route.path()))
                .is_none_or(|route| route.address != address)
            && self.path.address.allows(active, address)
    }

    /// Authorizes amplification credit only for the active ingress source.
    pub(in crate::conn) fn credit(&self, bytes: usize) -> Option<Credit<'_>> {
        if !self.is_active() {
            return None;
        }
        let (bytes, outer_bytes) = self
            .path
            .address
            .ingress
            .map_or((bytes, bytes), |source| (source.bytes, source.outer_bytes));
        Some(Credit {
            bytes,
            outer_bytes,
            path: marker::PhantomData,
        })
    }

    pub(in crate::conn) fn is_active(&self) -> bool {
        self.path
            .address
            .ingress
            .is_none_or(|source| self.path.address.active == Some(source.address))
    }

    pub(in crate::conn) fn has_reset(&self, source: Option<Peer>, token: reset::Token) -> bool {
        let source = source.or_else(|| self.path.address.ingress.map(|ingress| ingress.address));
        let Some(source) = source else {
            return self.path.peer.has_active(token);
        };
        if self.path.address.active == Some(source) {
            return self.path.peer.has_active(token);
        }
        if self
            .path
            .address
            .candidate
            .and_then(|route| self.path.address.get(route.path()))
            .is_some_and(|route| route.address == source)
            && self.path.peer.has_spare(token)
        {
            return true;
        }
        self.path
            .address
            .previous
            .and_then(|route| self.path.address.get(route.path()))
            .is_some_and(|route| route.address == source)
            && self.path.peer.has_previous(token)
    }

    pub(in crate::conn) fn source(&self) -> Option<Peer> {
        self.path.address.ingress.map(|source| source.address)
    }

    pub(in crate::conn) fn response(&self) -> Option<path::Key> {
        let source = self.source()?;
        (Some(source) != self.path.address.active)
            .then(|| self.path.address.key_for(source))
            .flatten()
    }

    pub(in crate::conn) fn peer(&self) -> Option<Peer> {
        self.path.address.active
    }
}

const _: () = {
    assert!(mem::size_of::<Credit<'static>>() == 2 * mem::size_of::<usize>());
    assert!(!mem::needs_drop::<Credit<'static>>());
};
