use std::{num, time};

use crate::conn;
use crate::conn::output::{Activity, control};
use crate::conn::wire::path;
use crate::proto::path::Peer;
use crate::proto::recovery::{numbers, pmtud};
use crate::wire::packet::cid;

#[derive(Clone, Copy)]
pub(in crate::conn) struct Output<'a> {
    path: &'a path::State,
}

impl<'a> Output<'a> {
    pub(in crate::conn::wire::path) const fn new(path: &'a path::State) -> Self {
        Self { path }
    }

    pub(in crate::conn) fn deadline(&self) -> Option<time::Instant> {
        self.path
            .address
            .next_validation()
            .and_then(|(_, item)| item.deadline())
    }

    pub(in crate::conn) fn is_retry_due(&self, now: time::Instant) -> bool {
        self.path
            .address
            .next_validation()
            .is_some_and(|(_, item)| item.is_retry_due(now))
    }

    pub(in crate::conn) fn max_overhead(&self) -> usize {
        self.path
            .address
            .active
            .into_iter()
            .chain(self.path.address.observed.iter().map(|path| path.address))
            .map(Peer::overhead)
            .max()
            .unwrap_or(0)
    }

    pub(in crate::conn) fn max_payload(&self) -> num::NonZero<u16> {
        self.path
            .address
            .active
            .as_ref()
            .map_or(num::NonZero::<u16>::MAX, |peer| peer.max_payload())
    }

    pub(in crate::conn) fn permit(
        &self,
        control: &control::Pending,
        ceiling: usize,
        packet: &numbers::Packet,
    ) -> path::OutputPermit {
        control
            .probe()
            .find_path(control, |path| self.try_path(path, ceiling, packet))
            .unwrap_or(path::OutputPermit {
                route: path::OutputRoute::ACTIVE,
                ceiling: ceiling.min(usize::from(self.max_payload().get())),
            })
    }

    fn try_path(
        &self,
        path: path::Key,
        mut ceiling: usize,
        packet: &numbers::Packet,
    ) -> Option<path::OutputPermit> {
        let observed = self.path.address.get(path)?;
        let cid = self.cid_for(path, observed)?;
        ceiling = ceiling.min(usize::from(observed.address.max_payload().get()));
        let mut expanded = observed.validated;
        if !observed.validated {
            let inner = Activity::credit(false, observed.received, observed.sent);
            let outer = Activity::credit(false, observed.outer_received, observed.outer_sent)
                .saturating_sub(observed.address.overhead() as u64);
            let room = inner.min(outer).min(usize::MAX as u64) as usize;
            expanded = room >= pmtud::BASE_PMTU as usize;
            ceiling = ceiling.min(room);
        }
        let route = path::OutputRoute { path: Some(path) };
        let minimum = 1usize
            .saturating_add(cid.len())
            .saturating_add(usize::from(packet.encoded_len()))
            .saturating_add(9)
            .saturating_add(conn::TAG_LEN);
        let minimum = if expanded {
            minimum.max(pmtud::BASE_PMTU as usize)
        } else {
            minimum
        };
        (ceiling >= minimum).then_some(path::OutputPermit { route, ceiling })
    }

    #[inline]
    pub(in crate::conn) fn target(&self, route: path::OutputRoute) -> Option<Option<Peer>> {
        match route.path() {
            Some(path) => self
                .path
                .address
                .get(path)
                .map(|observed| Some(observed.address)),
            None => Some(None),
        }
    }

    pub(in crate::conn) fn cid(&self, route: path::OutputRoute) -> cid::Ref<'a> {
        let Some(path) = route.path() else {
            return self.path.peer.id().as_ref_id();
        };
        let Some(observed) = self.path.address.get(path) else {
            unreachable!("path")
        };
        self.cid_for(path, observed)
            .unwrap_or_else(|| unreachable!("pathcid"))
    }

    #[inline(always)]
    fn cid_for(
        &self,
        path: path::Key,
        observed: &path::address::state::Observed,
    ) -> Option<cid::Ref<'a>> {
        let peer = if self.path.address.active.as_ref() == Some(&observed.address) {
            Some(self.path.peer.id())
        } else if self
            .path
            .address
            .candidate
            .is_some_and(|item| item.path() == path)
        {
            self.path.peer.spare()
        } else if self
            .path
            .address
            .previous
            .is_some_and(|item| item.path() == path)
        {
            self.path.peer.previous()
        } else {
            None
        };
        peer.map(cid::Id::as_ref_id)
    }

    pub(in crate::conn) fn has_output(&self, control: &control::Pending) -> bool {
        control.probe().has_output(control)
    }
}
