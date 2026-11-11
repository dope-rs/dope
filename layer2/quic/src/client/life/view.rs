use crate::runtime::brand;

use std::time;

use crate::client::{self, life};
use crate::{conn::id, errors};
use life::{due, endpoint};

#[repr(transparent)]
pub(in crate::client) struct View<'a, 'd, const DOMAIN: u8, B>(&'a life::Lifecycle<'d, DOMAIN, B>);

impl<'d, const DOMAIN: u8, B: client::Backoff> View<'_, 'd, DOMAIN, B> {
    pub(super) fn new<'a>(life: &'a life::Lifecycle<'d, DOMAIN, B>) -> View<'a, 'd, DOMAIN, B> {
        View(life)
    }

    pub(in crate::client) fn len(&self) -> usize {
        self.0.endpoints.capacity()
    }

    pub(in crate::client) fn live(&self) -> usize {
        self.0.endpoints.active()
    }

    /// Returns a due endpoint without removing its resident retry entry.
    pub(in crate::client) fn next_due(
        &self,
        domain: id::Domain,
        now: time::Instant,
    ) -> Option<due::Due> {
        let (index, deadline, endpoint, ready) = self.0.endpoints.peek()?;
        if *deadline > now {
            return None;
        }
        ready?;
        Some(due::Due {
            domain,
            slot: client::SlotId::from_index(index as u32),
            attempt: endpoint.attempt,
        })
    }

    pub(in crate::client) fn handle(
        &self,
        slot: client::SlotId,
    ) -> Option<id::Handle<id::Driver<'d>, DOMAIN>> {
        self.0
            .endpoints
            .get(slot.index() as usize)
            .and_then(|(_, value)| endpoint::handle(value))
    }

    pub(in crate::client) fn resolve(
        &self,
        domain: id::Domain,
        attempt: client::AttemptId,
    ) -> Result<id::Handle<id::Driver<'d>, DOMAIN>, errors::Access> {
        if attempt.domain != domain {
            return Err(errors::Access::InvalidHandle);
        }
        let (endpoint, value) = self
            .0
            .endpoints
            .get(attempt.slot.index() as usize)
            .ok_or(errors::Access::Closed)?;
        let handle = endpoint::handle(value).ok_or(errors::Access::Closed)?;
        if endpoint.attempt.generation() != attempt.generation {
            return Err(errors::Access::InvalidHandle);
        }
        Ok(handle)
    }

    pub(in crate::client) fn handle_at(
        &self,
        index: usize,
    ) -> Option<id::Handle<id::Driver<'d>, DOMAIN>> {
        self.0
            .endpoints
            .get(index)
            .and_then(|(_, value)| endpoint::handle(value))
    }

    pub(in crate::client) fn bind<'scope>(
        &self,
        brand: brand::Token<'scope>,
        handle: id::Handle<id::Driver<'d>, DOMAIN>,
    ) -> Option<client::SlotScope<'scope>> {
        let bound = self.0.index(handle)?;
        let attempt = bound.attempt.id(handle.domain().identity(), bound.slot);
        Some(client::SlotScope::new(brand, attempt))
    }

    /// Only a consumed ready event admits application delivery. Pending native
    /// input retains its existing owner until that event has been delivered.
    pub(in crate::client) fn bind_active<'scope>(
        &self,
        brand: brand::Token<'scope>,
        handle: id::Handle<id::Driver<'d>, DOMAIN>,
    ) -> Result<Option<client::SlotScope<'scope>>, errors::Access> {
        let bound = self.0.index(handle).ok_or(errors::Access::InvalidHandle)?;
        if !bound.active {
            return Ok(None);
        }
        let attempt = bound.attempt.id(handle.domain().identity(), bound.slot);
        Ok(Some(client::SlotScope::new(brand, attempt)))
    }

    pub(in crate::client) fn has_events(&self) -> bool {
        self.0.endpoints.has_events()
    }

    pub(in crate::client) fn next_retry(&self) -> Option<time::Instant> {
        self.0.endpoints.peek().map(|entry| *entry.1)
    }
}
