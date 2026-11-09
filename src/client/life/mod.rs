use crate::runtime::brand;
use crate::runtime::mux;
use indexed::resident::{self, phase};
use o3::alloc::BoxSliceExt as _;
use o3::depot::{capacity, heap::indexed};
use std::time;

use crate::client;
use crate::conn::{id, life};
use client::{build, protocol};

mod due;
mod endpoint;
mod retry;
pub(super) mod topology;
pub(super) mod view;

pub(super) use due::Due;
use retry::Retry;

type Capacity = capacity::Limit<{ mux::MAX_CONNECTIONS }>;

#[derive(Clone, Copy)]
struct Bound {
    slot: client::SlotId,
    attempt: client::AttemptState,
    active: bool,
}

/// The heap and event owner retain each endpoint's exact resident phase.
/// Live cardinality belongs to consumed Active transitions in that same domain.
pub(super) struct Lifecycle<'d, const DOMAIN: u8, B> {
    endpoints: resident::Min<time::Instant, endpoint::Types<'d, DOMAIN>, Capacity>,
    handle_to_endpoint: Box<[Option<client::SlotId>]>,
    backoff: B,
    reconnect: bool,
    failure: Option<protocol::Failure>,
}

impl<'d, const DOMAIN: u8, B: client::Backoff> Lifecycle<'d, DOMAIN, B> {
    pub(super) fn try_new(
        backoff: B,
        capacity: Capacity,
        now: time::Instant,
    ) -> Result<Self, build::Error> {
        let entries =
            resident::Min::try_repeat(capacity, now, |_| (endpoint::Endpoint::default(), ()))
                .map_err(build::Error::Storage)?;
        let reverse = Box::try_repeat(capacity.get(), None).map_err(build::Error::Reverse)?;
        Ok(Self {
            endpoints: entries,
            handle_to_endpoint: reverse,
            backoff,
            reconnect: true,
            failure: None,
        })
    }

    pub(super) fn view(&self) -> view::View<'_, 'd, DOMAIN, B> {
        view::View::new(self)
    }

    /// The retained Scheduled entry owns both the deadline and this endpoint.
    pub(super) fn bind_due(&mut self, due: due::Due, handle: id::Handle<id::Driver<'d>, DOMAIN>) {
        let due::Due { slot, attempt, .. } = due;
        let index = slot.index() as usize;
        let Some(resident::Entry::Scheduled(endpoint)) = self.endpoints.entry(index) else {
            return;
        };
        if endpoint.get().0.attempt != attempt {
            return;
        }
        let Some(reverse) = self.handle_to_endpoint.get_mut(handle.index() as usize) else {
            return;
        };
        if reverse.is_some() {
            return;
        }
        endpoint.retire(endpoint::Idle::Handshaking(handle));
        *reverse = Some(slot);
    }

    pub(super) fn defer_due(
        &mut self,
        scope: brand::Token<'_>,
        due: due::Due,
        seen: time::Instant,
    ) {
        let due::Due {
            domain,
            slot,
            attempt,
        } = due;
        let index = slot.index() as usize;
        let Some(resident::Entry::Scheduled(mut endpoint)) = self.endpoints.entry(index) else {
            return;
        };
        if endpoint.get().0.attempt != attempt {
            return;
        }
        match Retry::try_new(
            &self.backoff,
            self.reconnect,
            scope.reborrow(),
            domain,
            slot,
            attempt,
            false,
            seen,
        ) {
            Ok(Retry::Scheduled { next, at }) => {
                let (mut endpoint, _) = endpoint.replace(at);
                endpoint.get_mut().0.attempt = next;
            }
            result @ (Ok(Retry::Dormant) | Err(_)) => {
                endpoint.get_mut().0.reconnect = None;
                endpoint.retire(endpoint::Idle::Dormant);
                if let Err(failure) = result {
                    self.fail(failure);
                }
            }
        }
    }

    pub(super) fn establish(&mut self, handle: id::Handle<id::Driver<'d>, DOMAIN>) {
        let Some(slot) = self
            .handle_to_endpoint
            .get(handle.index() as usize)
            .copied()
            .flatten()
        else {
            return;
        };
        let Some(resident::Entry::Idle(endpoint)) = self.endpoints.entry(slot.index() as usize)
        else {
            return;
        };
        let endpoint::Idle::Handshaking(current) = *endpoint.get().1 else {
            return;
        };
        if current != handle {
            return;
        }
        endpoint.change::<phase::Data>(handle);
    }

    pub(super) fn reconnect(
        &mut self,
        handle: id::Handle<id::Driver<'d>, DOMAIN>,
        again: Option<life::Reconnect>,
    ) {
        let Some(bound) = self.index(handle) else {
            return;
        };
        let index = bound.slot.index() as usize;
        let Some(endpoint) = self.endpoints.common_mut(index) else {
            return;
        };
        endpoint.reconnect = again;
    }

    pub(super) fn take_again(&mut self, due: &due::Due) -> Option<life::Reconnect> {
        let resident::Entry::Scheduled(mut endpoint) =
            self.endpoints.entry(due.slot.index() as usize)?
        else {
            return None;
        };
        endpoint.get_mut().0.reconnect.take()
    }

    pub(super) fn put_again(&mut self, due: &due::Due, again: life::Reconnect) {
        let Some(resident::Entry::Scheduled(mut endpoint)) =
            self.endpoints.entry(due.slot.index() as usize)
        else {
            return;
        };
        let endpoint = endpoint.get_mut().0;
        if endpoint.reconnect.is_some() {
            return;
        }
        endpoint.reconnect = Some(again);
    }

    pub(super) fn close_at(
        &mut self,
        scope: brand::Token<'_>,
        handle: id::Handle<id::Driver<'d>, DOMAIN>,
        seen: time::Instant,
    ) {
        let Some(reverse) = self.handle_to_endpoint.get_mut(handle.index() as usize) else {
            return;
        };
        let Some(slot) = *reverse else {
            return;
        };
        let Some(endpoint) = self.endpoints.entry(slot.index() as usize) else {
            return;
        };
        let failure = match endpoint {
            resident::Entry::Idle(endpoint) => {
                let endpoint::Idle::Handshaking(current) = *endpoint.get().1 else {
                    return;
                };
                if current != handle {
                    return;
                }
                close(
                    endpoint,
                    reverse,
                    &self.backoff,
                    self.reconnect,
                    scope,
                    handle.domain().identity(),
                    false,
                    seen,
                )
            }
            resident::Entry::Data(endpoint) => {
                if *endpoint.get().1 != handle {
                    return;
                }
                close(
                    endpoint,
                    reverse,
                    &self.backoff,
                    self.reconnect,
                    scope,
                    handle.domain().identity(),
                    true,
                    seen,
                )
            }
            resident::Entry::Active(endpoint) => {
                if *endpoint.get().1 != handle {
                    return;
                }
                close(
                    endpoint,
                    reverse,
                    &self.backoff,
                    self.reconnect,
                    scope,
                    handle.domain().identity(),
                    true,
                    seen,
                )
            }
            resident::Entry::Scheduled(_) | resident::Entry::Control(_) => None,
        };
        if let Some(failure) = failure {
            self.fail(failure);
        }
    }

    /// Consumes the exact close field before executing its external callback.
    pub(super) fn settle_close<'scope>(
        &mut self,
        scope: brand::Token<'scope>,
    ) -> Option<client::SlotScope<'scope>> {
        let attempt = match self.endpoints.control()? {
            resident::Control::Full(endpoint) => endpoint.change::<phase::Scheduled>(()).1,
            resident::Control::Empty(mut endpoint) => {
                endpoint.get_mut().0.reconnect = None;
                endpoint.change::<phase::Idle>(endpoint::Idle::Dormant).1
            }
        };
        Some(client::SlotScope::new(scope, attempt))
    }

    /// The Data owner supplies both the handle and its unused Active slot.
    pub(super) fn settle_ready<'scope>(
        &mut self,
        scope: brand::Token<'scope>,
    ) -> Option<(
        client::SlotScope<'scope>,
        id::Handle<id::Driver<'d>, DOMAIN>,
    )> {
        let endpoint = self.endpoints.data()?;
        let slot = client::SlotId::from_index(endpoint.index() as u32);
        let (common, handle) = endpoint.get();
        let attempt = common.attempt.id(handle.domain().identity(), slot);
        let handle = *handle;
        endpoint.change::<phase::Active>(handle);
        Some((client::SlotScope::new(scope, attempt), handle))
    }

    pub(super) fn seal(&mut self) {
        self.reconnect = false;
        self.endpoints.clear(|endpoint, ()| {
            endpoint.reconnect = None;
            endpoint::Idle::Dormant
        });
    }

    pub(super) const fn failure(&self) -> Option<protocol::Failure> {
        self.failure
    }

    pub(super) fn fail(&mut self, failure: protocol::Failure) {
        if self.failure.is_none() {
            self.failure = Some(failure);
            self.seal();
        }
    }

    fn index(&self, handle: id::Handle<id::Driver<'d>, DOMAIN>) -> Option<Bound> {
        let slot = self
            .handle_to_endpoint
            .get(handle.index() as usize)
            .copied()
            .flatten()?;
        let (common, value) = self.endpoints.get(slot.index() as usize)?;
        let active = matches!(&value, resident::Value::Active(_));
        if endpoint::handle(value)? != handle {
            return None;
        }
        Some(Bound {
            slot,
            attempt: common.attempt,
            active,
        })
    }
}

fn close<
    'd,
    const DOMAIN: u8,
    B: client::Backoff,
    F: resident::Phase<endpoint::Types<'d, DOMAIN>>,
>(
    mut endpoint: resident::Vacant<'_, time::Instant, endpoint::Types<'d, DOMAIN>, F, Capacity>,
    reverse: &mut Option<client::SlotId>,
    backoff: &B,
    again: bool,
    scope: brand::Token<'_>,
    domain: id::Domain,
    restart: bool,
    seen: time::Instant,
) -> Option<protocol::Failure> {
    let slot = client::SlotId::from_index(endpoint.index() as u32);
    let previous = endpoint.get().0.attempt;
    let plan = Retry::try_new(
        backoff,
        again,
        scope.reborrow(),
        domain,
        slot,
        previous,
        restart,
        seen,
    );
    *reverse = None;
    let attempt = previous.id(domain, slot);
    match plan {
        Ok(Retry::Scheduled { next, at }) => {
            endpoint.get_mut().0.attempt = next;
            endpoint.schedule(at, attempt);
        }
        Ok(Retry::Dormant) => {
            endpoint.change::<phase::Control>(attempt);
        }
        Err(failure) => {
            endpoint.change::<phase::Control>(attempt);
            return Some(failure);
        }
    }
    None
}
