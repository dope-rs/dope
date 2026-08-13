use std::{cmp, time};

use dope_core::{
    driver::route::{self, table},
    io::socket::option,
};
use o3::collections::{
    self, heap,
    queue::round,
    slab::{self, key},
};

use crate::{
    connector::attempt,
    service::{self, health, reconcile},
};

mod control;
mod dials;

pub(super) enum Member {}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(super) struct MemberKey(key::Handle<Member>);

impl MemberKey {
    const fn index(self) -> usize {
        self.0.index() as usize
    }

    const fn generation(self) -> key::Generation {
        self.0.generation()
    }
}

#[repr(transparent)]
struct Unscheduled(MemberKey);

impl Unscheduled {
    const fn member(&self) -> MemberKey {
        self.0
    }
}

const _: () = {
    assert!(std::mem::size_of::<Unscheduled>() == std::mem::size_of::<MemberKey>());
    assert!(std::mem::size_of::<Option<Unscheduled>>() == std::mem::size_of::<Option<MemberKey>>());
};

struct Retry {
    member: MemberKey,
    retry_at: time::Instant,
}

impl PartialEq for Retry {
    fn eq(&self, other: &Self) -> bool {
        self.retry_at == other.retry_at && self.member == other.member
    }
}

impl Eq for Retry {}

impl PartialOrd for Retry {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Retry {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.retry_at
            .cmp(&other.retry_at)
            .then_with(|| self.member.index().cmp(&other.member.index()))
            .then_with(|| {
                self.member
                    .generation()
                    .get()
                    .cmp(&other.member.generation().get())
            })
    }
}

struct Scheduler {
    ready: round::Robin,
    retries: heap::Min<Retry>,
}

impl Scheduler {
    fn try_with_capacity(capacity: usize) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            ready: round::Robin::try_with_capacity(capacity)?,
            retries: heap::Min::try_with_capacity(capacity)?,
        })
    }

    fn recover_ready(&mut self, member: MemberKey) {
        if self.ready.insert(member.index()) {
            let _ = self.retries.remove(member.index());
        }
    }

    fn admit_ready(&mut self, member: Unscheduled) {
        let inserted = self.ready.insert(member.member().index());
        debug_assert!(inserted);
    }

    fn schedule_backoff(&mut self, member: MemberKey, retry_at: time::Instant) {
        self.ready.remove(member.index());
        self.retries.set(member.0, Retry { member, retry_at });
    }

    fn reorder_ready(&mut self, member: MemberKey) {
        if self.ready.remove(member.index()) {
            self.ready.insert(member.index());
        }
    }

    fn remove(&mut self, member: MemberKey) {
        self.ready.remove(member.index());
        let _ = self.retries.remove(member.index());
    }

    fn pop_due(&mut self, now: time::Instant) -> Option<Unscheduled> {
        let (_, retry) = self.retries.pop_if(|retry| retry.retry_at <= now)?;
        Some(Unscheduled(retry.member))
    }

    fn next_ready(&mut self) -> Option<u32> {
        self.ready.next_index()
    }

    fn remove_ready_index(&mut self, index: u32) {
        self.ready.remove(index as usize);
    }

    fn retry_at(&self) -> Option<time::Instant> {
        self.retries.peek().map(|(_, retry)| retry.retry_at)
    }
}

struct Record<I, A> {
    id: I,
    addr: A,
    failures: health::Failures,
}

struct Membership<I, A> {
    revision: service::Revision,
    records: slab::Exclusive<Record<I, A>, Member>,
    scheduler: Scheduler,
}

struct Choice<'a, A> {
    member: MemberKey,
    addr: &'a A,
}

impl<I, A> Membership<I, A> {
    fn contains(&self, member: MemberKey) -> bool {
        self.records.get(member.0).is_some()
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn recover(&mut self, member: MemberKey) {
        if let Some(record) = self.records.get_mut(member.0) {
            record.failures.reset();
            self.scheduler.recover_ready(member);
        }
    }

    fn fail(&mut self, member: MemberKey, backoff: &mut health::Backoff, now: time::Instant) {
        if let Some(record) = self.records.get_mut(member.0) {
            let retry_at = backoff.retry_at(&mut record.failures, now);
            self.scheduler.schedule_backoff(member, retry_at);
        }
    }

    fn reorder_ready(&mut self, member: MemberKey) {
        self.scheduler.reorder_ready(member);
    }

    fn promote_one(&mut self, now: time::Instant) {
        while let Some(member) = self.scheduler.pop_due(now) {
            if self.records.get(member.member().0).is_some() {
                self.scheduler.admit_ready(member);
                return;
            }
        }
    }

    fn choose(&mut self, now: time::Instant) -> Result<Choice<'_, A>, Option<time::Instant>> {
        self.promote_one(now);
        let records = &mut self.records;
        let scheduler = &mut self.scheduler;
        let member = loop {
            let Some(index) = scheduler.next_ready() else {
                return Err(scheduler.retry_at());
            };
            let Some(member) = records.slots().key(index) else {
                scheduler.remove_ready_index(index);
                continue;
            };
            break member;
        };
        let Some(record) = records.get(member) else {
            scheduler.remove(MemberKey(member));
            return Err(scheduler.retry_at());
        };
        Ok(Choice {
            member: MemberKey(member),
            addr: &record.addr,
        })
    }

    fn remove(&mut self, member: MemberKey) {
        self.scheduler.remove(member);
        drop(self.records.remove(member.0));
    }

    fn retire_absent(&mut self, endpoints: &[service::Endpoint<I, A>])
    where
        I: Eq,
    {
        for index in 0..self.records.capacity() {
            let Ok(index) = u32::try_from(index) else {
                break;
            };
            let retired = self
                .records
                .slots()
                .index(index)
                .and_then(|(record, member)| {
                    (!endpoints.iter().any(|endpoint| &record.id == endpoint.id()))
                        .then_some(MemberKey(member))
                });
            if let Some(member) = retired {
                self.remove(member);
            }
        }
    }

    fn replace_addr(&mut self, id: &I, addr: A) -> Result<MemberKey, A>
    where
        I: Eq,
    {
        for index in 0..self.records.capacity() {
            let Ok(index) = u32::try_from(index) else {
                break;
            };
            if let Some((record, member)) = self.records.slots().index_mut(index)
                && &record.id == id
            {
                record.addr = addr;
                return Ok(MemberKey(member));
            }
        }
        Err(addr)
    }

    fn insert(&mut self, id: I, addr: A) -> Result<MemberKey, Record<I, A>> {
        let record = Record {
            id,
            addr,
            failures: health::Failures::new(),
        };
        let member = self.records.insert(record).map(MemberKey)?;
        self.scheduler.admit_ready(Unscheduled(member));
        Ok(member)
    }
}

#[repr(transparent)]
struct Plan<I, A, const N: usize> {
    snapshot: service::Snapshot<service::Endpoint<I, A>, N>,
}

#[doc(hidden)]
pub struct Pool<
    T: dope_net::Transport,
    R: reconcile::Policy,
    I = <T as dope_net::Transport>::Addr,
    const N: usize = 16,
> where
    I: Eq,
{
    membership: Membership<I, T::Addr>,
    dials: dials::Dials<attempt::Plan<T>>,
    options: option::StreamOptions,
    backoff: health::Backoff,
    refresh: Option<health::Cause>,
    retirements: R::Retirements,
}

impl<T, R, I, const N: usize> attempt::Contract for Pool<T, R, I, N>
where
    T: dope_net::Transport,
    R: reconcile::Policy,
    I: Eq,
{
}

impl<T, R, I, const N: usize> Pool<T, R, I, N>
where
    T: dope_net::Transport,
    R: reconcile::Policy,
    I: Eq,
{
    pub(crate) fn try_new(
        connections: usize,
        backoff: health::Backoff,
        options: option::StreamOptions,
    ) -> Result<Self, collections::AllocationError> {
        let () = service::EndpointCount::<N>::BOUNDED;
        let capacity = slab::Capacity::new(N as u32);
        Ok(Self {
            membership: Membership {
                revision: service::Revision::INITIAL,
                records: slab::Exclusive::try_with_capacity(capacity)?,
                scheduler: Scheduler::try_with_capacity(capacity.get())?,
            },
            dials: dials::Dials::try_with_capacity(connections)?,
            options,
            backoff,
            refresh: None,
            retirements: Default::default(),
        })
    }

    pub(crate) fn take_refresh(&mut self) -> Option<health::Cause> {
        self.refresh.take()
    }

    pub(crate) const fn has_refresh(&self) -> bool {
        self.refresh.is_some()
    }

    pub(crate) fn reconcile(
        &mut self,
        snapshot: service::Snapshot<service::Endpoint<I, T::Addr>, N>,
        capacity: table::Capacity,
    ) -> Result<service::Change, service::ReconcileError> {
        let plan = self.prepare(snapshot)?;
        self.commit(plan, capacity)
    }

    fn prepare(
        &mut self,
        snapshot: service::Snapshot<service::Endpoint<I, T::Addr>, N>,
    ) -> Result<Plan<I, T::Addr, N>, service::ReconcileError> {
        if snapshot.revision() <= self.membership.revision {
            return Err(service::ReconcileError::Stale {
                current: self.membership.revision,
                incoming: snapshot.revision(),
            });
        }
        self.validate(snapshot)
    }

    fn validate(
        &mut self,
        snapshot: service::Snapshot<service::Endpoint<I, T::Addr>, N>,
    ) -> Result<Plan<I, T::Addr, N>, service::ReconcileError> {
        let endpoints = snapshot.endpoints();
        Self::validate_unique(endpoints)?;
        self.validate_member_capacity(endpoints)?;
        Ok(Plan { snapshot })
    }

    fn validate_unique(
        endpoints: &[service::Endpoint<I, T::Addr>],
    ) -> Result<(), service::ReconcileError> {
        for (index, endpoint) in endpoints.iter().enumerate() {
            if endpoints[..index]
                .iter()
                .any(|seen| seen.id() == endpoint.id())
            {
                return Err(service::ReconcileError::Duplicate);
            }
        }
        Ok(())
    }

    fn validate_member_capacity(
        &mut self,
        endpoints: &[service::Endpoint<I, T::Addr>],
    ) -> Result<(), service::ReconcileError> {
        let mut retained = 0;
        let mut reusable = 0;
        for index in 0..self.membership.records.capacity() {
            let Ok(index) = u32::try_from(index) else {
                break;
            };
            if let Some((record, member)) = self.membership.records.slots().index(index) {
                if endpoints.iter().any(|endpoint| &record.id == endpoint.id()) {
                    retained += 1;
                } else if member.generation().checked_add(1).is_some() {
                    reusable += 1;
                }
            }
        }
        let limit = retained + self.membership.records.available() + reusable;
        if endpoints.len() > limit {
            return Err(service::ReconcileError::Capacity { limit });
        }
        Ok(())
    }

    fn commit(
        &mut self,
        plan: Plan<I, T::Addr, N>,
        capacity: table::Capacity,
    ) -> Result<service::Change, service::ReconcileError> {
        let revision = plan.snapshot.revision();
        let change = self.install(plan)?;
        self.membership.revision = revision;
        if change.retired != 0 {
            <R as reconcile::Supported>::arm(&mut self.retirements, capacity);
        }
        self.dials.refresh_pending(!self.membership.is_empty());
        Ok(change)
    }

    fn install(
        &mut self,
        plan: Plan<I, T::Addr, N>,
    ) -> Result<service::Change, service::ReconcileError> {
        let snapshot = plan.snapshot;
        let previous = self.membership.records.len();
        self.membership.retire_absent(snapshot.endpoints());
        let retained = self.membership.records.len();
        let retired = previous - retained;
        let mut added = 0;
        for endpoint in snapshot.into_endpoints() {
            let (id, addr) = endpoint.into_parts();
            match self.membership.replace_addr(&id, addr) {
                Ok(member) => self.membership.reorder_ready(member),
                Err(addr) => {
                    self.membership
                        .insert(id, addr)
                        .map_err(|_| service::ReconcileError::Capacity { limit: N })?;
                    added += 1;
                }
            }
        }

        Ok(service::Change {
            added,
            retained,
            retired,
        })
    }

    pub(crate) fn next_retirement(
        &mut self,
        capacity: table::Capacity,
    ) -> Option<route::SlotIndex> {
        <R as reconcile::Supported>::next(&mut self.retirements, capacity)
    }

    pub(crate) fn has_retirements(&self) -> bool {
        <R as reconcile::Supported>::pending(&self.retirements)
    }

    pub(crate) fn should_retire<const ID: u8>(&self, key: attempt::Id<'_, ID>) -> bool {
        let Some(member) = self.dials.active(key) else {
            return false;
        };
        !self.membership.contains(member)
    }

    fn record_failure(&mut self, member: MemberKey, cause: health::Cause, now: time::Instant) {
        self.membership.fail(member, &mut self.backoff, now);
        self.refresh = Some(cause);
    }
}
