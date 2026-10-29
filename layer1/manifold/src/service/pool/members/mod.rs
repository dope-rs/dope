use std::{marker::PhantomData, mem};

use dope_core::driver::schedule::clock;
use o3::{
    depot,
    depot::{
        capacity,
        heap::keyed,
        queue::round::{self, set},
    },
};

use crate::{
    service,
    service::{
        health,
        pool::{Member, MemberKey, Target},
        sync,
    },
};

/// Outcome of one membership transition.
/// `Missing` discards an already-stale identity; the caller retains its original member.
pub(super) enum Update {
    Applied,
    Ignored,
    Exhausted,
    Missing,
}

/// One member resolution for a pending dial's target.
pub(super) enum Peer<'h, R> {
    Ready { target: Target<'h>, output: R },
    Missing,
    Exhausted,
}

pub(super) enum Selection<'a, 'h, 'd, A> {
    Ready { target: Target<'h>, addr: &'a A },
    Idle,
    Backoff(clock::Deadline<'d>),
}

const _: () = assert!(mem::size_of::<Selection<'static, 'static, 'static, ()>>() == 32);
const _: () = assert!(mem::size_of::<Update>() == 1);

mod backoff;
mod entries;
mod failure;
mod queues;
mod record;

use record::{Exhausted, Idle, Ready, Record};

type Records<'h, I, A> = round::Set<Record<I, A>, Member, { u32::MAX }, capacity::Domain<'h>>;
type Occupied<'a, 'h, I, A, const ROOT: bool = true> =
    set::Occupied<'a, Record<I, A>, Member, { u32::MAX }, ROOT, capacity::Domain<'h>>;
type Unlinked<'a, 'h, I, A, const ROOT: bool = true> =
    set::Occupied<'a, Record<I, A>, Member, { u32::MAX }, ROOT, capacity::Domain<'h>, set::Idle>;

/// Owns canonical health, scheduled eligibility, and retry membership through every transition.
pub(super) struct Set<'h, 'd, I, A, R: sync::Policy> {
    revision: service::Revision,
    records: Records<'h, I, A>,
    queue: queues::Queue<'h, 'd>,
    desired: u32,
    policy: PhantomData<R>,
}

impl<'d, I, A, R: sync::Policy> Set<'_, 'd, I, A, R> {
    pub(in crate::service::pool) fn try_with<O>(
        limit: capacity::Count,
        build: impl for<'h> FnOnce(Set<'h, 'd, I, A, R>) -> O,
    ) -> Result<O, service::Error> {
        keyed::Min::try_with(limit, |retries, domain| {
            let records = round::Set::try_in(domain).map_err(service::Error::Allocation)?;
            Ok(build(Set {
                revision: service::Revision::INITIAL,
                records,
                queue: queues::Queue::new(retries),
                desired: 0,
                policy: PhantomData,
            }))
        })
        .map_err(service::Error::Heap)?
    }
}

impl<'h, 'd, I, A, R: sync::Policy> Set<'h, 'd, I, A, R> {
    pub(in crate::service::pool) fn contains(&self, member: MemberKey<'h>) -> bool {
        self.records.contains(member.0)
    }

    pub(in crate::service::pool) fn is_empty(&self) -> bool {
        self.desired == 0
    }

    pub(in crate::service::pool) const fn revision(&self) -> service::Revision {
        self.revision
    }

    pub(in crate::service::pool) fn is_stale(&self, eligible: impl Fn(&I) -> bool) -> bool {
        self.records
            .values()
            .any(|record| record.is_desired() && record.health.demand != Some(eligible(&record.id)))
    }

    pub(in crate::service::pool) fn set_demand(&mut self, eligible: impl Fn(&I) -> bool) {
        for entry in self.records.entries() {
            if let set::Entry::Occupied(mut entry) = entry {
                if !entry.get().is_desired() {
                    continue;
                }
                let eligible = eligible(&entry.get().id);
                entry.get_mut().health.demand = Some(eligible);
            }
        }
    }

    pub(in crate::service::pool) fn cancel_if(
        &mut self,
        target: Target<'h>,
        cancel: impl FnOnce(&I) -> bool,
    ) -> Option<Update> {
        let Ok(entry) = self.records.try_entry(target.member.0) else {
            return Some(Update::Missing);
        };
        if !cancel(&entry.get().id) {
            return None;
        }
        Some(match Self::settle(entry, &mut self.queue) {
            Some(entry) => Record::cancel(entry, target),
            None => Update::Ignored,
        })
    }

    pub(in crate::service::pool) fn recover(&mut self, target: Target<'h>) -> Peer<'h, &I> {
        let member = target.member;
        let Ok(entry) = self.records.try_entry(member.0) else {
            return Peer::Missing;
        };
        if !entry.get().is_desired() {
            return Peer::Ready {
                target,
                output: &entry.into_mut().id,
            };
        }
        let record = match entry.state() {
            set::State::Ready(mut entry) => {
                if (entry.get().health.ready != Ready::Healthy
                    || entry.get().health.matches(target))
                    && let Err(self::Exhausted) = entry.get_mut().health.try_ready(Ready::Healthy)
                {
                    entry.remove();
                    return Peer::Exhausted;
                }
                entry.into_mut()
            }
            set::State::Idle(entry) => {
                let entry = match backoff::Backoff::try_new(entry) {
                    Ok(backoff) => self.queue.recover(backoff),
                    Err(backoff::Unready::Probing(entry)) => entry,
                    Err(backoff::Unready::Exhausted(_)) => return Peer::Exhausted,
                };
                match Record::try_resume(entry, Ready::Healthy) {
                    Ok(record) => record,
                    Err(self::Exhausted) => return Peer::Exhausted,
                }
            }
        };
        Peer::Ready {
            target: record.health.target(member),
            output: &record.id,
        }
    }

    /// Releases only this generation's unfinished probe without asserting peer health.
    pub(in crate::service::pool) fn cancel(&mut self, target: Target<'h>) -> Update {
        let Ok(entry) = self.records.try_entry(target.member.0) else {
            return Update::Missing;
        };
        match Self::settle(entry, &mut self.queue) {
            Some(entry) => Record::cancel(entry, target),
            None => Update::Ignored,
        }
    }

    /// Every selected epoch settles once through its dial's terminal transition.
    fn settle<'a>(
        mut entry: Occupied<'a, 'h, I, A>,
        queue: &mut queues::Queue<'h, 'd>,
    ) -> Option<Occupied<'a, 'h, I, A>> {
        if R::AUTHORITATIVE {
            return Some(entry);
        }
        entry.get_mut().health.owners -= 1;
        if entry.get().is_desired() {
            return Some(entry);
        }
        if entry.get().health.owners == 0 {
            queue.remove(entry).discard();
        }
        None
    }

    /// Rejects admission first, then ignores late events without penalizing recovered health.
    pub(in crate::service::pool) fn fail(
        &mut self,
        target: Target<'h>,
        backoff: &mut health::Backoff,
        now: clock::Moment<'d>,
    ) -> Update {
        let Ok(entry) = self.records.try_entry(target.member.0) else {
            return Update::Missing;
        };
        let Some(entry) = Self::settle(entry, &mut self.queue) else {
            return Update::Ignored;
        };
        let failure = match failure::Failure::try_new(entry, target) {
            Ok(failure) => failure,
            Err(
                failure::Rejected::Generation
                | failure::Rejected::Probe
                | failure::Rejected::Backoff
                | failure::Rejected::Exhausted,
            ) => return Update::Ignored,
        };
        failure.apply(&mut self.queue, backoff, now);
        Update::Applied
    }

    fn promote(&mut self, now: clock::Moment<'d>) {
        while let Some(retry) = self.queue.first() {
            if retry.priority().retry_at > now {
                return;
            }
            let key = retry.key();
            // SAFETY: only an admitted failure consumes an unlinked resident to create a retry.
            // Recovery, prune, and reuse remove that retry before retiring the resident.
            // This exclusive joint owner retains both the retry and its idle source.
            let (entry, retry) = Unlinked::from_live(unsafe {
                depot::raw::Proof::new(((&mut self.records, retry), key))
            });
            retry.remove();
            if Record::try_resume(entry, Ready::Probe).is_err() {
                continue;
            }
            return;
        }
    }

    pub(in crate::service::pool) fn choose(
        &mut self,
        now: clock::Moment<'d>,
        mut eligible: impl FnMut(&I) -> bool,
    ) -> Selection<'_, 'h, 'd, A> {
        self.promote(now);
        let mut left = self.desired;
        while left != 0 {
            let Some(turn) = self.records.turn() else {
                break;
            };
            if eligible(&turn.get().id) {
                break;
            }
            turn.advance();
            left -= 1;
        }
        if left != 0
            && let Some(turn) = self.records.turn()
        {
            let member = MemberKey(turn.key());
            let target = turn.get().health.target(member);
            let record = match turn.get().health.ready {
                Ready::Healthy => turn.advance(),
                Ready::Probe => {
                    let record = turn.remove().into_mut();
                    record.health.idle = Idle::Probing;
                    record
                }
            };
            if !R::AUTHORITATIVE {
                record.health.owners += 1;
            }
            return Selection::Ready {
                target,
                addr: &record.addr,
            };
        }
        match self.queue.retry_at() {
            Some(at) => Selection::Backoff(at),
            None => Selection::Idle,
        }
    }

    fn prune(&mut self, current: &[Option<MemberKey<'h>>]) {
        for entry in self.records.entries() {
            let set::Entry::Occupied(entry) = entry else {
                continue;
            };
            if entry.get().is_desired() && !current.contains(&Some(MemberKey(entry.key()))) {
                let mut entry = self.queue.remove(entry);
                if !R::AUTHORITATIVE && entry.get().health.owners != 0 {
                    entry.get_mut().health.demand = None;
                } else {
                    entry.discard();
                }
            }
        }
    }
}

impl<'h, 'd, I: Eq, A, R: sync::Policy> Set<'h, 'd, I, A, R> {
    pub(in crate::service::pool) fn try_apply<const N: usize>(
        &mut self,
        snapshot: service::Snapshot<service::Endpoint<I, A>, N>,
    ) -> Result<service::Change, service::Reconcile> {
        let revision = snapshot.revision();
        if revision <= self.revision {
            return Err(service::Reconcile::Stale {
                current: self.revision,
                incoming: revision,
            });
        }
        for (index, item) in snapshot.endpoints().iter().enumerate() {
            if let Some(first) = snapshot.endpoints()[..index]
                .iter()
                .position(|seen| seen.id() == item.id())
            {
                return Err(service::Reconcile::Duplicate {
                    first,
                    duplicate: index,
                });
            }
        }
        let actual = snapshot.endpoints().len();
        let prior = self.desired as usize;
        let reserved =
            entries::Reservation::try_new(&mut self.records, snapshot, !R::AUTHORITATIVE)?;
        let (keys, retained) = reserved.commit(&mut self.queue);
        self.prune(&keys);
        self.revision = revision;
        self.desired = actual as u32;
        Ok(service::Change {
            added: actual - retained,
            retained,
            retired: prior - retained,
        })
    }
}
