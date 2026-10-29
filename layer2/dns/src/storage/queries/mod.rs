use std::{mem, net, ops, time, time::Instant};

use dope::{
    core::driver::schedule::{app, clock},
    manifold::client::{close, deadline},
};
use o3::{
    base::{
        cell::region,
        time::{Nanos, Positive, Unix},
    },
    buffer::pool,
    depot::index::set,
    mem::{Lease, Return, quota},
};

use crate::{
    config, lookup,
    lookup::error,
    net::entropy,
    storage::queries::state::poll,
    wire::{frames, tx::Id},
};

pub(crate) mod index;
pub(crate) mod ledger;
pub(crate) mod receive;
pub(crate) mod state;
pub(crate) mod stream;
pub(crate) mod tx;

type Entry<'s> = set::Entry<
    &'s set::State<index::Branch, u16, { config::MAX_SERVERS + 1 }>,
    index::Branch,
    u16,
    { config::MAX_SERVERS + 1 },
>;

pub(crate) struct Queries<'s, 'd, const LANES: usize, const N: usize> {
    pub(in crate::storage) inner:
        region::Value<region::Identity<'d, 1>, ledger::Ledger<'s, LANES, N>>,
    pub(in crate::storage) lanes: set::State<index::Lane, u16, 2, set::Disjoint>,
    pub(crate) config: &'s config::Validated,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Submission {
    Committed,
    Deferred,
    Failed(error::Kind),
}

#[must_use = "a pending completion must update its exact ledger lane"]
enum Finish<T> {
    Done(T),
    Complete { value: T, lane: index::Lane },
}

impl<T> Finish<T> {
    fn apply<const LANES: usize, const N: usize>(
        self,
        inner: &mut ledger::Ledger<'_, LANES, N>,
        now: time::Instant,
    ) -> T {
        match self {
            Self::Done(value) => value,
            Self::Complete { value, lane } => {
                inner.complete(lane, now);
                value
            }
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum RetireStream {
    Done,
    Retry,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Retirement {
    Resume,
    Expired,
    Fail(error::Kind),
}

impl Retirement {
    pub(crate) const fn from_close(reason: close::Reason) -> Self {
        match reason {
            close::Reason::Local(
                close::LocalOrigin::ApiRequest
                | close::LocalOrigin::SideComplete
                | close::LocalOrigin::OperationCancelled
                | close::LocalOrigin::Superseded
                | close::LocalOrigin::ScheduledRotation
                | close::LocalOrigin::RuntimeShutdown
                | close::LocalOrigin::ResourceFailure,
            )
            | close::Reason::EndpointRetired
            | close::Reason::Timeout(deadline::Kind::Lifetime) => Self::Resume,
            close::Reason::Timeout(deadline::Kind::Inbound) => Self::Expired,
            reason @ (close::Reason::Timeout(deadline::Kind::Connect | deadline::Kind::Send)
            | close::Reason::Capacity(
                close::Capacity::IngressStaging
                | close::Capacity::IngressRetention
                | close::Capacity::Transport
                | close::Capacity::IdentityExhausted,
            )
            | close::Reason::Protocol
            | close::Reason::Receive(_)
            | close::Reason::Ingress(_)
            | close::Reason::Egress(_)
            | close::Reason::Transport
            | close::Reason::Remote) => Self::Fail(error::Kind::Close { reason }),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct StreamStatus {
    pub(crate) pending_for_server: bool,
    pub(crate) live: bool,
    pub(crate) live_for_server: bool,
    pub(crate) transactions: bool,
}

impl StreamStatus {
    pub(in crate::storage) fn read<'d, const LANES: usize, const N: usize>(
        queries: &Queries<'_, 'd, LANES, N>,
        token: &mut region::Token<region::Identity<'d, 1>>,
        server: index::Server,
    ) -> Self {
        let inner = queries.get_mut(token);
        Self {
            pending_for_server: inner.reg.pending.has_server(server),
            live: inner.reg.live.any(),
            live_for_server: inner.reg.live.contains(server),
            transactions: inner.reg.ids.has_stream(),
        }
    }
}

impl<'s, 'd, const LANES: usize, const N: usize> Queries<'s, 'd, LANES, N> {
    pub(in crate::storage::queries) fn get_mut<'a>(
        &'a self,
        token: &'a mut region::Token<region::Identity<'d, 1>>,
    ) -> &'a mut ledger::Ledger<'s, LANES, N> {
        token.borrow_mut(&self.inner)
    }

    pub(in crate::storage::queries) fn is_cancelled(&self, lane: index::Lane) -> bool {
        self.lanes.contains(set::Id::at::<0>(), lane.ordinal())
    }

    fn take<'queries, 'loan>(
        &'queries self,
        plane: set::Id<2>,
        token: &'loan mut region::Token<region::Identity<'d, 1>>,
    ) -> Option<(
        Removed<'queries>,
        ledger::Lane<&'loan mut ledger::Ledger<'s, LANES, N>>,
    )>
    where
        'queries: 'loan,
    {
        let removed = self.lanes.take(plane)?;
        let index = index::Lane::from_ordinal(removed.index());
        // SAFETY: construction gives this set and ledger the same immutable lane extent.
        let lane = unsafe { ledger::Lane::new((self.get_mut(token), index)) };
        Some((removed, lane))
    }

    pub(crate) fn cleanup(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        work: app::Work<'_, '_>,
    ) -> ops::ControlFlow<()> {
        while let Some((removed, lane)) = self.take(set::Id::at::<0>(), token) {
            let Some(permit) = work.permit() else {
                removed.restore();
                return ops::ControlFlow::Break(());
            };
            permit.run(|| {
                ledger::Ledger::retire_lane(lane);
                removed.insert(set::Id::at::<1>());
            });
        }
        ops::ControlFlow::Continue(())
    }

    pub(crate) fn claim(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) -> Option<Lane<'_, 's, 'd, LANES, N>> {
        let removed = match self.lanes.take(set::Id::at::<1>()) {
            Some(removed) => removed,
            None => {
                let (removed, lane) = self.take(set::Id::at::<0>(), token)?;
                ledger::Ledger::retire_lane(lane);
                removed
            }
        };
        Some(Lane::new(self, removed))
    }

    pub(crate) fn admit_udp(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        now: time::Instant,
        work: app::Work<'_, '_>,
        value: pool::Cursor<pool::Width<{ frames::MAX_QUERY_BYTES }>>,
        random: &mut entropy::Datagram<'_>,
        submit: impl FnOnce(frames::Datagram, index::Server) -> Submission,
    ) -> quota::Admission<Submission> {
        let inner = self.get_mut(token);
        let Some(start) = inner.start_udp(now) else {
            return quota::Admission::Empty;
        };
        let Some(permit) = work.permit() else {
            return quota::Admission::Exhausted;
        };
        let finish = permit.run(|| start.start(random, value, submit));
        quota::Admission::Item(finish.apply(inner, now))
    }

    pub(in crate::storage) fn submit(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        now: time::Instant,
        server: index::Server,
        id: Id,
        submit: impl FnOnce(stream::Frame) -> stream::Admission,
    ) -> stream::Submission {
        if !self.lanes.is_empty(set::Id::at::<0>()) {
            return stream::Submission::Deferred;
        }
        let inner = self.get_mut(token);
        let Some(start) = inner.start_tcp(server, id, now) else {
            return stream::Submission::Deferred;
        };
        let finish = start.submit(submit);
        finish.apply(inner, now)
    }

    pub(crate) fn has_udp(
        &self,
        token: &region::Token<region::Identity<'d, 1>>,
        spare: usize,
    ) -> bool {
        !self.lanes.is_empty(set::Id::at::<0>())
            || (spare != 0 && token.borrow(&self.inner).reg.pending.has_udp())
    }

    pub(crate) fn has_stream(&self, token: &region::Token<region::Identity<'d, 1>>) -> bool {
        let inner = token.borrow(&self.inner);
        inner.reg.pending.has_stream()
            || inner.reg.live.any()
            || !self.lanes.is_empty(set::Id::at::<0>())
    }

    pub(crate) fn has_pending(
        &self,
        token: &region::Token<region::Identity<'d, 1>>,
        server: index::Server,
    ) -> bool {
        self.lanes.is_empty(set::Id::at::<0>())
            && token.borrow(&self.inner).reg.pending.has_server(server)
    }

    pub(crate) fn bind_udp(&self, token: &mut region::Token<region::Identity<'d, 1>>) {
        self.get_mut(token).reg.ids.bind_udp();
    }

    pub(crate) fn exhaust_tcp(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        cause: error::Kind,
    ) {
        self.get_mut(token).reg.stream_failure.get_or_insert(cause);
    }

    pub(crate) fn fail_udp(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        id: Id,
        epoch: tx::Epoch,
        server: net::SocketAddr,
        cause: error::Kind,
        now: time::Instant,
    ) {
        let retry = state::Retry::from_config(self.config);
        let inner = self.get_mut(token);
        let Some(branch) = inner.reg.ids.udp_owner(id) else {
            return;
        };
        let lane = branch.lane();
        if self.is_cancelled(lane) {
            return;
        }
        let Some(wait) = inner.claim(
            lane,
            branch.family(),
            ledger::Filter::Datagram { id, epoch, server },
        ) else {
            return;
        };
        wait.failed(retry, now, cause);
        inner.complete(lane, now);
    }

    pub(crate) fn undo_stream(&self, token: &mut region::Token<region::Identity<'d, 1>>, id: Id) {
        let inner = self.get_mut(token);
        let Some(branch) = inner.reg.ids.stream_owner(id) else {
            return;
        };
        if self.is_cancelled(branch.lane()) {
            return;
        }
        inner.rollback(branch.lane(), branch.family(), id);
    }

    pub(in crate::storage) fn retire(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        server: index::Server,
        end: Retirement,
        now: time::Instant,
        work: app::Work<'_, '_>,
        branches: &mut index::Retirement<LANES>,
    ) -> RetireStream {
        let retry = state::Retry::from_config(self.config);
        let inner = self.get_mut(token);
        if !inner.reg.ids.has_stream() {
            return RetireStream::Done;
        }
        let end = inner.reg.stream_failure.map_or(end, Retirement::Fail);
        loop {
            let Some((branch, next)) = branches.next() else {
                return RetireStream::Done;
            };
            let Some(permit) = work.permit() else {
                return RetireStream::Retry;
            };
            *branches = next;
            permit.run(|| {
                let lane = branch.lane();
                if self.is_cancelled(lane) {
                    return;
                }
                let family = branch.family();
                let Some(wait) = inner.claim(lane, family, ledger::Filter::Stream(server)) else {
                    return;
                };
                match end {
                    Retirement::Resume => drop(wait),
                    Retirement::Expired if wait.deadline() <= now => {
                        wait.failed(retry, now, error::Kind::Timeout)
                    }
                    Retirement::Expired => drop(wait),
                    Retirement::Fail(error) => wait.failed(retry, now, error),
                }
                inner.complete(lane, now);
            });
        }
    }
}

type Removed<'queries> = set::Entry<
    &'queries set::State<index::Lane, u16, 2, set::Disjoint>,
    index::Lane,
    u16,
    2,
    set::Id<2>,
>;

pub(crate) struct Lane<'queries, 's, 'd, const LANES: usize, const N: usize> {
    queries: &'queries Queries<'s, 'd, LANES, N>,
    removed: Lease<Cancel<'queries>>,
}

struct Cancel<'queries>(Removed<'queries>);

impl Return for Cancel<'_> {
    type Item = ();

    fn return_item(self, (): Self::Item) {
        self.0.insert(set::Id::at::<0>());
    }
}

impl<'queries, 's, 'd, const LANES: usize, const N: usize> Lane<'queries, 's, 'd, LANES, N> {
    pub(in crate::storage::queries) fn new(
        queries: &'queries Queries<'s, 'd, LANES, N>,
        removed: Removed<'queries>,
    ) -> Self {
        Self {
            queries,
            removed: Lease::new(Cancel(removed), ()),
        }
    }

    fn read<'a>(
        &'a self,
        token: &'a region::Token<region::Identity<'d, 1>>,
    ) -> ledger::Lane<&'a ledger::Ledger<'s, LANES, N>> {
        let index = index::Lane::from_ordinal(self.removed.sink().0.index());
        // SAFETY: this retained lane belongs to the exact queries owner being borrowed.
        unsafe { ledger::Lane::new((token.borrow(&self.queries.inner), index)) }
    }

    fn write<'a>(
        &'a self,
        token: &'a mut region::Token<region::Identity<'d, 1>>,
    ) -> ledger::Lane<&'a mut ledger::Ledger<'s, LANES, N>> {
        let index = index::Lane::from_ordinal(self.removed.sink().0.index());
        // SAFETY: this retained lane belongs to the exact queries owner being borrowed.
        unsafe { ledger::Lane::new((self.queries.get_mut(token), index)) }
    }

    pub(crate) fn start(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        reason: lookup::Reason,
        unix: Unix<Nanos>,
        name: &crate::host::Name,
        now: clock::Moment<'d>,
    ) -> Result<(), clock::Range<Positive>> {
        let lane = self.write(token);
        let config = self.queries.config;
        let hard = now.try_add(config.query.total_timeout)?;
        let seed = state::Seed::new(reason, unix, now, hard, name, config);
        ledger::Ledger::start(lane, seed);
        Ok(())
    }

    pub(crate) fn poll(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        now: Instant,
    ) -> poll::State<N> {
        let retry = state::Retry::from_config(self.queries.config);
        let (slot, reg) = ledger::Ledger::write(self.write(token));
        slot.poll(now, retry, reg)
    }

    pub(crate) fn is_ready(&self, token: &region::Token<region::Identity<'d, 1>>) -> bool {
        let (slot, reg) = ledger::Ledger::read(self.read(token));
        slot.is_ready(reg.stream_failure.is_some())
    }
}

const _: () =
    assert!(mem::size_of::<Lane<'static, 'static, 'static, 1, 1>>() == 3 * mem::size_of::<usize>());
