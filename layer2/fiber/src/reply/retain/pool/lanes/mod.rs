use std::{
    cell,
    mem::{needs_drop, size_of},
    num,
    num::NonZero,
};

use demand::Demand;
use dope::core::driver::schedule::ready;
use o3::{
    alloc,
    alloc::tail,
    depot::arena::linked::fifo::ends,
    mem,
    mem::{Rejected, credit::fair, raw::Source as _},
};
use wake::{Wait, Wake};

use crate::reply::{
    LaneIndex, Length,
    arena::state::EntryKey,
    layout, retain,
    retain::{
        grant::{entries, payload},
        pool,
        pool::Acquire,
    },
    usage,
};

mod demand;
mod wake;

const DATA: fair::Axis<3> = fair::Axis::at::<0>();
const VALUES: fair::Axis<3> = fair::Axis::at::<1>();
const BYTES: fair::Axis<3> = fair::Axis::at::<2>();

pub(super) struct Lanes<'credits, 'region> {
    pub(super) credits: Credits<'credits, 'region>,
    next: cell::Cell<fair::Axis<3>>,
}

impl<'credits, 'region> Lanes<'credits, 'region> {
    pub(super) fn new(slots: &'credits Slots<'region>) -> Self {
        Self {
            credits: Credits::new(slots),
            next: cell::Cell::new(DATA),
        }
    }

    pub(super) fn bind(&self, lane: LaneIndex, target: ready::Target<'region>) {
        let wake = self.credits.waiters.state(lane);
        wake.target.set(Some(target));
        wake.waiting.set(None);
    }

    pub(super) fn wake(&self, lane: LaneIndex) {
        if let Some(target) = self.credits.waiters.state(lane).target.get() {
            target.wake();
        }
    }

    pub(super) fn cancel(&self, lane: LaneIndex) {
        self.credits.waiters.state(lane).waiting.set(None);
    }

    pub(super) fn cancel_entry(&self, lane: LaneIndex, key: EntryKey) {
        self.credits.waiters.state(lane).cancel(key);
    }

    pub(super) fn take_waiting<A>(
        &self,
        lane: LaneIndex,
        key: EntryKey,
        sum: &mut payload::raw::Sum<'credits>,
        amount: A,
    ) -> Result<(), Acquire>
    where
        A: mem::raw::Amount<3, usize> + Into<payload::Returned> + Into<payload::Requested>,
    {
        let wake = self.credits.waiters.state(lane);
        match sum.try_acquire(amount) {
            Ok(()) => {
                wake.cancel(key);
                Ok(())
            }
            Err(cause) => {
                match cause {
                    fair::Cause::Overflow { .. } => wake.cancel(key),
                    fair::Cause::Exhausted { .. } => {
                        let request = amount.into();
                        wake.waiting.set(Some(Wait { key, request }));
                        self.queue_need(lane, request);
                    }
                }
                Err(Acquire::new(cause))
            }
        }
    }

    pub(super) fn try_reserve(
        &self,
        lane: LaneIndex,
        usage: usage::Usage,
    ) -> Result<payload::Grant<'credits, payload::Failure>, usage::Capacity> {
        self.credits
            .payload(lane)
            .try_acquire(payload::Failure::new(usage))
            .map(payload::Grant::new)
            .map_err(|rejected| match Acquire::new(rejected.discard()) {
                Acquire::Capacity(cause) => cause,
                Acquire::Backpressure(cause) => usage::Capacity::Unavailable(cause),
            })
    }

    pub(super) fn release_data<A>(&self, grant: payload::Grant<'credits, A>)
    where
        A: mem::raw::Amount<3, usize> + Into<payload::Returned>,
    {
        let returned = grant.amount().into();
        grant.into_inner().release();
        self.returned(returned);
    }

    pub(super) fn returned(&self, returned: payload::Returned) {
        let usage = match returned {
            payload::Returned::Axis(axis) => {
                self.refund_axis(axis);
                return;
            }
            payload::Returned::Value(value) => {
                self.refund_axis(DATA);
                value.usage()
            }
            payload::Returned::Failure(failure) => failure.usage(),
        };
        if usage.values() != usage::Values::ZERO {
            self.refund_axis(VALUES);
        }
        if usage.bytes() != usage::Bytes::ZERO {
            self.refund_axis(BYTES);
        }
    }

    pub(super) fn refund(&self, grant: payload::Grant<'credits, payload::Used>) {
        let amount = grant.amount();
        grant.into_inner().release();
        match amount {
            payload::Used::Values { bytes, .. } => {
                self.refund_axis(VALUES);
                if bytes != usage::Bytes::ZERO {
                    self.refund_axis(BYTES);
                }
            }
            payload::Used::Bytes(_) => self.refund_axis(BYTES),
        }
    }

    fn queue_need(&self, lane: LaneIndex, request: payload::Requested) {
        let usage = match request {
            payload::Requested::Value(value) => {
                self.queue_demand(lane, Demand::Data);
                value.usage()
            }
            payload::Requested::Failure(failure) => failure.usage(),
        };
        if let Some(values) = num::NonZero::<usize>::new(usage.values().get()) {
            self.queue_demand(lane, Demand::Values(payload::Values::new(values)));
        }
        if let Some(bytes) = num::NonZero::<usize>::new(usage.bytes().get()) {
            self.queue_demand(lane, Demand::Bytes(payload::Bytes::new(bytes)));
        }
    }

    fn queue_demand(&self, lane: LaneIndex, demand: Demand) {
        let part = demand.axis();
        if self.credits.can_satisfy(lane, demand) {
            return;
        }
        self.credits.waiters.queue(part, lane);
    }

    fn refund_axis(&self, part: fair::Axis<3>) {
        self.credits.waiters.chains[part.index()].refund();
    }

    pub(super) fn has_pending(&self) -> bool {
        self.credits
            .waiters
            .chains
            .iter()
            .any(pool::raw::Round::has_pending)
    }

    /// Examines at most one waiter under the arena's already-consumed permit.
    /// Refunds request a subsequent round without restarting the current one.
    pub(super) fn advance(&self) {
        for _ in 0..3 {
            let part = self.next.get();
            self.next.set(fair::Axis::ALL[(part.index() + 1) % 3]);
            let Some(lane) = self.credits.waiters.advance(part) else {
                continue;
            };
            let wake = self.credits.waiters.state(lane);
            let Some(wait) = wake.waiting.get() else {
                return;
            };
            let request = wait.request;
            if self.credits.can_take(lane, request)
                && let Some(target) = wake.target.get()
            {
                wake.waiting.set(None);
                target.wake();
                return;
            }
            self.queue_need(lane, request);
            return;
        }
    }
}

struct Slot<'region> {
    wake: Wake<'region>,
    credit: retain::raw::State,
}

/// The complete credit epoch borrowed by every public arena, claim, and grant.
/// Usable acquisitions exclude reuse; forgotten acquisitions stay charged until
/// their entire private accounting domain ends.
pub(in crate::reply) struct Slots<'region>(
    tail::Block<retain::raw::Pools, Slot<'region>, NonZero<u32>>,
);

impl<'region> Slots<'region> {
    pub(in crate::reply) fn try_new(layout: layout::Layout) -> Result<Self, alloc::Error> {
        let capacity = layout.admission();
        let payload = layout.payload();
        let memory = tail::Plan::try_new(layout.lane_count())?.try_allocate()?;
        let pools = retain::raw::Pools::new(capacity, payload);
        Ok(Self(memory.fill(pools, |pools, _| Slot {
            wake: Wake::new(),
            // SAFETY: fill writes the header first at its final address; both
            // states stay in this same allocation throughout every lane borrow.
            credit: unsafe { retain::raw::State::new(capacity, payload, pools) },
        })))
    }
}

pub(super) struct Credits<'credits, 'region> {
    entries: cell::Cell<entries::Entries>,
    waiters: Waiters<'credits, 'region>,
}

/// One successful admission and the exact lane whose entry count it increased.
/// Register consumes it immediately after admission, with no later failure point.
#[must_use = "registration must initialize its original entry"]
pub(in crate::reply) struct Registration<
    'arena,
    'credits,
    'region: 'arena,
    A: mem::raw::Amount<2, usize>,
> {
    lane: pool::Lane<'arena, 'credits, 'region>,
    grant: entries::Grant<'credits, A>,
}

pub(super) struct Waiters<'credits, 'region> {
    slots: tail::View<'credits, retain::raw::Pools, Slot<'region>, NonZero<u32>>,
    chains: [pool::raw::Round<LaneIndex>; 3],
}

struct Links<'a, 'credits, 'region> {
    waiters: &'a Waiters<'credits, 'region>,
    part: fair::Axis<3>,
}

// SAFETY: every view binds a chain and link axis from the same Waiters; entry
// views carry a lane validated against the layout that allocated the slots.
unsafe impl pool::raw::Domain<LaneIndex> for Links<'_, '_, '_> {
    fn chain(&self) -> &ends::Cell<pool::raw::Slot<LaneIndex>> {
        &self.waiters.chains[self.part.index()].chain
    }

    fn link(&self, index: LaneIndex) -> &pool::raw::Link<LaneIndex> {
        // SAFETY: Domain admits only layout-validated or already-stored lanes,
        // and Axis proves the exact link part.
        unsafe {
            self.waiters
                .slots
                .as_slice()
                .get_unchecked(index.get())
                .wake
                .links
                .get_unchecked(self.part.index())
        }
    }
}

impl<'credits, 'region> Waiters<'credits, 'region> {
    fn state(&self, lane: LaneIndex) -> &Wake<'region> {
        &self.slot(lane).wake
    }

    /// Publishes `lane` unless it is already linked; cancellation keeps its link.
    fn queue(&self, part: fair::Axis<3>, lane: LaneIndex) {
        pool::raw::Domain::push_back(
            &Links {
                waiters: self,
                part,
            },
            lane,
        );
    }

    /// Only `advance` removes these links; it walks the exact chain it owns.
    fn advance(&self, part: fair::Axis<3>) -> Option<LaneIndex> {
        self.chains[part.index()].advance(&Links {
            waiters: self,
            part,
        })
    }

    fn slot(&self, lane: LaneIndex) -> &'credits Slot<'region> {
        // SAFETY: LaneIndex is created only from a Lane validated against the same
        // layout that created this slot slice.
        unsafe { self.slots.as_slice().get_unchecked(lane.get()) }
    }
}

impl<'credits, 'region> Credits<'credits, 'region> {
    fn new(slots: &'credits Slots<'region>) -> Self {
        Self {
            entries: cell::Cell::new(entries::Entries::EMPTY),
            waiters: Waiters {
                slots: tail::View::new(&slots.0),
                chains: [const { pool::raw::Round::new() }; 3],
            },
        }
    }

    pub(in crate::reply::retain::pool) fn can_admit<A>(&self, lane: LaneIndex, request: A) -> bool
    where
        A: mem::raw::Amount<2, usize>,
    {
        self.capacity(lane)
            .can_acquire(mem::raw::Amount::get(request))
    }

    fn can_take(&self, lane: LaneIndex, request: payload::Requested) -> bool {
        self.payload(lane)
            .can_acquire(mem::raw::Amount::get(request))
    }

    fn can_satisfy(&self, lane: LaneIndex, demand: Demand) -> bool {
        let amount = match demand {
            Demand::Data => [1, 0, 0],
            Demand::Values(values) => mem::raw::Amount::get(values),
            Demand::Bytes(bytes) => mem::raw::Amount::get(bytes),
        };
        self.payload(lane).can_acquire(amount)
    }

    pub(in crate::reply::retain::pool) fn shared_slots(&self) -> [usize; 2] {
        self.waiters.slots.header().shared()
    }

    pub(in crate::reply::retain::pool) fn used_entries(&self) -> Length {
        Length::new(self.entries.get().get())
    }

    pub(in crate::reply::retain::pool) fn capacity(
        &self,
        lane: LaneIndex,
    ) -> retain::raw::Lane<'credits, 2, entries::Tag> {
        let state = &self.waiters.slot(lane).credit;
        // SAFETY: the borrowed slot belongs to this pool's original credit layout.
        state.capacity()
    }

    pub(in crate::reply::retain::pool) fn payload(
        &self,
        lane: LaneIndex,
    ) -> retain::raw::Lane<'credits, 3, payload::Tag> {
        let state = &self.waiters.slot(lane).credit;
        // SAFETY: the borrowed slot belongs to this pool's original credit layout.
        state.payload()
    }
}

impl<'arena, 'credits, 'region, A: mem::raw::Amount<2, usize>>
    Registration<'arena, 'credits, 'region, A>
{
    pub(in crate::reply) fn try_new(
        lane: pool::Lane<'arena, 'credits, 'region>,
        request: A,
    ) -> Result<Self, fair::Cause<usize, 2>> {
        let credits = &lane.pool().lanes().credits;
        let grant = credits
            .capacity(lane.index())
            .try_acquire(request)
            .map_err(Rejected::discard)?;
        credits.entries.set(credits.entries.get().admit(request));
        Ok(Self {
            lane,
            grant: entries::Grant::new(grant),
        })
    }
}

impl<'arena, 'credits, 'region, A> Registration<'arena, 'credits, 'region, A>
where
    A: mem::raw::Amount<2, usize>
        + mem::raw::Exact<2, usize, A, Input = (), Right = entries::Units<0, 1>>,
    A::Left: mem::raw::Amount<2, usize>,
{
    pub(in crate::reply) fn split(
        self,
    ) -> (
        Registration<'arena, 'credits, 'region, A::Left>,
        entries::Grant<'credits, entries::Units<0, 1>>,
    ) {
        let (left, order) = self.grant.split();
        (
            Registration {
                lane: self.lane,
                grant: left,
            },
            order,
        )
    }
}

impl<'arena, 'credits, 'region, const N: usize>
    Registration<'arena, 'credits, 'region, entries::Group<N>>
{
    pub(in crate::reply) fn split_group<T>(
        self,
        values: [T; N],
    ) -> (
        [(
            Registration<'arena, 'credits, 'region, entries::Units<1, 1>>,
            T,
        ); N],
        Registration<'arena, 'credits, 'region, entries::Units<1, 1>>,
    ) {
        let (commands, terminal) = self.grant.split_group(values);
        let lane = self.lane;
        (
            commands.map(|(grant, value)| (Registration { lane, grant }, value)),
            Registration {
                lane,
                grant: terminal,
            },
        )
    }
}

impl<'arena, 'credits, 'region> Registration<'arena, 'credits, 'region, entries::Units<1, 0>> {
    pub(in crate::reply::retain::pool) fn into_parts(
        self,
    ) -> (
        pool::Lane<'arena, 'credits, 'region>,
        entries::Grant<'credits, entries::Units<1, 0>>,
    ) {
        (self.lane, self.grant)
    }

    pub(in crate::reply) fn register(
        self,
        entry: EntryKey,
    ) -> pool::Owner<'arena, 'credits, 'region> {
        pool::Owner::new(self, entry)
    }
}

// SAFETY: release consumes the caller's original counted acquisition exactly
// once. The private Payload owner captures its issuing key and grant together.
unsafe impl<'credits> pool::raw::Release<'credits> for Credits<'credits, '_> {
    unsafe fn release_slot(&self, grant: entries::Grant<'credits, entries::Units<1, 0>>) {
        grant.into_inner().release();
        self.entries
            .set(self.entries.get().release(entries::Units::<1, 0>::VALUE));
    }
}

const _: () = {
    assert!(
        size_of::<Registration<'static, 'static, 'static, entries::Units<1, 0>>>()
            == 3 * size_of::<usize>()
    );
    assert!(!needs_drop::<
        Registration<'static, 'static, 'static, entries::Units<1, 0>>,
    >());
};
