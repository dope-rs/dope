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
    depot::{arena::linked::fifo::ends, array::init},
    mem,
    mem::credit::fair,
};
use wake::{Wait, Wake};

use crate::reply::{
    LaneIndex, Length,
    arena::state::Index,
    layout, recv, retain,
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

    pub(super) fn cancel_entry(&self, lane: LaneIndex, key: Index) {
        self.credits.waiters.state(lane).cancel(key);
    }

    pub(super) fn wait_local(&self, lane: LaneIndex, key: Index) {
        self.credits
            .waiters
            .state(lane)
            .waiting
            .set(Some(Wait { key, request: None }));
    }

    pub(super) fn wake_local(&self, lane: LaneIndex, key: Index) {
        let wake = self.credits.waiters.state(lane);
        if wake
            .waiting
            .get()
            .is_some_and(|wait| wait.key == key && wait.request.is_none())
        {
            wake.waiting.set(None);
            if let Some(target) = wake.target.get() {
                target.wake();
            }
        }
    }

    pub(super) fn take_waiting<A>(
        &self,
        lane: LaneIndex,
        key: Index,
        source: &retain::raw::State,
        amount: A,
        acceptance: recv::Acceptance,
    ) -> Result<(), Acquire>
    where
        A: mem::raw::Amount<3, usize> + Into<payload::Requested>,
    {
        let wake = self.credits.waiters.state(lane);
        match source.try_acquire(amount.get()) {
            Ok(()) => {
                wake.cancel(key);
                Ok(())
            }
            Err(cause) => {
                let cause = match Acquire::new(cause) {
                    Acquire::Backpressure(cause)
                        if matches!(
                            acceptance,
                            recv::Acceptance::Collected | recv::Acceptance::First
                        ) =>
                    {
                        Acquire::Capacity(usage::Capacity::Unavailable(cause))
                    }
                    cause => cause,
                };
                match cause {
                    Acquire::Capacity(_) => wake.cancel(key),
                    Acquire::Backpressure(_) => {
                        let request = amount.into();
                        wake.waiting.set(Some(Wait {
                            key,
                            request: Some(request),
                        }));
                        self.queue_need(lane, request);
                    }
                }
                Err(cause)
            }
        }
    }

    /// # Safety
    /// The consuming owner carries this source and its exact outstanding amount.
    pub(super) unsafe fn release_data<A>(
        &self,
        source: &retain::raw::State,
        amount: A,
    ) where
        A: mem::raw::Amount<3, usize>,
    {
        unsafe { source.release(amount.get()) };
        self.returned(amount);
    }

    pub(super) fn returned<A: mem::raw::Amount<3, usize>>(&self, amount: A) {
        let [data, values, bytes] = amount.get();
        if data != 0 {
            self.refund_axis(DATA);
        }
        if values != 0 {
            self.refund_axis(VALUES);
        }
        if bytes != 0 {
            self.refund_axis(BYTES);
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
            let Some(Wait {
                request: Some(request),
                ..
            }) = wake.waiting.get()
            else {
                return;
            };
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
    A: mem::raw::Amount<1, usize>,
> {
    lane: pool::Lane<'arena, 'credits, 'region>,
    source: &'credits retain::raw::State,
    amount: A,
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
        A: mem::raw::Amount<1, usize>,
    {
        self.state(lane).can_admit(mem::raw::Amount::get(request))
    }

    fn can_take(&self, lane: LaneIndex, request: payload::Requested) -> bool {
        self.state(lane).can_acquire(mem::raw::Amount::get(request))
    }

    fn can_satisfy(&self, lane: LaneIndex, demand: Demand) -> bool {
        let amount = match demand {
            Demand::Data => [1, 0, 0],
            Demand::Values(values) => mem::raw::Amount::get(values),
            Demand::Bytes(bytes) => mem::raw::Amount::get(bytes),
        };
        self.state(lane).can_acquire(amount)
    }

    pub(in crate::reply::retain::pool) fn shared_slots(&self) -> [usize; 1] {
        self.waiters.slots.header().shared()
    }

    pub(in crate::reply::retain::pool) fn used_entries(&self) -> Length {
        Length::new(self.entries.get().get())
    }

    pub(in crate::reply::retain::pool) fn state(
        &self,
        lane: LaneIndex,
    ) -> &'credits retain::raw::State {
        &self.waiters.slot(lane).credit
    }

    /// # Safety
    /// The consumed Payload owns this source's one original counted admission.
    pub(in crate::reply::retain::pool) unsafe fn release_slot(
        &self,
        source: &retain::raw::State,
    ) {
        unsafe { source.release_slot() };
        self.entries.set(self.entries.get().release(entries::Unit));
    }
}

impl<'arena, 'credits, 'region, A: mem::raw::Amount<1, usize>>
    Registration<'arena, 'credits, 'region, A>
{
    pub(in crate::reply) fn try_new(
        lane: pool::Lane<'arena, 'credits, 'region>,
        request: A,
    ) -> Result<Self, fair::Cause<usize, 2>> {
        let credits = &lane.pool().lanes().credits;
        let source = credits.state(lane.index());
        source
            .try_admit(request.get())
            .map_err(|error| match error {
                fair::Cause::Overflow {
                    held, requested, ..
                } => fair::Cause::Overflow {
                    axis: fair::Axis::at::<0>(),
                    held,
                    requested,
                },
                fair::Cause::Exhausted {
                    required,
                    available,
                    ..
                } => fair::Cause::Exhausted {
                    axis: fair::Axis::at::<0>(),
                    required,
                    available,
                },
            })?;
        credits.entries.set(credits.entries.get().admit(request));
        Ok(Self {
            lane,
            source,
            amount: request,
        })
    }
}

impl<'arena, 'credits, 'region, const N: usize>
    Registration<'arena, 'credits, 'region, entries::Group<N>>
{
    pub(in crate::reply) fn map_group<T, U, V>(
        self,
        commands: init::Guard<'_, T, N, true, init::Full>,
        values: [U; N],
        outputs: &mut [V; N],
        mut map: impl FnMut(usize, T, Registration<'arena, 'credits, 'region, entries::Unit>, U, &mut V),
    ) -> Registration<'arena, 'credits, 'region, entries::Unit> {
        let Self {
            lane,
            source,
            amount: entries::Group,
        } = self;
        for (index, ((command, value), output)) in
            commands.into_iter().zip(values).zip(outputs).enumerate()
        {
            // Full and both arrays transfer exactly N of the admitted N+1 units.
            let admitted = Registration {
                lane,
                source,
                amount: entries::Unit,
            };
            map(index, command, admitted, value, output);
        }
        Registration {
            lane,
            source,
            amount: entries::Unit,
        }
    }
}

impl<'arena, 'credits, 'region> Registration<'arena, 'credits, 'region, entries::Unit> {
    pub(in crate::reply::retain::pool) fn into_parts(
        self,
    ) -> (
        pool::Lane<'arena, 'credits, 'region>,
        &'credits retain::raw::State,
    ) {
        let Self {
            lane,
            source,
            amount: entries::Unit,
        } = self;
        (lane, source)
    }
}

const _: () = {
    assert!(
        size_of::<Registration<'static, 'static, 'static, entries::Unit>>()
            == 3 * size_of::<usize>()
    );
    assert!(!needs_drop::<
        Registration<'static, 'static, 'static, entries::Unit>,
    >());
};
