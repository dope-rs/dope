use std::{mem::MaybeUninit, num::NonZero, ops::ControlFlow};

use o3::depot::{arena::slab, array::init, queue::pool::fifo};
use slab::{Vacancy, entries::Pending};
use state::order::Queues;

use crate::reply::{
    self, Registration,
    arena::state,
    layout,
    recv::{self, attach},
    retain::{
        grant::{self, entries},
        pool::{self, Lane},
    },
};

/// Exclusive registration access to the owning reply book.
pub(in crate::reply::arena) struct Register<'a, 'arena, 'credits, 'region, D, E, K> {
    state: &'a mut state::Book<'arena, 'credits, 'region, D, E, K>,
}

type Vacant<'a, 'arena, 'credits, 'region, D, E, const ROOT: bool> =
    slab::Vacant<Pending<'a, state::Entry<'arena, 'credits, 'region, D, E>>, state::EntryTag, ROOT>;

impl<'a, 'arena, 'credits, 'region, D, E, K> Register<'a, 'arena, 'credits, 'region, D, E, K> {
    pub(in crate::reply::arena) const fn new(
        state: &'a mut state::Book<'arena, 'credits, 'region, D, E, K>,
    ) -> Self {
        Self { state }
    }

    pub(in crate::reply::arena) fn registration(
        &self,
        lane: reply::Lane<'_, 'arena, 'credits, 'region, D, E, K>,
    ) -> Registration {
        if self.state.slots.is_full() {
            return if self.state.slots.is_exhausted() {
                Registration::IdentityExhausted
            } else {
                Registration::Full
            };
        }
        Lane::new(lane).registration(entries::Units::<1, 1>::VALUE)
    }

    pub(in crate::reply::arena) fn insert(
        &mut self,
        lane: reply::Lane<'_, 'arena, 'credits, 'region, D, E, K>,
        accept: recv::Acceptance,
        tag: K,
        data: fifo::Fifo<'arena, reply::Item<D>, false>,
    ) -> Result<state::EntryKey, attach::Error> {
        let state = &mut *self.state;
        let full = attach::Error::Full {
            len: state.slots.0.len(),
            capacity: layout::Entries::new(state.slots.capacity()),
        };
        let vacant = state.slots.0.try_vacant().map_err(|cause| match cause {
            Vacancy::Full => full,
            Vacancy::IdentityExhausted => attach::Error::IdentityExhausted,
        })?;
        let capacity = Lane::new(lane);
        let plan = pool::Registration::try_new(capacity, entries::Units::<1, 1>::VALUE).map_err(
            |cause| attach::Error::Capacity {
                lane: lane.index,
                cause,
            },
        )?;
        if !capacity.has_reserve() {
            state.order.remove::<1>(capacity.index().into());
        }
        Ok(Self::publish(
            &mut state.order,
            lane,
            vacant,
            plan,
            accept,
            tag,
            data,
        ))
    }

    /// Holds every slot and credit before consuming input for infallible publication.
    pub(in crate::reply::arena) fn group_with<I, R, const N: usize>(
        &mut self,
        lane: reply::Lane<'_, 'arena, 'credits, 'region, D, E, K>,
        accept: recv::Acceptance,
        mode: recv::Acceptance,
        input: I,
        make: impl FnOnce(I) -> ([K; N], K, R),
    ) -> Result<([state::EntryKey; N], state::EntryKey, R), (attach::Error, I)> {
        let request = entries::Group::<N>::VALUE;
        let state = &mut *self.state;
        let needed = NonZero::<usize>::MIN.saturating_add(N);
        let capacity = grant::Plan::<2>::entry_limit(state.slots.capacity(), lane.arena.lanes);
        if needed.get() > capacity {
            return Err((
                attach::Error::Oversized {
                    requested: needed,
                    capacity: layout::Entries::new(capacity),
                },
                input,
            ));
        }
        if needed.get() > state.slots.0.remaining().get() {
            return Err((attach::Error::IdentityExhausted, input));
        }
        let full = attach::Error::Full {
            len: state.slots.0.len(),
            capacity: layout::Entries::new(state.slots.capacity()),
        };
        let mut free = state.slots.0.vacancies();
        let Some(terminal) = free.next() else {
            return Err((full, input));
        };
        let mut storage = MaybeUninit::uninit();
        let mut commands = init::Guard::<_, N, true>::new(&mut storage);
        let commands = loop {
            match commands.next() {
                ControlFlow::Break(full) => break full,
                ControlFlow::Continue(next) => {
                    let Some(vacant) = free.next() else {
                        return Err((full, input));
                    };
                    commands = next.insert(vacant);
                }
            }
        };
        let capacity = Lane::new(lane);
        let plan = match pool::Registration::try_new(capacity, request) {
            Ok(plan) => plan,
            Err(cause) => {
                return Err((
                    attach::Error::Capacity {
                        lane: lane.index,
                        cause,
                    },
                    input,
                ));
            }
        };
        let (tags, end, output) = make(input);
        let (plans, tail) = plan.split_group(tags);
        if !capacity.has_reserve() {
            state.order.remove::<1>(capacity.index().into());
        }
        let keys = commands.map_with(plans, |vacant, (plan, tag)| {
            Self::publish(
                &mut state.order,
                lane,
                vacant,
                plan,
                accept,
                tag,
                lane.arena.backing.items.fifo(),
            )
        });
        let terminal = Self::publish(
            &mut state.order,
            lane,
            terminal,
            tail,
            mode,
            end,
            lane.arena.backing.items.fifo(),
        );
        Ok((keys, terminal, output))
    }

    /// Publishes one attachment from a distinct reserved entry in this bounded store.
    fn publish<const ROOT: bool>(
        queues: &mut Queues<'credits, K>,
        lane: reply::Lane<'_, 'arena, 'credits, 'region, D, E, K>,
        vacant: Vacant<'_, 'arena, 'credits, 'region, D, E, ROOT>,
        plan: pool::Registration<'arena, 'credits, 'region, entries::Units<1, 1>>,
        accept: recv::Acceptance,
        tag: K,
        data: fifo::Fifo<'arena, reply::Item<D>, false>,
    ) -> state::EntryKey {
        let (entry, order) = plan.split();
        let raw = vacant.insert_with(|raw| {
            let route = queues.route(lane.index).into_state();
            route.live = route.live.wrapping_add(1);
            // SAFETY: the vacancy retains Entries' validated physical domain.
            let key = unsafe { state::EntryKey::from_raw(raw) };
            state::Entry {
                body: state::EntryBody::Attached(state::Attached(reply::State::new(data))),
                acceptance: accept,
                owners: entry.register(key),
                signal: state::Signal::new(),
            }
        });
        // SAFETY: insertion preserves the reserved physical identity.
        let key = unsafe { state::EntryKey::from_raw(raw) };
        queues
            .route(lane.index)
            .push(state::Ordered::new(key, order, tag));
        key
    }
}
