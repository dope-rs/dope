use std::{mem::MaybeUninit, num::NonZero, ops::ControlFlow};

use o3::{
    base::cell::region,
    depot::{arena::slab, array::init},
};
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
    usage::Footprint,
};

pub(in crate::reply::arena) struct Ordered<K = ()> {
    key: state::EntryKey,
    tag: K,
}

impl<K> Ordered<K> {
    pub(super) const fn key(&self) -> state::EntryKey {
        self.key
    }

    pub(super) const fn tag(&self) -> &K {
        &self.tag
    }
}

type Vacant<'a, 'arena, 'credits, 'region, D, E, const ROOT: bool> =
    slab::Vacant<Pending<'a, state::Entry<'arena, 'credits, 'region, D, E>>, state::EntryTag, ROOT>;

impl<'arena, 'credits, 'region, D: Footprint, E, K>
    state::Book<'arena, 'credits, 'region, D, E, K>
{
    pub(in crate::reply::arena) fn registration(
        &self,
        lane: reply::Lane<'_, 'arena, 'credits, 'region, D, E, K>,
    ) -> Registration {
        if self.slots.is_full() {
            return if self.slots.is_exhausted() {
                Registration::IdentityExhausted
            } else {
                Registration::Full
            };
        }
        Lane::new(lane).registration(entries::Unit)
    }

    pub(in crate::reply::arena) fn insert(
        &mut self,
        lane: reply::Lane<'_, 'arena, 'credits, 'region, D, E, K>,
        accept: recv::Acceptance,
        tag: K,
    ) -> Result<state::EntryKey, attach::Error> {
        let state = self;
        let full = attach::Error::Full {
            len: state.slots.0.len(),
            capacity: layout::Entries::new(state.slots.capacity()),
        };
        let vacant = state.slots.0.try_vacant().map_err(|cause| match cause {
            Vacancy::Full => full,
            Vacancy::IdentityExhausted => attach::Error::IdentityExhausted,
        })?;
        let capacity = Lane::new(lane);
        let plan = pool::Registration::try_new(capacity, entries::Unit).map_err(|cause| {
            attach::Error::Capacity {
                lane: lane.index,
                cause,
            }
        })?;
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
        ))
    }

    /// Publishes one attachment from a distinct reserved entry in this bounded store.
    fn publish<const ROOT: bool>(
        queues: &mut Queues<K>,
        lane: reply::Lane<'_, 'arena, 'credits, 'region, D, E, K>,
        vacant: Vacant<'_, 'arena, 'credits, 'region, D, E, ROOT>,
        plan: pool::Registration<'arena, 'credits, 'region, entries::Unit>,
        accept: recv::Acceptance,
        tag: K,
    ) -> state::EntryKey {
        let raw = vacant.insert_with(|raw| {
            // SAFETY: the vacancy retains Entries' validated physical domain.
            let key = unsafe { state::EntryKey::from_raw(raw) };
            state::Entry {
                body: state::EntryBody::Attached(state::Attached(reply::State::new(
                    lane.arena.backing.items.fifo(),
                ))),
                owners: pool::Owner::new(plan, key.index(), accept),
                signal: state::Signal::new(),
            }
        });
        // SAFETY: insertion preserves the reserved physical identity.
        let key = unsafe { state::EntryKey::from_raw(raw) };
        queues.route(lane.index).push(Ordered { key, tag });
        key
    }
}

impl<'s, 'arena, 'credits, 'region, D: Footprint, E, K>
    reply::Lane<'s, 'arena, 'credits, 'region, D, E, K>
{
    pub(in crate::reply) fn group_with<I, V, F, O, R, const N: usize>(
        self,
        token: &mut region::Token<region::Identity<'region>>,
        (accept, mode): (recv::Acceptance, recv::Acceptance),
        outputs: &mut [O; N],
        input: I,
        make: impl FnOnce(I) -> ([V; N], F, K, R),
        mut attach: impl FnMut(&mut O, state::EntryKey),
    ) -> Result<(state::EntryKey, R), (attach::Error, I)>
    where
        F: FnMut(usize, V) -> K,
    {
        let request = entries::Group::<N>::VALUE;
        let lane = self;
        let state = token.borrow_mut(&lane.arena.state);
        let needed = NonZero::<usize>::MIN.saturating_add(N);
        let capacity = grant::Plan::<1>::entry_limit(state.slots.capacity(), lane.arena.lanes);
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
        let (values, mut tag, end, output) = make(input);
        if !capacity.has_reserve() {
            state.order.remove::<1>(capacity.index().into());
        }
        let tail = plan.map_group(
            commands,
            values,
            outputs,
            |index, vacant, plan, value, out| {
                let key = state::Book::publish(
                    &mut state.order,
                    lane,
                    vacant,
                    plan,
                    accept,
                    tag(index, value),
                );
                attach(out, key);
            },
        );
        let terminal = state::Book::publish(&mut state.order, lane, terminal, tail, mode, end);
        Ok((terminal, output))
    }
}
