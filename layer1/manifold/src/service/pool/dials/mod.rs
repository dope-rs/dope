use std::{cell::Cell, marker, mem};

use dope_core::{
    driver::{
        route::{
            Identity, KeyTag, indices,
            table::{self, conn},
        },
        schedule::clock,
    },
    io::socket::option,
};
use dope_net::link::pool::change::opening;
use o3::{
    alloc,
    depot::{arena::slab::key, capacity},
};

use crate::{
    client::attempt,
    service::{
        self,
        pool::{self, members},
        sync,
    },
};

mod connect;
pub(super) use connect::Connection;

#[derive(Clone, Copy)]
enum State<'h> {
    Ready,
    Submitted(pool::Target<'h>),
    Connected(pool::Target<'h>),
    Closed,
    Exhausted,
}

struct Dial<'q, 'h, 'd, ID: Identity> {
    generation: Cell<key::Generation>,
    next: Cell<Option<Ready<'q, 'h, 'd, ID>>>,
    state: Cell<State<'h>>,
    slot: attempt::Slot,
    brand: marker::PhantomData<attempt::Id<'d, ID>>,
}

struct Ready<'q, 'h, 'd, ID: Identity>(&'q Dial<'q, 'h, 'd, ID>);

struct Chain<'q, 'h, 'd, ID: Identity> {
    front: Ready<'q, 'h, 'd, ID>,
    back: &'q Cell<Option<Ready<'q, 'h, 'd, ID>>>,
}

struct Held<'q, 'h, 'd, ID: Identity, T: dope_net::Transport> {
    chain: Chain<'q, 'h, 'd, ID>,
    target: pool::Target<'h>,
    plan: attempt::Plan<T>,
}

enum Queue<'q, 'h, 'd, ID: Identity, T: dope_net::Transport> {
    Open(Option<Chain<'q, 'h, 'd, ID>>),
    Deferred(Held<'q, 'h, 'd, ID, T>),
    Rejected(Option<(attempt::Id<'d, ID>, opening::Exhaustion)>),
}

enum Tail<'a, 'q, 'h, 'd, ID: Identity> {
    First(&'a mut Option<Chain<'q, 'h, 'd, ID>>),
    Last(&'a mut Chain<'q, 'h, 'd, ID>),
}

pub(super) struct Dials<'q, 'h, 'd, ID: Identity, T: dope_net::Transport> {
    records: &'q [Dial<'q, 'h, 'd, ID>],
    ready: Queue<'q, 'h, 'd, ID, T>,
    capacity: conn::Capacity,
    depleted: bool,
    pub(super) needs_poll: bool,
}

impl<ID: Identity> Default for Dial<'_, '_, '_, ID> {
    fn default() -> Self {
        Self {
            generation: Cell::new(key::Generation::MIN),
            next: Cell::new(None),
            state: Cell::new(State::Ready),
            slot: attempt::Slot::new(indices::SlotIndex::ZERO),
            brand: marker::PhantomData,
        }
    }
}

impl<'q, 'h, 'd, ID: Identity> Dial<'q, 'h, 'd, ID> {
    fn requeue(&'q self, ready: Tail<'_, 'q, 'h, 'd, ID>) {
        if let Some(epoch) = self.generation.get().checked_add(1) {
            self.generation.set(epoch);
            self.state.set(State::Ready);
            self.next.set(None);
            ready.push_back(Ready(self));
        } else {
            self.state.set(State::Exhausted);
        }
    }

    fn matches(&self, key: attempt::Id<'d, ID>) -> bool {
        self.generation.get() == key.generation()
    }

    fn active(&self) -> Option<pool::MemberKey<'h>> {
        let (State::Submitted(target) | State::Connected(target)) = self.state.get() else {
            return None;
        };
        Some(target.member)
    }
}

impl<T: dope_net::Transport, ID: Identity> Queue<'_, '_, '_, ID, T> {
    const EMPTY: Self = Self::Open(None);

    const LAYOUT: () = {
        assert!(
            mem::size_of::<Self>()
                <= mem::size_of::<Held<'_, '_, '_, ID, T>>()
                    + mem::align_of::<Held<'_, '_, '_, ID, T>>()
        );
        assert!(mem::align_of::<Self>() == mem::align_of::<Held<'_, '_, '_, ID, T>>());
    };
}

impl<'a, 'q, 'h, 'd, ID: Identity> From<&'a mut Option<Chain<'q, 'h, 'd, ID>>>
    for Tail<'a, 'q, 'h, 'd, ID>
{
    fn from(chain: &'a mut Option<Chain<'q, 'h, 'd, ID>>) -> Self {
        match chain {
            Some(chain) => Self::Last(chain),
            None => Self::First(chain),
        }
    }
}

impl<'q, 'h, 'd, ID: Identity> Tail<'_, 'q, 'h, 'd, ID> {
    fn push_back(self, ready: Ready<'q, 'h, 'd, ID>) {
        let back = &ready.0.next;
        match self {
            Self::First(chain) => {
                *chain = Some(Chain { front: ready, back });
            }
            Self::Last(chain) => {
                chain.back.set(Some(ready));
                chain.back = back;
            }
        }
    }
}

impl<'q, 'h, 'd, ID: Identity, T: dope_net::Transport> Dials<'q, 'h, 'd, ID, T> {
    pub(super) fn try_with<O>(
        cap: conn::Capacity,
        build: impl for<'a> FnOnce(Dials<'a, 'h, 'd, ID, T>) -> O,
    ) -> Result<O, alloc::Error> {
        let () = Queue::<ID, T>::LAYOUT;
        let mut records: Box<[Dial<'_, 'h, 'd, ID>]> =
            alloc::BoxSliceExt::try_default(cap.entries().get())?;
        for (record, index) in records.iter_mut().zip(table::Capacity::from(cap)) {
            record.slot = attempt::Slot::new(index);
        }
        let mut ready = None;
        for record in &records {
            Tail::from(&mut ready).push_back(Ready(record));
        }
        Ok(build(Dials {
            records: &records,
            ready: Queue::Open(ready),
            capacity: cap,
            depleted: false,
            needs_poll: false,
        }))
    }

    fn get(&self, key: attempt::Id<'d, ID>) -> Option<&'q Dial<'q, 'h, 'd, ID>> {
        let dial = self.records.get(key.index().into_usize())?;
        dial.matches(key).then_some(dial)
    }

    pub(super) const fn capacity(&self) -> capacity::Count {
        self.capacity.scale::<1>()
    }

    pub(super) fn active(&self, key: attempt::Id<'d, ID>) -> Option<pool::MemberKey<'h>> {
        self.get(key)?.active()
    }

    pub(super) fn poll<I, E, C, R: sync::Policy>(
        &mut self,
        members: &mut members::Set<'h, 'd, I, T::Addr, R>,
        options: option::Set,
        now: clock::Moment<'d>,
        context: &mut C,
        mut eligible: impl FnMut(&I, &mut C) -> bool,
        submit: impl FnOnce(
            attempt::Id<'d, ID>,
            attempt::Plan<T>,
            &mut C,
        ) -> opening::Attempt<'d, ID, attempt::Plan<T>, (), E>,
    ) -> service::Opening<'d, E, ID> {
        use service::Opening;
        let present = !members.is_empty();
        let (dial, chain, target, plan) = match mem::replace(&mut self.ready, Queue::EMPTY) {
            Queue::Open(None) => {
                self.needs_poll = false;
                return Opening::Idle;
            }
            Queue::Rejected(cause) => {
                self.ready = Queue::Rejected(cause);
                return self.terminal();
            }
            Queue::Open(Some(chain)) => {
                let (target, addr) =
                    match members.choose(now, |endpoint| eligible(endpoint, context)) {
                        members::Selection::Ready { target, addr } => (target, addr),
                        members::Selection::Idle => {
                            self.ready = Queue::Open(Some(chain));
                            self.needs_poll = false;
                            return Opening::Idle;
                        }
                        members::Selection::Backoff(at) => {
                            self.ready = Queue::Open(Some(chain));
                            self.needs_poll = false;
                            return Opening::Backoff(at);
                        }
                    };
                let plan = attempt::Plan::new(addr, options);
                let dial = chain.front.0;
                (dial, chain, target, plan)
            }
            Queue::Deferred(held) => {
                let dial = held.chain.front.0;
                (dial, held.chain, held.target, held.plan)
            }
        };
        let key = attempt::Id::from_epoch(dial.slot, dial.generation.get());
        match submit(key, plan, context) {
            opening::Attempt::Submitted { key: _, output: () } => {
                dial.state.set(State::Submitted(target));
                let next = dial.next.take().map(|front| Chain {
                    front,
                    back: chain.back,
                });
                self.needs_poll = present && next.is_some();
                self.ready = Queue::Open(next);
                Opening::Submitted
            }
            opening::Attempt::Deferred { cause, input: plan } => {
                self.ready = Queue::Deferred(Held {
                    chain,
                    target,
                    plan,
                });
                self.needs_poll = present || !R::AUTHORITATIVE;
                Opening::Deferred {
                    attempt: key,
                    cause,
                }
            }
            opening::Attempt::Failed { error } => {
                dial.next.set(None);
                members.cancel(target);
                dial.state.set(State::Closed);
                match error.into_wire() {
                    Ok(cause) => {
                        self.ready = Queue::Rejected(None);
                        self.needs_poll = false;
                        Opening::Failed {
                            attempt: key,
                            error: opening::Failure::Wire(cause),
                        }
                    }
                    Err(cause) => {
                        self.reject(key, cause, |target| {
                            members.cancel(target);
                        });
                        self.terminal()
                    }
                }
            }
        }
    }

    pub(super) fn connection(
        &mut self,
        key: attempt::Id<'d, ID>,
    ) -> Connection<'_, 'q, 'h, 'd, ID> {
        let Some(dial) = self.get(key) else {
            return Connection::Stale;
        };
        let State::Submitted(target) = dial.state.get() else {
            return Connection::Stale;
        };
        Connection::Pending(connect::Connect { dial, target })
    }

    pub(super) fn recycle(
        &mut self,
        key: attempt::Id<'d, ID>,
        present: bool,
        update: impl FnOnce(pool::Target<'h>),
    ) {
        let Some(dial) = self.get(key) else {
            return;
        };
        let (State::Submitted(target) | State::Connected(target)) = dial.state.get() else {
            return;
        };
        update(target);
        self.requeue(dial, present);
    }

    pub(super) fn retry(
        &mut self,
        key: attempt::Id<'d, ID>,
        present: bool,
        update: impl FnOnce(pool::Target<'h>),
    ) {
        let Some(dial) = self.get(key) else {
            return;
        };
        let State::Submitted(target) = dial.state.get() else {
            return;
        };
        update(target);
        self.requeue(dial, present);
    }

    /// Member admission has finished; every terminal attempt releases its own dial slot.
    fn requeue(&mut self, dial: &'q Dial<'q, 'h, 'd, ID>, present: bool) {
        let ready = match &mut self.ready {
            Queue::Open(chain) => Tail::from(chain),
            Queue::Deferred(held) => Tail::Last(&mut held.chain),
            Queue::Rejected(_) => {
                dial.state.set(State::Closed);
                self.refresh(present);
                return;
            }
        };
        dial.requeue(ready);
        self.depleted |= matches!(dial.state.get(), State::Exhausted);
        self.refresh(present);
    }

    /// Closing the dial does not depend on the continued existence of its member.
    pub(super) fn close(&mut self, key: attempt::Id<'d, ID>) -> Option<pool::Target<'h>> {
        let dial = self.get(key)?;
        let (State::Submitted(target) | State::Connected(target)) = dial.state.get() else {
            return None;
        };
        dial.state.set(State::Closed);
        Some(target)
    }

    pub(super) fn reset_front(&mut self, removed: impl FnOnce(pool::Target<'h>) -> bool) {
        self.ready = match mem::replace(&mut self.ready, Queue::EMPTY) {
            Queue::Deferred(held) if removed(held.target) => Queue::Open(Some(held.chain)),
            queue => queue,
        };
    }

    pub(super) fn refresh(&mut self, present: bool) {
        self.needs_poll = match self.ready {
            Queue::Open(Some(_)) | Queue::Deferred(_) => present,
            Queue::Rejected(Some(_)) => !self.records.iter().any(|dial| dial.active().is_some()),
            Queue::Open(None) | Queue::Rejected(None) => false,
        };
        if self.depleted && matches!(self.ready, Queue::Open(None)) {
            let mut retired = None;
            for dial in self.records {
                match dial.state.get() {
                    State::Exhausted => {
                        retired = Some(attempt::Id::from_epoch(dial.slot, dial.generation.get()));
                    }
                    State::Closed => {}
                    State::Ready | State::Submitted(_) | State::Connected(_) => return,
                }
            }
            if let Some(key) = retired {
                self.ready = Queue::Rejected(Some((key, opening::Exhaustion::Identity)));
                self.needs_poll = true;
            }
        }
    }

    /// Retain the first permanent opening failure until every issued connection retires.
    pub(super) fn reject(
        &mut self,
        key: attempt::Id<'d, ID>,
        cause: opening::Exhaustion,
        cancel: impl FnOnce(pool::Target<'h>),
    ) {
        if !matches!(self.ready, Queue::Rejected(_))
            && let Queue::Deferred(held) =
                mem::replace(&mut self.ready, Queue::Rejected(Some((key, cause))))
        {
            cancel(held.target);
        }
        self.refresh(false);
    }

    /// Takes one ready terminal notification without changing ordinary dial readiness.
    pub(super) fn terminal<E>(&mut self) -> service::Opening<'d, E, ID> {
        let cause = match &mut self.ready {
            Queue::Rejected(cause) if self.needs_poll => cause,
            Queue::Rejected(_) | Queue::Open(_) | Queue::Deferred(_) => {
                return service::Opening::Idle;
            }
        };
        self.needs_poll = false;
        match cause.take() {
            Some((attempt, error)) => service::Opening::Failed {
                attempt,
                error: error.into(),
            },
            None => service::Opening::Idle,
        }
    }

    pub(super) fn is_deferred(&self) -> bool {
        matches!(self.ready, Queue::Deferred(_))
    }
}

/// Retaining `Target<'h>` in active states does not enlarge this state machine.
const _: () = assert!(
    mem::size_of::<State<'static>>()
        == mem::size_of::<pool::Target<'static>>() + mem::size_of::<usize>()
);

const _: () = {
    use dope_net::tcp;

    assert!(mem::size_of::<Dial<'static, 'static, 'static, KeyTag<0>>>() == 40);
    assert!(
        mem::size_of::<Ready<'static, 'static, 'static, KeyTag<0>>>() == mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Option<Ready<'static, 'static, 'static, KeyTag<0>>>>()
            == mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Chain<'static, 'static, 'static, KeyTag<0>>>()
            == 2 * mem::size_of::<usize>()
    );
    assert!(mem::size_of::<Queue<'static, 'static, 'static, KeyTag<0>, tcp::Tcp>>() == 96);
    assert!(
        mem::size_of::<Tail<'static, 'static, 'static, 'static, KeyTag<0>>>()
            == 2 * mem::size_of::<usize>()
    );
    assert!(mem::size_of::<Dials<'static, 'static, 'static, KeyTag<0>, tcp::Tcp>>() == 120);
};
