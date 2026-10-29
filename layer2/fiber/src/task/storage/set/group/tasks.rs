use core::pin;
use std::num::NonZero;

use o3::{
    depot,
    depot::{
        capacity,
        map::{self, flat},
    },
};

use crate::{
    abi,
    context::wake,
    core::driver::{
        retained,
        schedule::{app, ready::task::owner},
    },
    task::storage::{
        fixed::schedule::ready::owned,
        set,
        set::{
            group::{self, Arrival, Error, Policy, order},
            insert, ready,
            ready::owners,
            slot,
            slot::state::State,
        },
    },
};

/// A task set whose completed outputs are grouped in owner-local FIFO order.
pub struct Tasks<'s, 'd, F, M, P, D>
where
    F: abi::Fiber<'d>,
    P: Policy<M>,
{
    tasks: set::Set<'d, F, M, group::Scope<'s, D>, order::Membership<'d, group::Scope<'s, D>>>,
    orders: flat::Table<(P::Key, order::Order<'d, group::Scope<'s, D>>)>,
    policy: P,
    ready: group::raw::Ready<'s, 'd>,
}

impl<'s, 'd, F, M, P, D> Tasks<'s, 'd, F, M, P, D>
where
    F: abi::Fiber<'d>,
    P: Policy<M>,
{
    pub fn len(&self) -> set::Length {
        self.tasks.len()
    }

    pub fn contains(&self, key: P::Key) -> bool {
        self.orders
            .get(self.policy.hash(&key), |entry| entry.0 == key)
            .is_some()
    }

    pub fn try_insert(
        &mut self,
        fiber: F,
        data: M,
        parent: wake::RootWaker<'d>,
    ) -> Result<group::Id<'s, 'd, D>, insert::Error> {
        let owner = owners::owner(P::key(&data));
        let mut entries = self.tasks.pool.entries();
        let entry = entries.vacant().ok_or(insert::Error::Full)?;
        Ok(entry
            .try_owned(fiber, data, parent, self.ready.get(), owner)?
            .into_group())
    }

    pub fn access<R>(
        &mut self,
        id: &group::Id<'s, 'd, D>,
        access: impl FnOnce(pin::Pin<&mut F>) -> R,
    ) -> Option<R> {
        self.tasks.pool.entries().resident(id.key, access)
    }

    pub fn try_wake(&self, id: &group::Id<'s, 'd, D>) -> Result<(), set::WakeError> {
        self.tasks.pool.view().try_wake(id.key)
    }

    pub fn poll_ready(
        &mut self,
        work: app::Work<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> set::Length {
        owners::drive(
            self.ready.get(),
            self.tasks.pool.entries(),
            owner::Scope::Any,
            work,
            driver,
            |_, _| {},
        )
    }

    pub fn poll_with(
        &mut self,
        key: P::Key,
        work: app::Work<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
        observe: impl FnMut(&M, pin::Pin<&mut F>),
    ) -> set::Length {
        let owner = owners::owner(key);
        owners::drive(
            self.ready.get(),
            self.tasks.pool.entries(),
            owner::Scope::Exact(owner),
            work,
            driver,
            observe,
        )
    }

    pub fn wake_next(&mut self) {
        owners::wake(self.ready.get(), &self.tasks.pool.view());
    }

    pub fn work(&self) -> set::Work {
        self.work_in(owner::Scope::Any)
    }

    /// Reports work for this owner's current generation, including ingress
    /// that has not yet been assigned to owner queues.
    pub fn work_for(&self, key: P::Key) -> set::Work {
        self.work_in(owner::Scope::Exact(owners::owner(key)))
    }

    fn work_in(&self, scope: owner::Scope<owners::Source>) -> set::Work {
        use crate::task::storage::fixed::schedule::queue::Wake;
        match self.ready.get().readiness(scope) {
            Wake::Idle => set::Work::Idle,
            Wake::Ready | Wake::Pending => set::Work::Pending,
        }
    }

    /// Restricts access to the pinned owner queue to a fresh invariant task scope.
    pub fn with<R>(
        capacity: NonZero<u32>,
        owners: owner::Capacity,
        policy: P,
        run: impl for<'a> FnOnce(Tasks<'a, 'd, F, M, P, D>) -> R,
    ) -> Result<R, Error> {
        let plan = map::Plan::new(capacity.get() as usize).map_err(Error::Capacity)?;
        let ready = owned::Queue::try_new(capacity, owners).map_err(Error::Ready)?;
        let ready = pin::pin!(ready);
        let tasks =
            set::Set::try_new(capacity::Count::new(capacity.get())).map_err(Error::Tasks)?;
        let orders = flat::Table::try_new(plan).map_err(Error::Orders)?;
        let result = run(Tasks {
            tasks,
            orders,
            policy,
            // SAFETY: this fresh invariant scope cannot escape run. Every
            // queue access precedes the end of the stack-pinned queue's scope.
            ready: unsafe { group::raw::Ready::new(ready.as_ref()) },
        });
        Ok(result)
    }

    /// Releases one completed task back to its group's order.
    ///
    /// A group's order entry is inserted by its first arrival and taken by its
    /// last removal, so a group with a completed task is always still ordered.
    pub fn release_done<R>(&mut self, use_task: impl FnOnce(&M) -> R) -> Option<(Arrival, R)> {
        use flat::Entry;

        let Self {
            tasks,
            orders,
            policy,
            ..
        } = self;
        let (view, mut token) = tasks.pool.entries().into_parts();
        let done = ready::Queue::pop(view.reborrow(), &mut token)?;
        let task = done.data();
        let key = P::key(task);
        let result = use_task(task);
        let hash = policy.hash(&key);
        // SAFETY: both stores were admitted with the same capacity. Every order owns
        // an external task, while this retained completion is not yet external.
        let source = unsafe { depot::raw::Proof::new((orders, done)) };
        let (entry, done) = flat::Table::admit(source, hash, |stored| stored.0 == key);
        let arrival = match entry {
            Entry::Occupied(mut entry) => {
                let order = &mut entry.get_mut().1;
                done.queue(&mut order.fifo, (hash, order.identity));
                Arrival::More
            }
            Entry::Vacant(entry) => {
                let identity = (hash, done.key());
                entry.insert((key, order::Order::new(done, identity)));
                Arrival::First
            }
        };
        Some((arrival, result))
    }

    pub fn pop(&mut self, key: P::Key) -> Option<(M, F::Output)> {
        let Self {
            tasks,
            orders,
            policy,
            ..
        } = self;
        let hash = policy.hash(&key);
        let occupied = orders.occupied(hash, |stored| stored.0 == key)?;
        let (view, mut token) = tasks.pool.entries().into_parts();
        let done = slot::Listed::front(view, &mut token, occupied)
            .remove()
            .release();
        Some(done.take())
    }

    /// Takes one listed task out of its group, failing with the id when the
    /// group lists none.
    ///
    /// A listed task retains its group's order entry for exactly that listing.
    pub fn take(
        &mut self,
        id: group::Id<'s, 'd, D>,
    ) -> Result<(M, F::Output), group::Id<'s, 'd, D>> {
        let Self { tasks, orders, .. } = self;
        let (view, mut token) = tasks.pool.entries().into_parts();
        let Some(listed) = slot::Listed::get(view, &mut token, id.key) else {
            return Err(id);
        };
        Ok(listed.detach(orders).take())
    }

    /// Discards one task's completed output, or the queued task when it lists none.
    ///
    /// A listed task retains its group's order entry for exactly that listing.
    pub fn discard(&mut self, id: group::Id<'s, 'd, D>) {
        let Self { tasks, orders, .. } = self;
        let (view, mut token) = tasks.pool.entries().into_parts();
        let Some(listed) = slot::Listed::get(view, &mut token, id.key) else {
            let (view, mut token) = tasks.pool.entries().into_parts();
            let state = owners::remove(self.ready.get(), &view, &mut token, id.key);
            if state == Some(State::Queued) {
                owners::wake(self.ready.get(), &view);
            }
            return;
        };
        listed.detach(orders).discard();
    }
}
