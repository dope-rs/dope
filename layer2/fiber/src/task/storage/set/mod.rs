use std::{num::NonZero, pin, task::Poll};

use o3::{
    alloc,
    depot::{arena::slab, capacity},
    mem::quota,
};

use crate::{
    abi, context,
    context::wake,
    core::driver::{
        self,
        schedule::{app, ready::task, ready::task::owner},
    },
    task::storage::{
        fixed::schedule::{
            queue,
            ready::{Global, owned},
        },
        scope::{self, Scope},
        slots::{self, Action, Phase},
    },
};

pub mod insert;
mod raw;

pub type Id<'s, 'd, D> = scope::Scoped<'s, 'd, D, Global>;

type Value<'d, F, T> = slots::Task<Live<'d, F, T>, <F as abi::Fiber<'d>>::Output>;
type Pool<'d, F, T, D> = slab::Pinned<Value<'d, F, T>, D, 1, true, 1, slab::storage::Lazy>;

#[derive(Debug)]
pub enum Error {
    Ready(alloc::Error),
    Tasks(alloc::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WakeError {
    Inactive,
}

#[pin_project::pin_project]
struct Live<'d, F, T> {
    root: task::raw::Root<'d>,
    #[pin]
    fiber: F,
    target: T,
}

/// Pinned fibers whose sole identities retain their completed outputs.
#[pin_project::pin_project(!Unpin)]
pub struct Tasks<'d, F: abi::Fiber<'d>, T, D> {
    #[pin]
    slots: Pool<'d, F, T, D>,
    #[pin]
    ready: owned::Queue<'d, Source>,
}

impl<'d, F: abi::Fiber<'d>, T, D> Tasks<'d, F, T, D> {
    pub fn with<R>(
        capacity: NonZero<u32>,
        owners: owner::Capacity,
        run: impl for<'s> FnOnce(Scope<'s, pin::Pin<&'s mut Self>>) -> R,
    ) -> Result<R, Error> {
        let count = capacity::Count::new(capacity.get());
        let ready = owned::Queue::try_new(count, owners).map_err(Error::Ready)?;
        let slots = Pool::try_new(count).map_err(Error::Tasks)?;
        let mut tasks = pin::pin!(Self { slots, ready });
        Ok(Scope::with(tasks.as_mut(), run))
    }
}

impl<'s, 'a, 'd, F: abi::Fiber<'d>, T, D> Scope<'s, pin::Pin<&'a mut Tasks<'d, F, T, D>>>
{
    pub fn try_insert(
        &mut self,
        fiber: F,
        target: T,
        parent: wake::RootWaker<'d>,
        owner: impl owner::Identity,
    ) -> Result<Id<'s, 'd, D>, insert::Error> {
        let this = self.source.as_mut().project();
        let vacant = this.slots.vacant().ok_or(insert::Error::Full)?;
        let index = Global::from_slot(vacant.index());
        let prepared = this
            .ready
            .into_ref()
            .rebind(identify(owner))
            .map_err(owner_error)
            .map_err(insert::Error::Owner)?;
        let wake: wake::Waker<'d> = parent.into();
        let admit = wake.0.try_admit().map_err(insert::Error::Admission)?;
        let (edge, prepared) = prepared
            .try_prepare(index, wake.0)
            .map_err(insert::Error::Topology)?;
        // SAFETY: admission and edge retain the same original parent.
        let root = unsafe { task::raw::Root::bind(admit, edge) };
        let _ = vacant.occupy(slots::Task::new(Live { root, fiber, target }));
        prepared.attach().enqueue();
        parent.wake();
        Ok(scope::Id::new(index).bind(self.proof))
    }

    pub fn access<R>(
        &mut self,
        id: &Id<'s, 'd, D>,
        access: impl FnOnce(pin::Pin<&mut F>) -> R,
    ) -> Option<R> {
        // SAFETY: this scoped non-Copy identity retains its occupied slot.
        let mut entry = unsafe { raw::entry(self.source.as_mut().project().slots, *id.key()) };
        match entry
            .value()
            .visit((), |live, ()| Action::Keep(access(live.project().fiber)))
        {
            Phase::Live(result) => Some(result),
            Phase::Done(()) => None,
            Phase::Vacant(()) => unreachable!("vacant"),
        }
    }

    pub fn try_wake(&mut self, id: &Id<'s, 'd, D>) -> Result<(), WakeError> {
        // SAFETY: this scoped non-Copy identity retains its occupied slot.
        let mut entry = unsafe { raw::entry(self.source.as_mut().project().slots, *id.key()) };
        match entry.value().visit((), |live, ()| {
            wake::Waker::from_parent(live.root.poll().1).wake();
            Action::Keep(())
        }) {
            Phase::Live(()) => Ok(()),
            Phase::Done(()) => Err(WakeError::Inactive),
            Phase::Vacant(()) => unreachable!("vacant"),
        }
    }

    pub fn poll_with(
        &mut self,
        owner: impl owner::Identity,
        work: app::Work<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
        mut observe: impl FnMut(&T, pin::Pin<&mut F>, Poll<()>),
    ) {
        let mut this = self.source.as_mut().project();
        let ready = this.ready.into_ref();
        for _ in (0..work.remaining().saturating_add(1)).map_while(|_| ready.sync()) {}
        let scope = owner::Scope::Exact(identify(owner));
        while let quota::Admission::Item((dequeued, permit)) = work.claim(|| ready.dequeue(scope))
        {
            // SAFETY: this paired queue's selected node retains its live occupied slot.
            let mut entry = unsafe { raw::entry(this.slots.as_mut(), dequeued.index()) };
            // SAFETY: only live tasks remain attached; completion detaches below.
            let complete = unsafe {
                entry.value().visit_live((), |live, ()| {
                    let fields = live.project();
                    let (parent, waker) = fields.root.poll();
                    let result = context::Context::with_waker(
                        wake::Waker::from_parent(waker),
                        wake::RootWaker::from(parent),
                        work,
                        driver.reborrow(),
                        |mut context| {
                            let mut fiber = fields.fiber;
                            let result = context::raw::Live::new(fiber.as_mut())
                                .with(context.as_mut(), permit);
                            let status = match &result {
                                Poll::Pending => Poll::Pending,
                                Poll::Ready(_) => Poll::Ready(()),
                            };
                            observe(fields.target, fiber, status);
                            result
                        },
                    );
                    match result {
                        Poll::Pending => Action::Keep(false),
                        Poll::Ready(output) => Action::Complete(output, true),
                    }
                })
            };
            if complete {
                dequeued.detach();
            } else {
                dequeued.idle();
            }
        }
        // The mutable slot projection ends before selecting the next parent.
        self.wake_next();
    }

    pub fn has_work_for(&self, owner: impl owner::Identity) -> bool {
        !matches!(
            self.source
                .as_ref()
                .project_ref()
                .ready
                .readiness(owner::Scope::Exact(identify(owner))),
            queue::Wake::Idle
        )
    }

    pub fn wake_next(&mut self) {
        let this = self.source.as_mut().project();
        if let Some((index, _)) = this.ready.into_ref().front(owner::Scope::Any) {
            // SAFETY: queue membership or ingress retains this same live slot.
            let mut entry = unsafe { raw::entry(this.slots, index) };
            // SAFETY: completed tasks have already detached their queue membership.
            unsafe {
                entry.value().visit_live((), |live, ()| {
                    live.root.parent().wake();
                    Action::Keep(())
                })
            };
        }
    }

    pub fn take(&mut self, id: Id<'s, 'd, D>) -> Result<F::Output, Id<'s, 'd, D>> {
        // SAFETY: consuming this scoped identity retains the sole physical release authority.
        let mut entry = unsafe { raw::entry(self.source.as_mut().project().slots, *id.key()) };
        let Some(output) = entry.value().take() else {
            return Err(id);
        };
        entry.discard();
        Ok(output)
    }

    pub fn discard(&mut self, id: Id<'s, 'd, D>) {
        let this = self.source.as_mut().project();
        let index = *id.key();
        // SAFETY: consuming this scoped identity retains the sole physical release authority.
        let mut entry = unsafe { raw::entry(this.slots, index) };
        entry.value().cancel(|| this.ready.into_ref().detach(index));
        entry.discard();
        self.wake_next();
    }
}

#[derive(Clone, Copy)]
struct Source;

#[derive(Clone, Copy)]
struct Identity(owner::Lane, owner::Generation);

impl owner::Identity for Identity {
    fn parts(self) -> (owner::Lane, owner::Generation) {
        (self.0, self.1)
    }
}

impl owner::Source for Source {
    type Identity = Identity;
}

fn identify(key: impl owner::Identity) -> owner::Owner<Source> {
    let (lane, generation) = key.parts();
    owner::Owner::from(Identity(lane, generation))
}

fn owner_error(error: queue::OwnerError<Source>) -> owner::Error {
    match error {
        queue::OwnerError::Foreign { owner } => owner::Error::Foreign {
            lane: owner.lane(),
            generation: owner.generation(),
        },
        queue::OwnerError::Stale { owner, current } => owner::Error::Stale {
            lane: owner.lane(),
            generation: owner.generation(),
            current,
        },
    }
}
