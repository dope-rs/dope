use std::{marker, num, pin, task::Poll};

use o3::alloc;
use queue::Backend as _;
use schedule::{
    ready::{Global, Path, linked},
    result::Outcome,
    selected,
    selected::{Access, EntryRef},
};

use crate::{
    abi, context,
    context::wake,
    core::driver::{
        retained,
        schedule::{
            app,
            app::step,
            ready::{task, task::Error},
        },
    },
    task::storage::{
        Id, Owner, caps,
        fixed::{local, schedule, schedule::queue, tree},
    },
};

mod proof;
mod slab;

pub(crate) use proof::Proof;
use slab::{Occupied, Slab};

#[pin_project::pin_project]
struct Entry<'d, F, T: Copy> {
    #[pin]
    fiber: F,
    binding: task::raw::Root<'d>,
    #[pin]
    ready: linked::Entry,
    target: T,
}

impl<'d, F, T: Copy> Entry<'d, F, T> {
    const fn new(fiber: F, target: T, binding: task::raw::Root<'d>, index: Global) -> Self {
        Self {
            fiber,
            binding,
            ready: linked::Entry::new(index),
            target,
        }
    }
}

/// Fixed task storage that retires each slot after `MAX` generations.
/// The queue binding, target, and driver lease are fiber lifecycle concerns, so
/// only the slots and their pinned access are delegated to o3.
#[pin_project::pin_project(!Unpin)]
pub struct Pool<'d, F, const N: usize, D, T: Copy = (), const MAX: u32 = { u32::MAX }>
where
    F: abi::Fiber<'d>,
{
    #[pin]
    inner: Slab<Entry<'d, F, T>, D, N, MAX>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, F, const N: usize, D, T, const MAX: u32> Pool<'d, F, N, D, T, MAX>
where
    F: abi::Fiber<'d>,
    T: Copy,
{
    pub fn try_new() -> Result<Self, alloc::Error> {
        let inner = Slab::try_new(caps::Tasks::new())?;
        Ok(Self {
            inner,
            driver: marker::PhantomData,
        })
    }
}

impl<'d, F, const N: usize, D, T, const MAX: u32> schedule::Ops<'d> for Pool<'d, F, N, D, T, MAX>
where
    F: abi::Fiber<'d>,
    T: Copy,
{
    fn target_index(entry: EntryRef<'_, Self>) -> T {
        Slab::selected(entry).target
    }

    fn try_admit<'a, S, R, P>(
        path: Path<'a, 'd, R, P>,
        queue: pin::Pin<&'a S>,
        input: S::Input,
        parent: wake::RootWaker<'d>,
    ) -> Result<impl FnOnce((F, T)) -> Id<'d, D>, S::Error>
    where
        S: queue::Backend<'d>,
        R: tree::At<'d, P, Member = Self>,
    {
        let (member, proof) = path.split();
        let owner = Owner::of(member.as_ref().get_ref());
        let this = member.project();
        let Some(entry) = this.inner.vacant() else {
            return Err(S::Error::from(Error::Capacity {
                requested: num::NonZero::<usize>::MIN,
                available: 0,
            }));
        };
        let global = Path::<R, P>::index(proof, entry.index());
        let (binding, prepared) = S::try_prepare(queue, global, input, parent)?;
        Ok(move |(fiber, target)| {
            let (key, occupied) = entry.occupy(Entry::new(fiber, target, binding, global));
            let fields = occupied.into_ref().project_ref();
            S::admit(prepared, fields.ready);
            Id::from_raw(key, owner)
        })
    }

    fn try_spawn<'a, R, P>(
        path: Path<'a, 'd, R, P>,
        queue: pin::Pin<&'a queue::Flat<'d>>,
        parent: wake::RootWaker<'d>,
    ) -> Result<impl FnOnce((F, T)) + 'a, Error>
    where
        R: tree::At<'d, P, Member = Self>,
    {
        let (member, proof) = path.split();
        let this = member.project();
        let Some(entry) = this.inner.vacant() else {
            return Err(Error::Capacity {
                requested: num::NonZero::<usize>::MIN,
                available: 0,
            });
        };
        let global = Path::<R, P>::index(proof, entry.index());
        let (binding, prepared) = queue.try_prepare(global, (), parent)?;
        Ok(move |(fiber, target)| {
            let occupied = entry.insert(Entry::new(fiber, target, binding, global));
            let fields = occupied.into_ref().project_ref();
            queue::Flat::admit(prepared, fields.ready);
        })
    }

    fn drive_index<'turn, 'queue, S>(
        entry: selected::Entry<'_, Self>,
        dequeued: S::Dequeued<'queue, Global>,
        work: app::Work<'turn, 'd>,
        permit: step::Delegated<'_, 'turn, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome<T, F::Output>
    where
        S: queue::Backend<'d> + 'queue,
    {
        let mut occupied = Occupied::selected(entry);
        let mut entry = occupied.as_mut();
        let fields = entry.as_mut().project();
        let target = *fields.target;
        let (parent, waker) = fields.binding.poll();
        let parent = wake::RootWaker::from(parent);
        let waker = wake::Waker::from_parent(waker);
        let (result, wake) =
            context::Context::with_waker(waker, parent, work, driver.reborrow(), |mut cx| {
                let result =
                    context::PollCall::delegated(fields.fiber, cx.as_mut(), permit, F::poll);
                let wake = cx.as_ref().get_ref().take_requeue();
                (result, wake)
            });
        match (result, wake) {
            (Poll::Pending, context::Requeue::Requested) => {
                S::commit(dequeued, fields.ready.into_ref(), queue::Disposition::Ready);
                Outcome::PendingReady { target }
            }
            (Poll::Pending, context::Requeue::Idle) => {
                S::commit(dequeued, fields.ready.into_ref(), queue::Disposition::Idle);
                Outcome::Pending { target }
            }
            (Poll::Ready(output), context::Requeue::Idle | context::Requeue::Requested) => {
                S::commit(
                    dequeued,
                    fields.ready.into_ref(),
                    queue::Disposition::Complete,
                );
                occupied.discard();
                Outcome::Complete { target, output }
            }
        }
    }

    fn remove<'a, S, R, P>(
        path: Path<'a, 'd, R, P>,
        id: Id<'d, D>,
        queue: pin::Pin<&S>,
    ) -> local::Removal
    where
        S: queue::Backend<'d>,
        R: tree::At<'d, P, Member = Self>,
    {
        let (member, proof) = path.split();
        let Some(key) = id.key_for(Owner::of(member.as_ref().get_ref())) else {
            return local::Removal::Absent;
        };
        let this = member.project();
        let removed = this.inner.remove_with(key.parts(), |index, mut entry| {
            let fields = entry.as_mut().project();
            let global = Path::<R, P>::index(proof, index);
            queue::Backend::detach(queue, fields.ready.as_ref(), global);
        });
        if removed.is_some() {
            local::Removal::Removed
        } else {
            local::Removal::Absent
        }
    }
}

impl<'d, F, const N: usize, D, T, const MAX: u32> tree::Build<'d>
    for tree::Leaf<Pool<'d, F, N, D, T, MAX>>
where
    F: abi::Fiber<'d>,
    T: Copy,
{
    fn try_build() -> Result<Self, alloc::Error> {
        Ok(Self::new(Pool::try_new()?))
    }
}

impl<'d, F, const N: usize, D, T, const MAX: u32> schedule::PlaneTree<'d>
    for tree::Leaf<Pool<'d, F, N, D, T, MAX>>
where
    F: abi::Fiber<'d>,
    T: Copy,
{
    fn wake_global(access: Access<'_, Self>) {
        let entry = Occupied::selected(access.entry()).get_mut();
        wake::RootWaker::from(entry.as_ref().project_ref().binding.parent()).wake();
    }

    fn merge_ready(access: Access<'_, Self>, queue: pin::Pin<&queue::Flat<'d>>) {
        let entry = Occupied::selected(access.entry()).get_mut();
        let fields = entry.into_ref().project_ref();
        queue.queue().merge(fields.ready);
        wake::RootWaker::from(fields.binding.parent()).wake();
    }

    fn restore<'queue, S>(access: Access<'_, Self>, dequeued: S::Dequeued<'queue, Global>)
    where
        S: queue::Backend<'d> + 'queue,
    {
        let entry = Occupied::selected(access.entry()).get_mut();
        let fields = entry.into_ref().project_ref();
        S::commit(dequeued, fields.ready, queue::Disposition::Ready);
        wake::RootWaker::from(fields.binding.parent()).wake();
    }
}

impl<'d, F: abi::Fiber<'d>, const N: usize, D, T: Copy, const MAX: u32> Proof
    for Pool<'d, F, N, D, T, MAX>
{
}

impl<'d, F, const N: usize, D, T, const MAX: u32> tree::Member<'d> for Pool<'d, F, N, D, T, MAX>
where
    F: abi::Fiber<'d>,
    T: Copy,
{
    type Fiber = F;
    type Domain = D;
    type Target = T;

    const CAPACITY: tree::Capacity = tree::Capacity::new(caps::Tasks::<N>::new().nonzero());
}
