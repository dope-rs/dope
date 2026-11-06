use std::{marker, num, ops::ControlFlow, pin, task::Poll};

use o3::{
    alloc,
    base::num::Epoch,
    depot::{self, arena::slab::external, capacity, index::Initialized},
};
use schedule::{
    Id,
    ready::{Global, Path, linked},
    result::Outcome,
    selected,
    selected::{Access, EntryRef},
};

use crate::{
    abi, context,
    context::wake,
    core::driver::{
        self,
        schedule::{
            app,
            app::step,
            ready::{task, task::Error},
        },
    },
    task::storage::{
        Owner, caps,
        fixed::{local, schedule, schedule::queue, tree},
    },
};

mod poll;
mod proof;

pub(crate) use proof::Proof;

pub type Pool<'d, F, const N: usize, D, T = ()> = Storage<'d, F, N, D, T, ()>;

/// Scheduled stream retaining its slot until EOF or explicit removal.
pub type Stream<'d, F, const N: usize, D, T = ()> = Storage<'d, F, N, D, T, poll::Stream>;

#[pin_project::pin_project]
struct Entry<'d, F, T: Copy, M> {
    #[pin]
    fiber: F,
    binding: task::raw::Root<'d>,
    #[pin]
    ready: linked::Entry,
    target: T,
    mode: M,
}

impl<'d, F, T: Copy, M: poll::Mode<'d, F>> Entry<'d, F, T, M> {
    const fn new(fiber: F, target: T, binding: task::raw::Root<'d>, index: Global) -> Self {
        Self {
            fiber,
            binding,
            ready: linked::Entry::new(index),
            target,
            mode: M::INITIAL,
        }
    }
}

#[pin_project::pin_project(!Unpin)]
pub struct Storage<'d, F, const N: usize, D, T: Copy, M> {
    #[pin]
    inner: external::Pinned<Entry<'d, F, T, M>, Epoch>,
    base: u32,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
    domain: marker::PhantomData<fn(D) -> D>,
}

impl<'d, F, const N: usize, D, T, M> Storage<'d, F, N, D, T, M>
where
    M: poll::Mode<'d, F>,
    T: Copy,
{
    pub fn try_new() -> Result<Self, alloc::Error> {
        Self::try_new_with_capacity(capacity::Count::MAX)
    }

    fn try_new_with_capacity(physical: capacity::Count) -> Result<Self, alloc::Error> {
        let count = physical.min(capacity::Count::new(
            caps::Tasks::<N>::new().nonzero().get(),
        ));
        Ok(Self {
            inner: external::Pinned::try_new(count)?,
            base: 0,
            driver: marker::PhantomData,
            domain: marker::PhantomData,
        })
    }

    fn try_prepare<'a, S>(
        member: pin::Pin<&'a mut Self>,
        queue: pin::Pin<&'a S>,
        input: S::Input,
        parent: wake::RootWaker<'d>,
    ) -> Result<impl FnOnce((F, T)) -> external::Key<Epoch>, S::Error>
    where
        S: queue::Backend<'d>,
    {
        let this = member.project();
        let Ok(entry) = this.inner.try_grant() else {
            return Err(S::Error::from(Error::Capacity {
                requested: num::NonZero::<usize>::MIN,
                available: 0,
            }));
        };
        // SAFETY: layout bounds this extent within the original u32 task domain;
        // the native reservation proves index below its physical capacity.
        let position = unsafe { num::NonZero::new_unchecked(*this.base + entry.index() + 1) };
        let global = Global::new(position);
        let (binding, prepared) = S::try_prepare(queue, global, input, parent)?;
        Ok(move |(fiber, target)| {
            let (key, occupied) = entry.occupy(Entry::new(fiber, target, binding, global));
            let fields = occupied.into_ref().project_ref();
            S::admit(prepared, fields.ready);
            key
        })
    }

    fn selected<'a>(entry: selected::EntryRef<'a, Self>) -> &'a Entry<'d, F, T, M> {
        let (member, index) = entry.into_parts();
        let index = index.local(member.base);
        // SAFETY: selection retains this exact pool's initialized entry and
        // shared plane loan, excluding removal or reuse until the borrow ends.
        unsafe { depot::raw::Proof::<_, Initialized<()>>::new((&member.inner, index)) }.into()
    }

    fn selected_mut<'a>(
        entry: selected::Entry<'a, Self>,
    ) -> external::Anchored<'a, Entry<'d, F, T, M>, Epoch> {
        let (member, index) = entry.into_parts();
        let index = index.local(member.as_ref().get_ref().base);
        let owner = member.project().inner;
        // SAFETY: selection retains this pool's live entry and its exclusive
        // pinned plane loan through the occupied projection.
        unsafe { depot::raw::Proof::<_, Initialized<()>>::new((owner, index)) }.into()
    }
}

impl<'d, F, const N: usize, D, T, M> schedule::Ops<'d> for Storage<'d, F, N, D, T, M>
where
    M: poll::Mode<'d, F>,
    T: Copy,
{
    type Output = M::Output;
    type Item = M::Item;

    fn target_index(entry: EntryRef<'_, Self>) -> T {
        Self::selected(entry).target
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
        let member = path.split();
        let owner = Owner::of(member.as_ref().get_ref());
        let insert = Self::try_prepare(member, queue, input, parent)?;
        Ok(move |input| Id::new(insert(input), owner))
    }

    fn try_spawn<'a, R, P>(
        path: Path<'a, 'd, R, P>,
        queue: pin::Pin<&'a queue::Flat<'d>>,
        parent: wake::RootWaker<'d>,
    ) -> Result<impl FnOnce((F, T)) + 'a, Error>
    where
        R: tree::At<'d, P, Member = Self> + 'a,
        P: 'a,
    {
        let insert = Self::try_prepare(path.split(), queue, (), parent)?;
        Ok(move |input| {
            let _ = insert(input);
        })
    }

    fn drive_index<'turn, 'queue, S>(
        entry: selected::Entry<'_, Self>,
        dequeued: S::Dequeued<'queue, Global>,
        work: app::Work<'turn, 'd>,
        permit: step::Delegated<'_, 'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Outcome<T, M::Output, M::Item>
    where
        S: queue::Backend<'d> + 'queue,
    {
        let mut occupied = Self::selected_mut(entry);
        let mut entry = occupied.value();
        let fields = entry.as_mut().project();
        let target = *fields.target;
        let (parent, waker) = fields.binding.poll();
        let parent = wake::RootWaker::from(parent);
        let waker = wake::Waker::from_parent(waker);
        let (result, wake) =
            context::Context::with_waker(waker, parent, work, driver.reborrow(), |mut cx| {
                let result = fields.mode.poll(context::PollCall::delegated(
                    fields.fiber,
                    cx.as_mut(),
                    permit,
                ));
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
            (Poll::Ready(ControlFlow::Continue(output)), _) => {
                S::commit(dequeued, fields.ready.into_ref(), queue::Disposition::Idle);
                Outcome::Yielded { target, output }
            }
            (Poll::Ready(ControlFlow::Break(output)), _) => {
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
        let member = path.split();
        let Some(key) = id.raw_for(Owner::of(member.as_ref().get_ref())) else {
            return local::Removal::Absent;
        };
        let this = member.project();
        let Ok(mut occupied) = this.inner.try_occupied(key) else {
            return local::Removal::Absent;
        };
        let mut entry = occupied.value();
        queue::Backend::detach(queue, entry.as_mut().project().ready.as_ref());
        occupied.discard();
        local::Removal::Removed
    }
}

impl<'d, F, const N: usize, D, T, M> tree::Build<'d> for tree::Leaf<Storage<'d, F, N, D, T, M>>
where
    M: poll::Mode<'d, F>,
    T: Copy,
{
    fn try_build_with_capacity(physical: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self::new(Storage::try_new_with_capacity(physical)?))
    }
}

impl<'d, F, const N: usize, D, T: Copy> Stream<'d, F, N, D, T> {
    fn held_entry(
        self: pin::Pin<&mut Self>,
        id: &Id<'d, D>,
    ) -> Result<pin::Pin<&mut Entry<'d, F, T, poll::Stream>>, schedule::result::Error> {
        let key = id
            .raw_for(Owner::of(self.as_ref().get_ref()))
            .ok_or(schedule::result::Error::Absent)?;
        let entry = self
            .project()
            .inner
            .try_occupied(key)
            .map_err(|_| schedule::result::Error::Absent)?
            .into_value();
        if !entry.as_ref().project_ref().mode.held {
            return Err(schedule::result::Error::Active);
        }
        Ok(entry)
    }
}

impl<'d, F, const N: usize, D, T> schedule::StreamOps<'d> for Stream<'d, F, N, D, T>
where
    F: abi::Stream<'d>,
    T: Copy,
{
    fn get_mut(
        self: pin::Pin<&mut Self>,
        id: &Id<'d, D>,
    ) -> Result<pin::Pin<&mut F>, schedule::result::Error> {
        Ok(self.held_entry(id)?.project().fiber)
    }

    fn resume(self: pin::Pin<&mut Self>, id: &Id<'d, D>) -> Result<(), schedule::result::Error> {
        let fields = self.held_entry(id)?.project();
        fields.mode.held = false;
        wake::Waker::from_parent(fields.binding.poll().1).wake();
        Ok(())
    }
}

impl<'d, F, const N: usize, D, T, M> schedule::PlaneTree<'d>
    for tree::Leaf<Storage<'d, F, N, D, T, M>>
where
    M: poll::Mode<'d, F>,
    T: Copy,
{
    fn layout(&mut self, base: u32) -> capacity::Count {
        let storage = self.storage_mut();
        storage.base = base;
        storage.inner.capacity()
    }

    fn first_base(&self) -> u32 {
        <Self as tree::At<'d, tree::Here>>::project_ref(self).base
    }

    fn root_waker(access: Access<'_, Self>) -> wake::RootWaker<'d> {
        let entry = Storage::selected_mut(access.entry()).into_value();
        wake::RootWaker::from(entry.as_ref().project_ref().binding.parent())
    }

    fn merge_ready(access: Access<'_, Self>, queue: pin::Pin<&queue::Flat<'d>>) {
        let entry = Storage::selected_mut(access.entry()).into_value();
        let fields = entry.into_ref().project_ref();
        queue.queue().merge(fields.ready);
        wake::RootWaker::from(fields.binding.parent()).wake();
    }

    fn restore<'queue, S>(access: Access<'_, Self>, dequeued: S::Dequeued<'queue, Global>)
    where
        S: queue::Backend<'d> + 'queue,
    {
        let entry = Storage::selected_mut(access.entry()).into_value();
        let fields = entry.into_ref().project_ref();
        S::commit(dequeued, fields.ready, queue::Disposition::Ready);
        wake::RootWaker::from(fields.binding.parent()).wake();
    }
}

impl<'d, F, const N: usize, D, T: Copy, M> Proof for Storage<'d, F, N, D, T, M> {
    fn base(&self) -> u32 {
        self.base
    }
}

impl<'d, F, const N: usize, D, T, M> tree::Member<'d> for Storage<'d, F, N, D, T, M>
where
    M: poll::Mode<'d, F>,
    T: Copy,
{
    type Fiber = F;
    type Domain = D;
    type Target = T;

    const CAPACITY: tree::Capacity = tree::Capacity::new(caps::Tasks::<N>::new().nonzero());
}
