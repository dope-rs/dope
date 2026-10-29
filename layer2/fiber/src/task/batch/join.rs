use core::{convert, pin, task::Poll};

use o3::depot::array;

use crate::{
    abi,
    abi::machine,
    context,
    context::wake,
    core::driver::schedule::{
        app,
        ready::{
            task,
            task::{pool, topology},
        },
    },
    task::{
        batch::{Completed, Error, Index, Pool, RootError, Tag, state},
        storage::slots,
    },
};

const LIMIT: usize = 32;

enum Progress<const N: usize> {
    Done(Completed<N>),
    More,
    Parked,
}

/// One ordered, fixed-capacity group of homogeneous fibers.
#[pin_project::pin_project(PinnedDrop)]
#[must_use = "a fiber does nothing unless it is driven"]
pub struct Join<'pool, 'd, F, const N: usize>
where
    F: abi::Fiber<'d>,
{
    #[pin]
    table: slots::Table<'d, F, F::Output, N>,
    pool: &'pool Pool<'d, N>,
    state: state::State<N>,
    live: machine::Machine<()>,
    root: Option<pool::Root<'d>>,
}

/// Exclusive proof of the next empty entry in one batch.
#[must_use = "a batch vacancy must be filled or abandoned"]
struct Vacant<'batch, F, O, const N: usize> {
    entry: slots::raw::Vacant<'batch, F, O>,
    state: state::Push<'batch, N>,
}

impl<F, O, const N: usize> Vacant<'_, F, O, N> {
    fn insert(self, fiber: F) {
        self.entry.insert(fiber);
        self.state.commit();
    }
}

impl<'pool, 'd, F, const N: usize> Join<'pool, 'd, F, N>
where
    F: abi::Fiber<'d>,
{
    /// Builds an empty fixed-capacity batch.
    pub fn new(pool: &'pool mut Pool<'d, N>) -> Self {
        Self::from_table(pool, slots::Table::vacant(), array::Len::ZERO)
    }

    /// Builds a batch containing every fiber in array order.
    pub fn from_array(pool: &'pool mut Pool<'d, N>, fibers: [F; N]) -> Self {
        Self::from_map(pool, fibers, convert::identity)
    }

    /// Consumes each input in array order, constructing every fiber before returning.
    pub fn from_map<P>(
        pool: &'pool mut Pool<'d, N>,
        input: [P; N],
        make: impl FnMut(P) -> F,
    ) -> Self {
        Self::from_table(pool, slots::Table::from_map(input, make), array::Len::MAX)
    }

    /// Builds a full batch without materializing an intermediate fiber array.
    pub fn from_fn(pool: &'pool mut Pool<'d, N>, make: impl FnMut(Index<N>) -> F) -> Self {
        Self::from_table(pool, slots::Table::from_fn(make), array::Len::MAX)
    }

    fn from_table(
        pool: &'pool mut Pool<'d, N>,
        table: slots::Table<'d, F, F::Output, N>,
        len: array::Len<N>,
    ) -> Self {
        Self {
            table,
            pool,
            state: state::State::new(len),
            live: machine::Machine::new(()),
            root: None,
        }
    }

    /// Borrows the next entry while the batch is still being built.
    fn vacant(&mut self) -> Option<Vacant<'_, F, F::Output, N>> {
        let state = self.state.vacancy()?;
        let index = slots::Index::from_vacant(state.slot());
        // SAFETY: only this owner pairs its table and admitted prefix. Empty
        // construction initializes every entry as vacant; full construction has
        // no vacancy. Each insertion fills exactly the next entry and commits
        // that same prefix. This exclusive borrow cannot overlap a pinned poll,
        // cancellation, or another insertion.
        let entry = unsafe { slots::raw::Vacant::new(&mut self.table, index) };
        Some(Vacant { entry, state })
    }

    pub fn try_push(&mut self, fiber: F) -> Result<(), F> {
        let Some(vacant) = self.vacant() else {
            return Err(fiber);
        };
        vacant.insert(fiber);
        Ok(())
    }

    fn progress(
        self: pin::Pin<&mut Self>,
        mut context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Result<Progress<N>, Error> {
        let mut this = self.project();
        if let Some(done) = this.state.completed() {
            return Ok(
                match this
                    .live
                    .turn(|()| machine::Flow::Suspend(Poll::Ready(done)))
                {
                    Poll::Ready(done) => Progress::Done(done),
                    Poll::Pending => Progress::Parked,
                },
            );
        }

        let parent: wake::Waker<'d> = context.as_ref().root_waker().into();
        let pool = *this.pool;
        let root = match pool.tasks.try_root(parent.0) {
            Ok(root) => root,
            Err(pool::RootError::Foreign) => {
                return Err(Error::Root(RootError::Foreign));
            }
            Err(pool::RootError::Invalid(error)) => {
                return Err(Error::Root(RootError::Invalid(error)));
            }
        };
        match *this.root {
            None => *this.root = Some(root),
            Some(active) if active == root => {}
            Some(active) => {
                return Err(Error::Root(RootError::Changed {
                    active: active.id(),
                    observed: root.id(),
                }));
            }
        }

        let mut budget = LIMIT;
        let mut initial = true;
        while budget != 0 {
            let Some(fresh) = this.state.fresh() else {
                break;
            };
            let index = slots::Index::from_vacant(fresh.slot());
            let permit = if initial {
                initial = false;
                None
            } else {
                match context.as_ref().try_continue() {
                    context::Continue::Permit(permit) => Some(permit),
                    context::Continue::Exhausted => return Ok(Progress::More),
                }
            };
            let attach = pool.ready.attach(index);
            let done = poll_slot(
                this.table.as_mut(),
                context.as_mut(),
                index,
                attach,
                permit,
                &pool.tasks,
                root,
            )
            .map_err(Error::Task)?;
            budget -= 1;
            if let state::Status::Done(all) = fresh.commit(done) {
                return Ok(Progress::Done(all));
            }
        }

        if budget == 0 {
            return Ok(if this.state.has_fresh() || !pool.ready.is_empty() {
                Progress::More
            } else {
                Progress::Parked
            });
        }

        let Some(mut drain) = pool.ready.drain() else {
            return Ok(Progress::Parked);
        };
        loop {
            let pending = match this.state.pending() {
                state::PendingState::Live(pending) => pending,
                state::PendingState::Done(done) => return Ok(Progress::Done(done)),
            };
            let (index, permit) = if initial {
                let Some(index) = drain.pop() else {
                    return Ok(Progress::Parked);
                };
                initial = false;
                (index, None)
            } else {
                match context.as_ref().work().next(&mut drain) {
                    app::Next::Item(index, permit) => (index, Some(permit)),
                    app::Next::Empty => return Ok(Progress::Parked),
                    app::Next::Exhausted(_) => return Ok(Progress::More),
                }
            };
            let attach = pool.ready.attach(index);
            budget -= 1;
            let done = poll_slot(
                this.table.as_mut(),
                context.as_mut(),
                index,
                attach,
                permit,
                &pool.tasks,
                root,
            )
            .map_err(Error::Task)?;
            if let state::Status::Done(all) = pending.commit(done) {
                return Ok(Progress::Done(all));
            }
            if budget == 0 {
                return Ok(if drain.peek().is_some() {
                    Progress::More
                } else {
                    Progress::Parked
                });
            }
        }
    }

    fn cancel(self: pin::Pin<&mut Self>) {
        let mut this = self.project();
        if let Some(root) = *this.root {
            let pool = *this.pool;
            for index in slots::Index::until(this.state.next()) {
                this.table.as_mut().cancel(index, |bindings| {
                    bindings.reclaim_pool(index, &pool.tasks, root);
                });
            }
        }
        this.state.reset();
    }
}

fn poll_slot<'turn, 'd: 'turn, F, const N: usize>(
    mut table: pin::Pin<&mut slots::Table<'d, F, F::Output, N>>,
    mut context: pin::Pin<&mut context::Context<'turn, 'd>>,
    index: slots::Index<N>,
    edge: topology::Edge<'_, 'd, slots::Index<N>>,
    permit: Option<app::Permit<'turn, 'd>>,
    pool: &pool::Pool<'d, Tag, N>,
    root: pool::Root<'d>,
) -> Result<state::Completion, task::Error>
where
    F: abi::Fiber<'d>,
{
    match table.as_mut().visit(index, (), |fiber, bindings, ()| {
        let poll = bindings.poll_pool(edge, pool, root, |wake| {
            match permit {
                Some(permit) => unsafe {
                    context::raw::Live::new(fiber).child_with(context.as_mut(), wake, permit)
                },
                None => {
                    // SAFETY: progress consumes its original PollCall admission
                    // for exactly the first live child; later children carry permits.
                    // Table::visit ends this child's live state on Ready.
                    unsafe { context::raw::Live::new(fiber).child(context.as_mut(), wake) }
                }
            }
        });
        let poll = match poll {
            Ok(poll) => poll,
            Err(error) => return slots::Action::Keep(Err(error)),
        };
        let Poll::Ready(output) = poll else {
            return slots::Action::Keep(Ok(state::Completion::Pending));
        };
        slots::Action::Complete(output, Ok(state::Completion::Ready))
    }) {
        slots::Phase::Live(result) => result,
        slots::Phase::Vacant(()) | slots::Phase::Done(()) => Ok(state::Completion::Pending),
    }
}

impl<'pool, 'd, F, const N: usize> abi::Fiber<'d> for Join<'pool, 'd, F, N>
where
    F: abi::Fiber<'d>,
{
    type Output = Result<array::IntoIter<F::Output, N>, Error>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (mut this, mut context) = call.into_parts();
        match this.as_mut().progress(context.as_mut()) {
            Ok(Progress::Done(done)) => {
                let output = this.as_mut().project().table.drain_exact(done);
                let _ = this.as_mut().project().live.take();
                Poll::Ready(Ok(output))
            }
            Ok(Progress::Parked) => Poll::Pending,
            Ok(Progress::More) => {
                context.requeue();
                Poll::Pending
            }
            Err(error) => {
                this.as_mut().cancel();
                let _ = this.as_mut().project().live.take();
                Poll::Ready(Err(error))
            }
        }
    }
}

#[pin_project::pinned_drop]
impl<'d, F, const N: usize> PinnedDrop for Join<'_, 'd, F, N>
where
    F: abi::Fiber<'d>,
{
    fn drop(mut self: pin::Pin<&mut Self>) {
        if self.as_mut().project().live.take().is_some() {
            self.cancel();
        }
    }
}

const _: () = {
    use core::mem;

    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<machine::Machine<()>>() == 1);
    assert!(mem::size_of::<Join<'static, 'static, abi::Ready<()>, 0>>() == 4 * word);
    assert!(mem::align_of::<Join<'static, 'static, abi::Ready<()>, 0>>() == word);
    assert!(mem::size_of::<Option<Join<'static, 'static, abi::Ready<()>, 0>>>() == 4 * word);
};
