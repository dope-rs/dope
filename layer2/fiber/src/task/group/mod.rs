use core::{mem, pin, task::Poll};
use std::convert;

use o3::depot::{array, array::bits};

use crate::{
    abi, context,
    context::wake,
    core::driver::schedule::{app, ready::task},
    task::{
        group::ready::Ready,
        storage::{
            caps, slots,
            slots::{Action, Bindings, Index, Phase, Table},
        },
    },
};

mod ready;

enum DomainTag {}

/// Failure to reserve the exact driver task domain for a group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Error(task::Error);

/// Result of one stable ready-batch drive, including any partial completion.
#[derive(Clone, Copy, PartialEq, Eq)]
#[must_use = "a group drive failure must be handled"]
pub enum Drive<const N: usize> {
    Complete(array::Len<N>),
    Failed {
        completed: array::Len<N>,
        error: Error,
    },
}

/// Inline streaming group for 1 through 16 homogeneous fibers.
/// Slots, bindings, readiness, and membership stay resident; wakes produced
/// while polling enter the next stable batch.
#[pin_project::pin_project(PinnedDrop)]
pub struct Group<'d, F, const N: usize> {
    #[pin]
    table: Table<'d, F, (), N>,
    #[pin]
    ready: Ready<'d, N>,
    domain: task::Domain<'d, DomainTag, N>,
    width: caps::Inline<N>,
}

/// Exclusive proof that one group slot remains vacant.
///
/// ```compile_fail
/// use dope_fiber::{abi::Ready, task::group::Vacant};
///
/// fn escape<'a, 'd>(vacant: Vacant<'a, 'd, Ready<()>, 1>)
///     -> Vacant<'static, 'd, Ready<()>, 1>
/// {
///     vacant
/// }
/// ```
#[must_use = "a vacancy must be filled before the fiber can run"]
#[repr(C)]
pub struct Vacant<'group, 'd, F, const N: usize> {
    group: pin::Pin<&'group mut Group<'d, F, N>>,
    slot: ready::Vacancy<N>,
}

impl Error {
    /// Returns the exact driver-domain failure without conversion or erasure.
    pub const fn domain(&self) -> &task::Error {
        &self.0
    }
}

impl<'d, F, const N: usize> Group<'d, F, N> {
    /// Reserves the exact driver task domain without allocating group storage.
    pub fn try_new(parent: wake::RootWaker<'d>) -> Result<Self, Error> {
        Self::try_table(parent, Table::vacant, bits::Set::EMPTY)
    }

    /// Reserves the task domain and schedules every input in array order.
    pub fn try_with(parent: wake::RootWaker<'d>, fibers: [F; N]) -> Result<Self, Error> {
        let group = Self::try_table(
            parent,
            || Table::from_map(fibers, convert::identity),
            bits::Set::EMPTY.complement(),
        )?;
        group.domain.wake_parent();
        Ok(group)
    }

    fn try_table(
        parent: wake::RootWaker<'d>,
        table: impl FnOnce() -> Table<'d, F, (), N>,
        live: bits::Set<Index<N>, N>,
    ) -> Result<Self, Error> {
        let width = caps::Inline::<N>::new();
        let parent: wake::Waker<'d> = parent.into();
        let ready = Ready::try_new(parent.0, live).map_err(Error)?;
        let domain = task::Domain::try_new(parent.0).map_err(Error)?;
        Ok(Self {
            table: table(),
            ready,
            domain,
            width,
        })
    }

    pub fn len(&self) -> array::Len<N> {
        self.ready.len()
    }

    /// Whether an inserted or woken member is ready for a stable batch.
    pub fn is_ready(&self) -> bool {
        self.ready.is_ready()
    }

    /// Borrows the group exclusively only when one inline slot is available.
    pub fn vacant(self: pin::Pin<&mut Self>) -> Option<Vacant<'_, 'd, F, N>> {
        let slot = self.ready.vacant()?;
        Some(Vacant { group: self, slot })
    }
}

impl<'d, F, const N: usize> Group<'d, F, N>
where
    F: abi::Fiber<'d>,
{
    /// Polls the current stable batch using one additional credit per member.
    /// Completed members are unbound, removed, and dropped before `complete`.
    pub fn drive_ready(
        self: pin::Pin<&mut Self>,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
        complete: impl FnMut(F::Output),
    ) -> Drive<N> {
        self.drive(context, false, complete)
    }

    /// Transfers this admission to the first selected member. Later members
    /// consume additional credits; the returned context retains no admission.
    pub fn drive_admit<'call, 'turn>(
        self: pin::Pin<&mut Self>,
        admit: context::Admission<'call, 'turn, 'd>,
        complete: impl FnMut(F::Output),
    ) -> (Drive<N>, pin::Pin<&'call mut context::Context<'turn, 'd>>)
    where
        'd: 'turn,
    {
        let mut context = admit.into_context();
        let result = self.drive(context.as_mut(), true, complete);
        (result, context)
    }

    fn drive(
        self: pin::Pin<&mut Self>,
        mut context: pin::Pin<&mut context::Context<'_, 'd>>,
        mut initial: bool,
        mut complete: impl FnMut(F::Output),
    ) -> Drive<N> {
        let mut this = self.project();
        let ready = this.ready.as_ref();
        let mut drain = ready.get_ref().begin();

        let mut done = bits::Set::<Index<N>, N>::EMPTY;
        loop {
            let (member, permit) = if initial {
                let Some(member) = drain.pop() else {
                    break;
                };
                initial = false;
                (member, None)
            } else {
                match context.as_ref().work().next(&mut drain) {
                    app::Next::Item(member, permit) => (member, Some(permit)),
                    app::Next::Empty => break,
                    app::Next::Exhausted(_) => {
                        context.requeue();
                        return Drive::Complete(done.len());
                    }
                }
            };
            let index = member.index();
            let attach = ready.attach(index);

            let output = match this.table.as_mut().visit(
                index,
                member,
                |fiber, bindings: pin::Pin<&Bindings<'d, N>>, member| {
                    let poll = bindings.poll_domain(attach, this.domain, |wake| {
                        // SAFETY: this live table slot is vacated on Ready.
                        // Only drive_admit supplies the first existing credit;
                        // every subsequent child carries its own permit.
                        let live = unsafe { context::raw::Live::new(fiber) };
                        match permit {
                            Some(permit) => live.child_with(context.as_mut(), wake, permit),
                            None => unsafe { live.child(context.as_mut(), wake) },
                        }
                    });
                    match poll {
                        Err(error) => {
                            member.retry();
                            Action::Keep(Err(Error(error)))
                        }
                        Ok(Poll::Pending) => Action::Keep(Ok(None)),
                        Ok(Poll::Ready((output, claim))) => {
                            match claim {
                                task::Reclaim::Reusable => member.release(),
                                task::Reclaim::Retired => member.retire(),
                            };
                            Action::Vacate(Ok(Some(output)))
                        }
                    }
                },
            ) {
                Phase::Live(Ok(output)) => output,
                Phase::Live(Err(error)) => {
                    return Drive::Failed {
                        completed: done.len(),
                        error,
                    };
                }
                Phase::Vacant(member) | Phase::Done(member) => {
                    member.release();
                    None
                }
            };
            let Some(output) = output else {
                continue;
            };
            done.insert(index);
            complete(output);
        }

        if ready.is_ready() {
            context.requeue();
        }
        Drive::Complete(done.len())
    }
}

impl<'group, 'd, F, const N: usize> Vacant<'group, 'd, F, N> {
    /// Fills the proven task-domain vacancy and schedules its initial poll.
    pub fn insert(mut self, fiber: F) {
        let mut this = self.group.as_mut().project();
        // SAFETY: this consumed vacancy retains the exact group's exclusive
        // pin. Its ready selection excludes live and retired members. Both
        // constructors initialize the table and membership together; completion
        // vacates an entry before its reusable membership can be observed.
        // No independent ready/table pair can enter this insertion path.
        let vacant = unsafe { slots::raw::Vacant::pinned(this.table.as_mut(), self.slot.index()) };
        vacant.insert(fiber);
        this.ready.as_ref().fill(self.slot);
        this.domain.wake_parent();
    }
}

#[pin_project::pinned_drop]
impl<'d, F, const N: usize> PinnedDrop for Group<'d, F, N> {
    fn drop(self: pin::Pin<&mut Self>) {
        let mut this = self.project();
        for index in Index::<N>::until(array::Len::MAX) {
            this.table.as_mut().cancel(index, |bindings| {
                let _ = bindings.reclaim(index, this.domain);
            });
        }
    }
}

const _: () = assert!(mem::size_of::<Drive<16>>() == 4 * mem::size_of::<usize>());
