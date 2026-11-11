use core::{mem, mem::MaybeUninit, pin, task::Poll};

use o3::{
    depot::{array, array::bits},
    mem::slot,
};

use crate::{
    abi, context,
    context::wake,
    core::driver::schedule::{app, ready::task},
    task::{
        group::ready::Ready,
        storage::slots::{Bindings, Index},
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
    slots: [MaybeUninit<F>; N],
    #[pin]
    bindings: Bindings<'d, N>,
    #[pin]
    ready: Ready<'d, N>,
    domain: task::Domain<'d, DomainTag, N>,
}

/// Exclusive proof that one group slot remains vacant.
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
    pub fn try_new<'a>(
        mut target: slot::Vacant<'a, Self>,
        parent: wake::RootWaker<'d>,
    ) -> Result<slot::Occupied<'a, Self>, Error> {
        let (ready, domain) = Self::admit(parent, bits::Set::EMPTY)?;
        let pointer = target.get_mut().as_mut_ptr();
        // SAFETY: this vacant slot is exclusive; slots contains only MaybeUninit.
        // Every other field is initialized before the occupied owner is issued.
        unsafe {
            (&raw mut (*pointer).bindings).write(Bindings::new());
            (&raw mut (*pointer).ready).write(ready);
            (&raw mut (*pointer).domain).write(domain);
            Ok(target.assume_init())
        }
    }

    /// Reserves the task domain and schedules every input in array order.
    pub fn try_with(parent: wake::RootWaker<'d>, fibers: [F; N]) -> Result<Self, Error> {
        let (ready, domain) = Self::admit(parent, bits::Set::EMPTY.complement())?;
        domain.wake_parent();
        Ok(Self {
            slots: fibers.map(MaybeUninit::new),
            bindings: Bindings::new(),
            ready,
            domain,
        })
    }

    fn admit(
        parent: wake::RootWaker<'d>,
        live: bits::Set<Index<N>, N>,
    ) -> Result<(Ready<'d, N>, task::Domain<'d, DomainTag, N>), Error> {
        const { assert!(N > 0 && N <= 16) };
        let parent: wake::Waker<'d> = parent.into();
        let ready = Ready::try_new(parent.0, live).map_err(Error)?;
        let domain = task::Domain::try_new(parent.0).map_err(Error)?;
        Ok((ready, domain))
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
                match context.as_ref().work().next(&mut drain, &mut ()) {
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

            let mut slot = index.project(this.slots.as_mut());
            let node = this.bindings.as_ref().at(index);
            let bound = match task::raw::Binding::get(node) {
                Some(bound) => bound,
                None => {
                    let admit = match this.domain.try_admit() {
                        Ok(admit) => admit,
                        Err(error) => {
                            member.retry();
                            return Drive::Failed {
                                completed: done.len(),
                                error: Error(error),
                            };
                        }
                    };
                    // SAFETY: this vacant pinned node and edge belong to this domain.
                    unsafe { task::raw::Binding::bind_leased(node, admit, attach) }
                }
            };
            let wake = wake::Waker::from_parent(bound.waker());
            // SAFETY: this member selects this group's initialized pinned fiber.
            // Only the initial admission can omit a new child credit.
            let poll = unsafe {
                let fiber = slot
                    .as_mut()
                    .map_unchecked_mut(|slot| slot.assume_init_mut());
                let live = context::raw::Live::new(fiber);
                match permit {
                    Some(permit) => live.child_with(context.as_mut(), wake, permit),
                    None => live.child(context.as_mut(), wake),
                }
            };
            let Poll::Ready(output) = poll else {
                continue;
            };
            // SAFETY: this node was admitted by this group's exact domain.
            let claim = unsafe { bound.reclaim(this.domain) };
            member.finish(claim);
            // SAFETY: reclaim detached this exact live slot before its sole drop.
            unsafe { slot.get_unchecked_mut().assume_init_drop() };
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
    pub fn insert(self, fiber: F) {
        let this = self.group.project();
        let slot = self.slot.index().project(this.slots);
        // SAFETY: this group's vacancy excludes initialized and retired slots.
        unsafe { slot.get_unchecked_mut() }.write(fiber);
        this.ready.as_ref().fill(self.slot);
        this.domain.wake_parent();
    }
}

#[pin_project::pinned_drop]
impl<'d, F, const N: usize> PinnedDrop for Group<'d, F, N> {
    fn drop(self: pin::Pin<&mut Self>) {
        let mut this = self.project();
        let mut live = this.ready.live();
        while let Some(index) = live.first() {
            live.remove(index);
            if let Some(bound) = task::raw::Binding::get(this.bindings.as_ref().at(index)) {
                // SAFETY: this node was admitted by this group's exact domain.
                let _ = unsafe { bound.reclaim(this.domain) };
            }
            let slot = index.project(this.slots.as_mut());
            // SAFETY: live retains each initialized fiber until this sole drop.
            unsafe { slot.get_unchecked_mut().assume_init_drop() };
        }
    }
}

const _: () = assert!(mem::size_of::<Drive<16>>() == 4 * mem::size_of::<usize>());
