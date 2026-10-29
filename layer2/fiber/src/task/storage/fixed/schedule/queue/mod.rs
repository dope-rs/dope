use std::{mem, num, pin};

use o3::alloc;

mod proof;
pub(crate) use proof::Proof;

use crate::{
    context::wake,
    core::driver::schedule::ready::task::{self, owner},
    task::storage::fixed::{
        schedule::ready::{
            self,
            linked::{self, Prepared, Queue},
            owned,
        },
        tree,
    },
};

/// Number of tasks still bound to an owner during retirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Bound(num::NonZero<u64>);

impl Bound {
    pub(in crate::task::storage::fixed::schedule) const fn new(value: num::NonZero<u64>) -> Self {
        Self(value)
    }

    pub const fn nonzero(self) -> num::NonZero<u64> {
        self.0
    }
}

#[repr(u8)]
#[derive(Debug)]
pub enum PrepareError {
    Routes(alloc::Error),
    Queue(alloc::Error),
}

pub enum Error<Source> {
    Task(task::Error),
    Owner(OwnerError<Source>),
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Wake {
    Ready,
    /// Global ingress is pending classification into owner queues.
    Pending,
    Idle,
}

pub enum Disposition {
    Ready,
    Idle,
    Complete,
}

pub enum OwnerError<Source> {
    Foreign {
        owner: owner::Owner<Source>,
    },
    Stale {
        owner: owner::Owner<Source>,
        current: Option<owner::Generation>,
    },
}

pub enum RetireError<Source> {
    Foreign {
        owner: owner::Owner<Source>,
    },
    Stale {
        owner: owner::Owner<Source>,
        current: Option<owner::Generation>,
    },
    Busy {
        owner: owner::Owner<Source>,
        tasks: Bound,
    },
}

/// Exact unadmitted reason for one owner registration, without the identity payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnerCause {
    Foreign,
    Stale,
}

/// Exact reason one owner lane could not be retired, without the identity payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetireCause {
    Foreign,
    Stale,
    Busy { tasks: Bound },
}

impl<Source> OwnerError<Source> {
    pub const fn cause(&self) -> OwnerCause {
        match self {
            Self::Foreign { .. } => OwnerCause::Foreign,
            Self::Stale { .. } => OwnerCause::Stale,
        }
    }
}

impl<Source> RetireError<Source> {
    pub const fn cause(&self) -> RetireCause {
        match self {
            Self::Foreign { .. } => RetireCause::Foreign,
            Self::Stale { .. } => RetireCause::Stale,
            Self::Busy { tasks, .. } => RetireCause::Busy { tasks: *tasks },
        }
    }
}

impl<Source> From<task::Error> for Error<Source> {
    fn from(error: task::Error) -> Self {
        Self::Task(error)
    }
}

impl<Source> From<OwnerError<Source>> for RetireError<Source> {
    fn from(error: OwnerError<Source>) -> Self {
        match error {
            OwnerError::Foreign { owner } => Self::Foreign { owner },
            OwnerError::Stale { owner, current } => Self::Stale { owner, current },
        }
    }
}

#[pin_project::pin_project(!Unpin)]
pub struct Flat<'d> {
    #[pin]
    ready: Queue<'d>,
}

impl<'d> Flat<'d> {
    pub(super) fn new() -> Self {
        Self {
            ready: Queue::new(),
        }
    }

    pub(super) fn queue(self: pin::Pin<&Self>) -> pin::Pin<&Queue<'d>> {
        self.project_ref().ready
    }
}

#[pin_project::pin_project(!Unpin)]
pub struct Owned<'d, Source> {
    #[pin]
    ready: owned::Queue<'d, Source>,
}

#[derive(Clone, Copy)]
pub struct Owners<'a, 'd, Source> {
    owned: pin::Pin<&'a Owned<'d, Source>>,
}

impl<'a, 'd, Source> Owners<'a, 'd, Source> {
    pub(super) const fn new(owned: pin::Pin<&'a Owned<'d, Source>>) -> Self {
        Self { owned }
    }

    pub fn register(self, owner: owner::Owner<Source>) -> Result<(), OwnerError<Source>> {
        self.owned.queue().register(owner)
    }

    pub fn try_retire(self, owner: owner::Owner<Source>) -> Result<(), RetireError<Source>> {
        self.owned.queue().try_retire(owner)
    }

    pub fn readiness(self, scope: owner::Scope<Source>) -> Wake {
        self.owned.queue().readiness(scope)
    }
}

impl<'d, Source> Owned<'d, Source> {
    pub(super) fn try_new(
        tasks: tree::Tasks,
        owners: owner::Capacity,
    ) -> Result<Self, PrepareError> {
        Ok(Self {
            ready: owned::Queue::try_new(tasks.nonzero(), owners).map_err(PrepareError::Queue)?,
        })
    }

    pub(super) fn queue(self: pin::Pin<&Self>) -> pin::Pin<&owned::Queue<'d, Source>> {
        self.project_ref().ready
    }
}

/// Queue behavior shared by flat and owner-partitioned schedulers.
pub trait Backend<'d>: Proof {
    type Error: From<task::Error>;
    type Input;
    type Prepared<'a>
    where
        Self: 'a;
    type Dequeued<'a, I>
    where
        Self: 'a;

    fn index<'a, I>(dequeued: &'a Self::Dequeued<'_, I>) -> &'a I;

    fn map<'a, I, J>(
        dequeued: Self::Dequeued<'a, I>,
        map: impl FnOnce(I) -> J,
    ) -> Self::Dequeued<'a, J>;

    fn branch<'a, I, L, R>(
        dequeued: Self::Dequeued<'a, I>,
        split: impl FnOnce(I) -> Result<L, R>,
    ) -> Result<Self::Dequeued<'a, L>, Self::Dequeued<'a, R>>;

    fn try_prepare<'a>(
        self: pin::Pin<&'a Self>,
        global: ready::Global,
        input: Self::Input,
        parent: wake::RootWaker<'d>,
    ) -> Result<(task::raw::Root<'d>, Self::Prepared<'a>), Self::Error>;

    fn admit<'a>(prepared: Self::Prepared<'a>, entry: pin::Pin<&'a linked::Entry>);

    fn commit(
        dequeued: Self::Dequeued<'_, ready::Global>,
        entry: pin::Pin<&linked::Entry>,
        next: Disposition,
    );

    fn detach(self: pin::Pin<&Self>, entry: pin::Pin<&linked::Entry>, global: ready::Global);
}

impl<'d, Source> Backend<'d> for Owned<'d, Source> {
    type Error = Error<Source>;
    type Input = owner::Owner<Source>;
    type Prepared<'a>
        = owned::Prepared<'a, 'd, Source>
    where
        Self: 'a;
    type Dequeued<'a, I>
        = owned::Dequeued<'a, 'd, Source, I>
    where
        Self: 'a;

    fn index<'a, I>(dequeued: &'a Self::Dequeued<'_, I>) -> &'a I {
        &dequeued.0.index
    }

    fn map<'a, I, J>(
        dequeued: Self::Dequeued<'a, I>,
        map: impl FnOnce(I) -> J,
    ) -> Self::Dequeued<'a, J> {
        let owned::Entry { queue, index, lane } = dequeued.0;
        owned::Dequeued(owned::Entry {
            queue,
            index: map(index),
            lane,
        })
    }

    fn branch<'a, I, L, R>(
        dequeued: Self::Dequeued<'a, I>,
        split: impl FnOnce(I) -> Result<L, R>,
    ) -> Result<Self::Dequeued<'a, L>, Self::Dequeued<'a, R>> {
        let owned::Entry { queue, index, lane } = dequeued.0;
        match split(index) {
            Ok(index) => Ok(owned::Dequeued(owned::Entry { queue, index, lane })),
            Err(index) => Err(owned::Dequeued(owned::Entry { queue, index, lane })),
        }
    }

    fn try_prepare<'a>(
        self: pin::Pin<&'a Self>,
        global: ready::Global,
        owner: Self::Input,
        parent: wake::RootWaker<'d>,
    ) -> Result<(task::raw::Root<'d>, Self::Prepared<'a>), Self::Error> {
        ready::try_bind(parent, |parent| {
            self.queue().try_prepare(global, owner, parent)
        })
    }

    fn admit<'a>(prepared: Self::Prepared<'a>, _: pin::Pin<&'a linked::Entry>) {
        prepared.attach().enqueue();
    }

    fn commit(
        dequeued: Self::Dequeued<'_, ready::Global>,
        _: pin::Pin<&linked::Entry>,
        next: Disposition,
    ) {
        match next {
            Disposition::Ready => dequeued.enqueue(),
            Disposition::Idle => dequeued.idle(),
            Disposition::Complete => dequeued.detach(),
        }
    }

    fn detach(self: pin::Pin<&Self>, entry: pin::Pin<&linked::Entry>, _: ready::Global) {
        self.queue().detach(entry);
    }
}

const _: () = assert!(mem::size_of::<Bound>() == mem::size_of::<num::NonZero<u64>>());
const _: () = assert!(mem::size_of::<Option<Bound>>() == mem::size_of::<Bound>());
const _: () = assert!(mem::size_of::<PrepareError>() == 2 * mem::size_of::<alloc::Error>());
const _: () = assert!(mem::align_of::<PrepareError>() == mem::align_of::<alloc::Error>());
const _: () = assert!(mem::size_of::<Error<()>>() == 4 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<OwnerError<()>>() == 3 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<RetireError<()>>() == 4 * mem::size_of::<usize>());
const _: () = assert!(
    mem::size_of::<Result<Owned<'static, ()>, PrepareError>>()
        == mem::size_of::<Owned<'static, ()>>()
);

impl<'d> Backend<'d> for Flat<'d> {
    type Error = task::Error;
    type Input = ();
    type Prepared<'a>
        = Prepared<'a, 'd>
    where
        Self: 'a;
    type Dequeued<'a, I>
        = linked::Dequeued<'a, 'd, I>
    where
        Self: 'a;

    fn index<'a, I>(dequeued: &'a Self::Dequeued<'_, I>) -> &'a I {
        &dequeued.index
    }

    fn map<'a, I, J>(
        dequeued: Self::Dequeued<'a, I>,
        map: impl FnOnce(I) -> J,
    ) -> Self::Dequeued<'a, J> {
        linked::Dequeued {
            owner: dequeued.owner,
            index: map(dequeued.index),
        }
    }

    fn branch<'a, I, L, R>(
        dequeued: Self::Dequeued<'a, I>,
        split: impl FnOnce(I) -> Result<L, R>,
    ) -> Result<Self::Dequeued<'a, L>, Self::Dequeued<'a, R>> {
        let linked::Dequeued { owner, index } = dequeued;
        match split(index) {
            Ok(index) => Ok(linked::Dequeued { owner, index }),
            Err(index) => Err(linked::Dequeued { owner, index }),
        }
    }

    fn try_prepare<'a>(
        self: pin::Pin<&'a Self>,
        global: ready::Global,
        (): Self::Input,
        parent: wake::RootWaker<'d>,
    ) -> Result<(task::raw::Root<'d>, Self::Prepared<'a>), task::Error> {
        ready::try_bind(parent, |parent| self.queue().try_prepare(global, parent))
    }

    fn admit<'a>(prepared: Self::Prepared<'a>, entry: pin::Pin<&'a linked::Entry>) {
        let Prepared { queue } = prepared;
        queue.state().enqueue(entry);
    }

    fn commit(
        dequeued: Self::Dequeued<'_, ready::Global>,
        entry: pin::Pin<&linked::Entry>,
        next: Disposition,
    ) {
        match next {
            Disposition::Ready => dequeued.enqueue(entry),
            Disposition::Idle | Disposition::Complete => {}
        }
    }

    fn detach(self: pin::Pin<&Self>, entry: pin::Pin<&linked::Entry>, _: ready::Global) {
        self.queue().detach(entry);
    }
}
