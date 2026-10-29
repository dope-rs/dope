use std::{
    cell,
    mem::{ManuallyDrop, MaybeUninit, size_of},
};

use dope_core::driver::{
    route::{Identity, KeyTag},
    schedule::ready::wake,
    storage,
};
use o3::{
    alloc::BoxSliceExt as _,
    depot::{arena::slab, capacity},
    mem::{self, Return, lease},
};

use crate::{
    app::creation,
    client::{attempt, attempt::queue},
};

/// One attempt queue and its exact-generation receipt slots.
pub struct Domain<'d, T, C, O, ID: Identity = KeyTag<0>>
where
    T: dope_net::Transport,
{
    source: queue::Source<'d, T, ID>,
    slots: Box<[Slot<'d, C, O, ID>]>,
}

/// One unpublished attempt generation paired with its receipt slot.
///
/// A completion receipt exists only after the context and target are published.
#[must_use = "dropping a vacant attempt releases its generation"]
pub struct Vacant<'source, 'd, T, C, O, ID: Identity = KeyTag<0>>
where
    T: dope_net::Transport,
{
    slot: Reservation<'source, 'd, C, O, ID>,
    reserve: queue::Reserve<'source, 'd, T, ID>,
}

/// Owns one receipt slot and pins its exact attempt generation.
#[must_use = "an attempt must be polled or cancelled"]
pub struct Lease<'source, 'd, T: dope_net::Transport, C, O, ID: Identity = KeyTag<0>> {
    slot: Initialized<'source, 'd, C, O, ID>,
    lease: queue::Lease<'source, 'd, T, ID>,
}

/// Completion transfers the attempt owner together with its outcome.
#[must_use = "a pending attempt or completed outcome must retain its owner"]
pub enum Poll<'source, 'd, T: dope_net::Transport, C, O, ID: Identity = KeyTag<0>> {
    Pending(Lease<'source, 'd, T, C, O, ID>),
    Ready(O, queue::Lease<'source, 'd, T, ID>),
}

impl<'d, T, C, O, ID: Identity> Domain<'d, T, C, O, ID>
where
    T: dope_net::Transport,
{
    pub fn try_new(
        capacity: capacity::Count,
        store: &mut storage::Context<'_, 'd>,
    ) -> Result<Self, creation::Error> {
        let slots =
            Box::try_init(capacity.get(), Slot::new).map_err(creation::Error::Allocation)?;
        let source = queue::Source::try_new(capacity, store)?;
        Ok(Self { source, slots })
    }

    pub fn source(&self) -> &queue::Source<'d, T, ID> {
        &self.source
    }

    pub fn try_reserve(&self) -> Result<Vacant<'_, 'd, T, C, O, ID>, Error<'d, ID>> {
        let reserve = self.source.try_reserve().map_err(Error::Queue)?;
        let key = reserve.id();
        // SAFETY: this source's live vacancy is inside the identical, fixed slot capacity.
        // Every mailbox owner clears its slot before releasing the paired queue
        // generation; the retained vacancy therefore also proves this slot vacant.
        let slot = unsafe { self.slots.get_unchecked(key.index().into_usize()) };
        slot.key.set(Some(key));
        let slot = Reservation(mem::Lease::open(Reserved(slot), ()));
        Ok(Vacant { reserve, slot })
    }

    pub fn try_settle(
        &self,
        receipt: &attempt::Completion<'_, 'd, ID>,
        outcome: O,
    ) -> Result<(C, Option<wake::Waker<'d>>), (Error<'d, ID>, O)> {
        let id = receipt.id();
        if !self.source.is_source(receipt) {
            return Err((Error::Slot(Conflict::Foreign(id)), outcome));
        }
        let Some(slot) = self.slots.get(id.index().into_usize()) else {
            return Err((
                Error::Index {
                    key: id,
                    len: self.slots.len(),
                },
                outcome,
            ));
        };
        slot.try_settle(id, outcome)
            .map_err(|(cause, outcome)| (Error::Slot(cause), outcome))
    }
}

impl<'source, 'd, T: dope_net::Transport, C, O, ID: Identity> Lease<'source, 'd, T, C, O, ID> {
    pub const fn id(&self) -> attempt::Id<'d, ID> {
        self.lease.id()
    }

    pub fn receipt(&self) -> attempt::Completion<'source, 'd, ID> {
        self.lease.receipt()
    }

    pub fn poll(self, waiter: wake::Waker<'d>) -> Poll<'source, 'd, T, C, O, ID> {
        let (slot, state) = self.slot.take();
        match state {
            State::Pending(context, _) => {
                let slot = slot.insert(State::Pending(context, Some(waiter)));
                Poll::Pending(Self {
                    slot,
                    lease: self.lease,
                })
            }
            State::Settled(outcome) => Poll::Ready(outcome, self.lease),
        }
    }

    pub fn cancel(self) -> Cancellation<O> {
        let outcome = {
            let (_, state) = self.slot.take();
            match state {
                State::Pending(..) => Cancellation::Pending,
                State::Settled(outcome) => Cancellation::Settled(outcome),
            }
        };
        self.lease.cancel();
        outcome
    }
}

impl<'source, 'd, T, C, O, ID: Identity> Vacant<'source, 'd, T, C, O, ID>
where
    T: dope_net::Transport,
{
    pub fn start(
        self,
        context: C,
        target: attempt::StreamTarget<T>,
    ) -> Lease<'source, 'd, T, C, O, ID> {
        let Self { reserve, slot } = self;
        let lease = reserve.start(target);
        let slot = slot.insert(State::Pending(context, None));
        Lease { slot, lease }
    }
}

const _: () = {
    assert!(
        size_of::<Domain<'static, dope_net::tcp::Tcp, (), ()>>()
            == size_of::<queue::Source<'static, dope_net::tcp::Tcp>>() + size_of::<[usize; 2]>()
    );
    assert!(
        size_of::<Vacant<'static, 'static, dope_net::tcp::Tcp, (), ()>>()
            == size_of::<queue::Reserve<'static, 'static, dope_net::tcp::Tcp>>()
                + size_of::<usize>()
    );
    assert!(
        size_of::<Lease<'static, 'static, dope_net::tcp::Tcp, (), ()>>()
            == size_of::<queue::Lease<'static, 'static, dope_net::tcp::Tcp>>() + size_of::<usize>()
    );
    assert!(
        size_of::<Option<Lease<'static, 'static, dope_net::tcp::Tcp, (), ()>>>()
            == size_of::<Lease<'static, 'static, dope_net::tcp::Tcp, (), ()>>()
    );
};

/// Failure of the exact attempt/mailbox operation, with both identities retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error<'d, ID: Identity = KeyTag<0>> {
    Queue(slab::Vacancy),
    Index {
        key: attempt::Id<'d, ID>,
        len: usize,
    },
    Slot(Conflict<'d, ID>),
}

/// Requested attempt and the generation that prevents its transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Conflict<'d, ID: Identity = KeyTag<0>> {
    Foreign(attempt::Id<'d, ID>),
    Busy(attempt::Id<'d, ID>),
    Vacant(attempt::Id<'d, ID>),
    Pending {
        key: attempt::Id<'d, ID>,
        current: attempt::Id<'d, ID>,
    },
    Settled {
        key: attempt::Id<'d, ID>,
        current: attempt::Id<'d, ID>,
    },
}

/// Cancellation retains any outcome requiring reclamation by its owner.
#[derive(Clone, Copy, PartialEq, Eq)]
#[must_use = "a settled cancellation outcome must be reclaimed"]
pub enum Cancellation<O> {
    Pending,
    Settled(O),
}

const _: () = assert!(size_of::<Error<'static>>() == size_of::<[usize; 3]>());

enum State<'d, C, O> {
    Pending(C, Option<wake::Waker<'d>>),
    Settled(O),
}

struct Slot<'d, C, O, ID: Identity> {
    key: cell::Cell<Option<attempt::Id<'d, ID>>>,
    initialized: cell::Cell<bool>,
    settled: cell::Cell<bool>,
    value: cell::UnsafeCell<MaybeUninit<Value<'d, C, O>>>,
}

union Value<'d, C, O> {
    pending: ManuallyDrop<(C, Option<wake::Waker<'d>>)>,
    settled: ManuallyDrop<O>,
}

struct Reservation<'a, 'd, C, O, ID: Identity>(mem::Lease<Reserved<'a, 'd, C, O, ID>, lease::Open>);

struct Initialized<'a, 'd, C, O, ID: Identity>(mem::Lease<Live<'a, 'd, C, O, ID>, lease::Open>);

struct Reserved<'a, 'd, C, O, ID: Identity>(&'a Slot<'d, C, O, ID>);
struct Live<'a, 'd, C, O, ID: Identity>(&'a Slot<'d, C, O, ID>);

impl<'d, C, O, ID: Identity> Slot<'d, C, O, ID> {
    const fn new() -> Self {
        Self {
            key: cell::Cell::new(None),
            initialized: cell::Cell::new(false),
            settled: cell::Cell::new(false),
            value: cell::UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    fn try_settle(
        &self,
        key: attempt::Id<'d, ID>,
        outcome: O,
    ) -> Result<(C, Option<wake::Waker<'d>>), (Conflict<'d, ID>, O)> {
        let Some(current) = self.key.get() else {
            return Err((Conflict::Vacant(key), outcome));
        };
        if !self.initialized.get() {
            return Err((Conflict::Busy(key), outcome));
        }
        if current != key {
            let cause = if self.settled.get() {
                Conflict::Settled { key, current }
            } else {
                Conflict::Pending { key, current }
            };
            return Err((cause, outcome));
        }
        self.initialized.set(false);
        // SAFETY: exact generation and initialized state were admitted above.
        // The cleared header excludes reentry until the transition is restored.
        let value = unsafe { self.take() };
        let (value, result) = match value {
            State::Pending(context, waiter) => (State::Settled(outcome), Ok((context, waiter))),
            state @ State::Settled(_) => {
                (state, Err((Conflict::Settled { key, current }, outcome)))
            }
        };
        // SAFETY: this checked transition owns the removed payload, and the
        // transition produced its exact successor without user reentry.
        unsafe { self.insert(value) };
        result
    }

    unsafe fn take(&self) -> State<'d, C, O> {
        // SAFETY: the caller owns the initialized payload and excludes shared
        // payload access by clearing the header or destroying the exclusive owner.
        // `settled` records the payload's only two legal variants.
        let value = unsafe { (*self.value.get()).assume_init_read() };
        if self.settled.get() {
            State::Settled(ManuallyDrop::into_inner(unsafe { value.settled }))
        } else {
            let (context, waiter) = ManuallyDrop::into_inner(unsafe { value.pending });
            State::Pending(context, waiter)
        }
    }

    unsafe fn insert(&self, state: State<'d, C, O>) {
        let (settled, value) = match state {
            State::Pending(context, waiter) => (
                false,
                Value {
                    pending: ManuallyDrop::new((context, waiter)),
                },
            ),
            State::Settled(outcome) => (
                true,
                Value {
                    settled: ManuallyDrop::new(outcome),
                },
            ),
        };
        // SAFETY: the caller owns the vacant payload storage. Publish its
        // variant before exposing initialized storage to shared admissions.
        unsafe { self.value.get().write(MaybeUninit::new(value)) };
        self.settled.set(settled);
        self.initialized.set(true);
    }
}

impl<'a, 'd, C, O, ID: Identity> Reservation<'a, 'd, C, O, ID> {
    fn insert(self, value: State<'d, C, O>) -> Initialized<'a, 'd, C, O, ID> {
        let (Reserved(slot), ()) = self.0.into_parts();
        // SAFETY: the consumed reserve exclusively owns uninitialized
        // payload storage. No other path can publish this reserve.
        unsafe { slot.insert(value) };
        Initialized(mem::Lease::open(Live(slot), ()))
    }
}

impl<'a, 'd, C, O, ID: Identity> Initialized<'a, 'd, C, O, ID> {
    fn take(self) -> (Reservation<'a, 'd, C, O, ID>, State<'d, C, O>) {
        let (live, ()) = self.0.into_parts();
        live.take()
    }
}

impl<'a, 'd, C, O, ID: Identity> Live<'a, 'd, C, O, ID> {
    fn take(self) -> (Reservation<'a, 'd, C, O, ID>, State<'d, C, O>) {
        self.0.initialized.set(false);
        // SAFETY: this linear owner was created only by insertion and is
        // consumed once; the cleared header withholds shared payload access.
        let value = unsafe { self.0.take() };
        (Reservation(mem::Lease::open(Reserved(self.0), ())), value)
    }
}

impl<C, O, ID: Identity> Return for Reserved<'_, '_, C, O, ID> {
    type Item = ();

    fn return_item(self, (): ()) {
        self.0.key.set(None);
    }
}

impl<C, O, ID: Identity> Return for Live<'_, '_, C, O, ID> {
    type Item = ();

    /// The returned pair destroys its reservation and then its payload, with the
    /// shared header already cleared.
    fn return_item(self, (): ()) {
        self.take();
    }
}

impl<C, O, ID: Identity> Drop for Slot<'_, C, O, ID> {
    fn drop(&mut self) {
        if *self.initialized.get_mut() {
            // SAFETY: exclusive storage destruction also reclaims an owner
            // deliberately forgotten while its payload remained initialized.
            unsafe { self.take() };
        }
    }
}

const _: () = {
    assert!(size_of::<Slot<'static, (), (), KeyTag<0>>>() == 4 * size_of::<usize>());
    assert!(size_of::<Slot<'static, u64, u64, KeyTag<0>>>() == 5 * size_of::<usize>());
};
