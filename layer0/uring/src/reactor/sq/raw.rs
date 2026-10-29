use std::{marker, mem, ops, ptr, sync::atomic};

use dope_sys::{
    engine::admit::{self, Context},
    io::{
        reactor::{Full, complete, submit},
        socket::{option, option::tuning},
    },
    kernel,
    table::token::{Codec, host},
};
use o3::{base::num::bounded, depot::capacity};

use crate::{
    Uring, error,
    reactor::{op::sockets, ring, sq},
};

/// A deferred request whose host retains every encoded address.
#[derive(Debug)]
pub(crate) struct Deferred(sq::Entry);

impl Deferred {
    /// # Safety
    /// Retain every encoded address until a submitted request completes or
    /// this capability is dropped without submission.
    pub(crate) unsafe fn new(entry: sq::Borrowed<'_>) -> Self {
        Self(entry.0)
    }

    pub(crate) fn submit<T: complete::raw::Token>(self, vacancy: Vacancy<'_, T>, token: T) {
        submit::raw::Vacancy::push_owned(vacancy, self.0, token);
    }
}

pub struct Queue<'a, T>(&'a mut State, marker::PhantomData<fn(T) -> T>);

#[must_use = "reserved submission capacity must be prepared or released"]
pub struct Vacancy<'queue, T>(&'queue mut State, marker::PhantomData<fn(T) -> T>);

impl<C, T> admit::Guard<C> for Vacancy<'_, T>
where
    C: Context<Native = Uring>,
    T: Codec,
    T::Context: host::Context<Key = C::Key>,
{
    fn submit(self, request: sq::Entry, key: C::Key) {
        submit::raw::Vacancy::push_owned(self, request, T::flight(key));
    }
}

#[must_use = "reserved submission capacity must be initialized or released"]
pub struct Reservation<'queue, T, const N: usize> {
    queue: Queue<'queue, T>,
    count: capacity::Limit<N>,
}

#[must_use = "prepared submission entries must be pushed or released"]
pub struct Prepared<'queue, 'entries, E, const N: usize> {
    state: &'queue mut State,
    entries: ptr::NonNull<E>,
    count: capacity::Limit<N>,
    lifetime: marker::PhantomData<&'entries [E]>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub(crate) struct Word(u64);

impl Word {
    pub(crate) fn encode<T: complete::raw::Token>(token: T) -> Self {
        const { assert!(mem::size_of::<T>() == mem::size_of::<u64>()) };
        const { assert!(mem::align_of::<T>() <= mem::align_of::<u64>()) };
        // SAFETY: the trait contract proves the source is eight initialized bytes.
        Self(unsafe { ptr::from_ref(&token).cast::<u64>().read_unaligned() })
    }

    pub(crate) fn decode<T: complete::raw::Token>(self) -> T {
        const { assert!(mem::size_of::<T>() == mem::size_of::<u64>()) };
        const { assert!(mem::align_of::<T>() <= mem::align_of::<u64>()) };
        // SAFETY: the trait contract admits every 64-bit representation.
        unsafe { ptr::from_ref(&self.0).cast::<T>().read_unaligned() }
    }

    pub(crate) const fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

const _: () = {
    assert!(mem::size_of::<Word>() == mem::size_of::<u64>());
    assert!(mem::align_of::<Word>() == mem::align_of::<u64>());
};

#[derive(Debug)]
struct Cursor {
    head: u32,
    tail: u32,
    published_tail: u32,
}

#[derive(Debug)]
pub(crate) struct State {
    head: ptr::NonNull<atomic::AtomicU32>,
    tail: ptr::NonNull<atomic::AtomicU32>,
    flags: ptr::NonNull<atomic::AtomicU32>,
    entries: ptr::NonNull<sq::Entry>,
    axis: ring::Axis,
    cursor: Cursor,
}

impl<'queue, T> Queue<'queue, T> {
    pub(crate) const fn new(state: &'queue mut State) -> Self {
        Self(state, marker::PhantomData)
    }

    /// Retains this queue through the commit decision; Idle proves zero pending SQEs.
    /// The exclusive owner and nonzero ring size preserve its first vacancy.
    pub(crate) fn try_idle(
        self,
        binding: ring::ffi::Binding<'queue>,
    ) -> Result<Option<Vacancy<'queue, T>>, kernel::Errno<error::Enter>> {
        use dope_sys::ring::enter::{Enter as _, Progress};

        let progress = ring::Enter::<T>::new(&mut *self.0, binding).try_commit()?;
        Ok(match progress {
            Progress::Idle => Some(Vacancy(self.0, marker::PhantomData)),
            Progress::Entered => None,
        })
    }
}

impl<'queue, T: complete::raw::Token> submit::Queue<'queue, T> for Queue<'queue, T> {
    type Owned = sq::Owned<T>;
    type Vacancy = Vacancy<'queue, T>;
    type Reserved<const N: usize> = Reservation<'queue, T, N>;
    type Prepared<'e, E: 'e, const N: usize> = Prepared<'queue, 'e, E, N>;

    /// Publishes every locally appended SQE and refreshes the cached kernel
    /// head. Normal submission does this through `Ring::enter`; callers only
    /// need this method when retrying a locally full queue without entering.
    fn refresh(&mut self) {
        self.0.publish();
    }

    fn len(&self) -> bounded::U32<0, { u32::MAX }> {
        self.0.pending().into()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn try_one(self) -> Result<Vacancy<'queue, T>, Full> {
        if !self.0.has_capacity(capacity::Limit::<1>::fixed::<1>()) {
            return Err(Full);
        }
        Ok(Vacancy(self.0, marker::PhantomData))
    }

    fn try_acquire(self) -> Result<Vacancy<'queue, T>, Full> {
        // SAFETY: construction binds this atomic to the live ring mapping.
        let head = unsafe { self.0.head.as_ref() }.load(atomic::Ordering::Acquire);
        if self.0.cursor.tail.wrapping_sub(head) == self.0.axis.entries().get() {
            return Err(Full);
        }
        self.0.cursor.head = head;
        Ok(Vacancy(self.0, marker::PhantomData))
    }

    fn try_reserve<const N: usize>(
        self,
        count: capacity::Limit<N>,
    ) -> Result<Reservation<'queue, T, N>, Full> {
        if !self.0.has_capacity(count) {
            return Err(Full);
        }
        Ok(Reservation { queue: self, count })
    }

    fn prepare<'entries, const N: usize>(
        self,
        entries: &'entries [sq::Owned<T>; N],
    ) -> Result<Prepared<'queue, 'entries, sq::Owned<T>, N>, Full> {
        let count = capacity::Limit::<N>::fixed::<N>();
        if !self.0.has_capacity(count) {
            return Err(Full);
        }
        Ok(Prepared {
            state: self.0,
            entries: ptr::NonNull::from(entries).cast(),
            count,
            lifetime: marker::PhantomData,
        })
    }
}

impl<T, const N: usize> Reservation<'_, T, N> {
    /// # Safety
    /// Every initialized address must remain live through completion or quiescence.
    unsafe fn push<'resources>(
        self,
        values: &'resources mut [libc::c_int; N],
        mut init: impl FnMut(&'resources mut libc::c_int) -> sq::Submit<'resources, T>,
    ) {
        let Self { queue, count } = self;
        let mut entries = [const { mem::MaybeUninit::uninit() }; N];
        // The reservation's exact count fits both arrays and initializes this prefix.
        for (entry, value) in entries.iter_mut().zip(values).take(count.get()) {
            entry.write(init(value));
        }
        queue.0.push(
            ptr::NonNull::from(&entries).cast::<sq::Submit<'resources, T>>(),
            count,
        );
    }
}

impl<'queue, T: complete::raw::Token> submit::raw::Vacancy<'queue, T> for Vacancy<'queue, T> {
    type Entry = sq::Entry;
    type Owned = sq::Owned<T>;
    type Borrowed<'r> = sq::Borrowed<'r>;
    type Submit<'r> = sq::Submit<'r, T>;
    type Prepared<'e, E: 'e> = Prepared<'queue, 'e, E, 1>;

    fn push_owned(self, entry: sq::Entry, token: T) {
        self.0
            .push_one(ptr::NonNull::from(&entry))
            .fields()
            .token(token);
    }

    /// # Safety
    /// Every encoded address must remain live through terminal completion or quiescence.
    unsafe fn push<'resources>(self, entry: sq::Borrowed<'resources>, token: T) {
        self.0
            .push_one(ptr::NonNull::from(&entry))
            .fields()
            .token(token);
    }

    /// # Safety
    /// Every encoded address must remain live through terminal completion or quiescence.
    unsafe fn borrow<'entries, 'resources>(
        self,
        entry: &'entries Self::Submit<'resources>,
    ) -> Self::Prepared<'entries, Self::Submit<'resources>> {
        Prepared::one(self.0, entry)
    }

    fn prepare<'entries>(
        self,
        entry: &'entries sq::Owned<T>,
    ) -> Prepared<'queue, 'entries, sq::Owned<T>, 1> {
        Prepared::one(self.0, entry)
    }
}

impl<'queue, 'entries, E> Prepared<'queue, 'entries, E, 1> {
    fn one(state: &'queue mut State, entry: &'entries E) -> Self {
        Self {
            state,
            entries: ptr::NonNull::from(entry),
            count: capacity::Limit::fixed::<1>(),
            lifetime: marker::PhantomData,
        }
    }
}

impl<E, const N: usize> submit::Ready for Prepared<'_, '_, E, N> {
    fn push(self) {
        self.state.push(self.entries, self.count);
    }
}

const _: () = {
    assert!(mem::size_of::<Vacancy<'static, ()>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Result<Vacancy<'static, ()>, Full>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Reservation<'static, (), 7>>() == 2 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Option<Reservation<'static, (), 8>>>()
            == mem::size_of::<Reservation<'static, (), 8>>()
    );
    assert!(
        mem::size_of::<Prepared<'static, 'static, sq::Owned<()>, 7>>()
            == 3 * mem::size_of::<usize>()
    );
};

impl State {
    /// # Safety
    /// Both mappings and parameters must describe one live kernel-created
    /// ring and outlive this state.
    pub(crate) unsafe fn new(
        ring: &ring::ffi::Map,
        entries: &ring::ffi::Map,
        layout: ring::Submit,
    ) -> Self {
        let head = ring.at(layout.head());
        let tail = ring.at(layout.tail());
        let flags = ring.at(layout.flags());
        let head_at = unsafe { head.as_ref().load(atomic::Ordering::Acquire) };
        let tail_at = unsafe { tail.as_ref().load(atomic::Ordering::Relaxed) };
        Self {
            head,
            tail,
            flags,
            entries: entries.at(layout.entries()),
            axis: layout.axis(),
            cursor: Cursor {
                head: head_at,
                tail: tail_at,
                published_tail: tail_at,
            },
        }
    }

    pub(crate) fn publish(&mut self) {
        if self.cursor.tail != self.cursor.published_tail {
            // SAFETY: construction binds this atomic to the live ring mapping.
            unsafe { self.tail.as_ref() }.store(self.cursor.tail, atomic::Ordering::Release);
            self.cursor.published_tail = self.cursor.tail;
        }
        // SAFETY: construction binds both atomics to this live ring mapping.
        self.cursor.head = unsafe { self.head.as_ref() }.load(atomic::Ordering::Acquire);
    }

    pub(crate) fn pending(&self) -> sq::Pending {
        sq::Pending::from_raw(self.cursor.tail.wrapping_sub(self.cursor.head))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.cursor.tail == self.cursor.head
    }

    pub(crate) fn has_capacity<const N: usize>(&self, required: capacity::Limit<N>) -> bool {
        let pending: bounded::U32<0, { u32::MAX }> = self.pending().into();
        self.axis.entries().get() - pending.get() >= required.count().get()
    }

    fn push<E, const N: usize>(&mut self, entries: ptr::NonNull<E>, count: capacity::Limit<N>) {
        const { assert!(mem::size_of::<E>() == mem::size_of::<sq::Entry>()) };
        const { assert!(mem::align_of::<E>() == mem::align_of::<sq::Entry>()) };
        if N == 1 {
            self.push_one(entries);
            return;
        }
        let start = (self.cursor.tail & self.axis.mask()) as usize;
        let len = count.get();
        let first = len.min(self.axis.entries().get() as usize - start);
        // SAFETY: the aggregate capacity check reserves both destination
        // spans, mapped SQEs cannot alias the caller-owned source slice, and
        // `first` bounds both regions. copy_nonoverlapping lets the compiler
        // select the target's optimal fixed-size or vectorized bulk copy.
        unsafe {
            ptr::copy_nonoverlapping(
                entries.as_ptr().cast::<sq::Entry>(),
                self.entries.as_ptr().add(start),
                first,
            );
        }
        let rest = len - first;
        if rest != 0 {
            // SAFETY: the remaining reserved slots wrap exactly once to the
            // start of the power-of-two SQ mapping and do not overlap source.
            unsafe {
                ptr::copy_nonoverlapping(
                    entries.as_ptr().add(first).cast::<sq::Entry>(),
                    self.entries.as_ptr(),
                    rest,
                );
            }
        }
        self.cursor.tail = self.cursor.tail.wrapping_add(count.count().get());
    }

    fn push_one<E>(&mut self, entry: ptr::NonNull<E>) -> &mut sq::Entry {
        const { assert!(mem::size_of::<E>() == mem::size_of::<sq::Entry>()) };
        const { assert!(mem::align_of::<E>() == mem::align_of::<sq::Entry>()) };
        let index = (self.cursor.tail & self.axis.mask()) as usize;
        // SAFETY: the exact one-slot reservation and ring mask prove this slot
        // is free and in bounds. The caller-owned SQE cannot alias the mapping.
        let target = unsafe {
            let target = self.entries.as_ptr().add(index);
            ptr::copy_nonoverlapping(entry.as_ptr().cast::<sq::Entry>(), target, 1);
            &mut *target
        };
        self.cursor.tail = self.cursor.tail.wrapping_add(1);
        target
    }

    /// Samples kernel-owned flags for this single-issuer ring.
    pub(crate) fn flags(&self) -> u32 {
        // SAFETY: construction binds this atomic to the live SQ ring mapping.
        unsafe { self.flags.as_ref() }.load(atomic::Ordering::Acquire)
    }
}

impl<T: complete::raw::Token, const N: usize> tuning::raw::Chain<T, N> for Reservation<'_, T, N> {
    type Target = sq::Fixed;

    unsafe fn submit(
        self,
        values: &mut [libc::c_int; N],
        mut options: impl option::Cursor,
        slot: Self::Target,
        token: T,
    ) {
        // SAFETY: the caller retains every pinned scalar until all option
        // CQEs arrive, and its cursor has the reserved exact count.
        unsafe {
            self.push(values, |value| {
                Self::option(slot, options.pop(), value, token)
            })
        };
    }
}

impl<T: complete::raw::Token, const N: usize> Reservation<'_, T, N> {
    fn option<'a>(
        slot: sq::Fixed,
        step: impl option::Step,
        value: &'a mut libc::c_int,
        token: T,
    ) -> sq::Submit<'a, T> {
        let (kind, linked) = match step.kind() {
            ops::ControlFlow::Continue(kind) => (kind, true),
            ops::ControlFlow::Break(kind) => (kind, false),
        };
        *value = step.value().raw();
        let entry = sockets::set_option(slot, kind, value).user_data(token);
        if linked {
            entry.flags(sq::Flags::LINK)
        } else {
            entry
        }
    }
}
