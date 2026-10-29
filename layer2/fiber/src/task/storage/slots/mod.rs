use core::{num::NonZero, pin, task::Poll};

use o3::{depot, depot::array};

use crate::{
    context::wake,
    core::driver::schedule::ready::{
        task,
        task::{Domain, Error, Reclaim, pool, topology},
    },
    task::{batch, storage::caps},
};

pub(crate) mod raw;

pub(crate) enum Action<O, R> {
    Keep(R),
    Complete(O, R),
    Vacate(R),
}

pub(crate) enum Phase<I, R> {
    Vacant(I),
    Live(R),
    Done(I),
}

/// Exact phase of the value retained by one task slot.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum State {
    Live,
    Staged,
    Vacant,
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub(crate) struct Index<const N: usize>(NonZero<u16>);

#[pin_project::pin_project(!Unpin)]
pub(crate) struct Bindings<'d, const N: usize> {
    #[pin]
    items: [crate::task::raw::Binding<'d, Index<N>>; N],
    capacity: caps::Slots<N>,
}

#[pin_project::pin_project(!Unpin)]
pub(crate) struct Table<'d, F, O, const N: usize> {
    #[pin]
    bindings: Bindings<'d, N>,
    #[pin]
    slots: [raw::Entry<F, O>; N],
}

struct Drain<'a, F, O, const N: usize> {
    slots: pin::Pin<&'a mut [raw::Entry<F, O>; N]>,
    complete: batch::Completed<N>,
}

impl<F, O, const N: usize> Drain<'_, F, O, N> {
    fn into_iter(mut self) -> array::IntoIter<O, N> {
        array::Vec::from_fn(self.complete.len(), |slot| self.take(slot)).into_iter()
    }

    fn take(&mut self, slot: array::Vacancy<N>) -> O {
        let index = Index::from_vacant(&slot);
        // SAFETY: the constructor's admitted ordinal is below N and this
        // projection retains the exact pinned table's exclusive borrow.
        let entry = unsafe {
            self.slots
                .as_mut()
                .map_unchecked_mut(|slots| slots.get_unchecked_mut(index.get()))
        };
        // SAFETY: this owner retains Completed until its last admitted output
        // is taken; no table mutation can intervene or revisit an ordinal.
        unsafe { raw::Done::exact(entry) }.take()
    }
}

impl<const N: usize> Index<N> {
    pub(crate) fn from_vacant(vacant: &array::Vacancy<N>) -> Self {
        // SAFETY: Vacancy<N> identifies one index below the validated N.
        unsafe { Self::from_valid(vacant.index() as u16) }
    }

    pub(crate) fn until(end: array::Len<N>) -> impl Iterator<Item = Self> {
        (0..end.get() as u16).map(|raw| {
            // SAFETY: the range proves raw < end <= N <= u16::MAX.
            unsafe { Self::from_valid(raw) }
        })
    }

    pub(crate) const fn get(self) -> usize {
        (self.0.get() - 1) as usize
    }

    /// Mints the niche encoding for an index already proven to be below `N`.
    /// # Safety
    /// `raw` must be below the element count `N`, which is at most `u16::MAX`.
    unsafe fn from_valid(raw: u16) -> Self {
        // SAFETY: every caller proves `raw < N <= u16::MAX`, hence `raw + 1`
        // lies in `1..=u16::MAX`.
        Self(unsafe { NonZero::<u16>::new_unchecked(raw + 1) })
    }
}

unsafe impl<const N: usize> depot::raw::BitIndex<N> for Index<N> {
    fn index(self) -> usize {
        (self.0.get() - 1) as usize
    }

    unsafe fn from_index(index: usize) -> Self {
        // SAFETY: Set admits positions below N <= 16; this encoding retains
        // the same native slot domain and its nonzero representation.
        unsafe { Self::from_valid(index as u16) }
    }
}

unsafe impl<const N: usize> topology::raw::Index for Index<N> {
    fn raw(self) -> u32 {
        u32::from(self.0.get() - 1)
    }

    unsafe fn from_raw(raw: u32) -> Self {
        // SAFETY: topology reconstructs only an index previously emitted by
        // this same Index<N> implementation.
        unsafe { Self::from_valid(raw as u16) }
    }
}

impl<'d, const N: usize> Bindings<'d, N> {
    fn new() -> Self {
        Self {
            items: [const { crate::task::raw::Binding::new() }; N],
            capacity: caps::Slots::<N>::new(),
        }
    }

    fn at(
        self: pin::Pin<&Self>,
        index: Index<N>,
    ) -> pin::Pin<&crate::task::raw::Binding<'d, Index<N>>> {
        let items = self.project_ref().items;
        // SAFETY: Index<N> proves the selected element is in bounds, and a
        // pinned array pins each element for this projection's lifetime.
        unsafe { items.map_unchecked(|items| items.get_unchecked(index.get())) }
    }

    pub(crate) fn poll_domain<'a, D, T>(
        self: pin::Pin<&'a Self>,
        edge: topology::Edge<'a, 'd, Index<N>>,
        domain: &mut Domain<'d, D, N>,
        poll: impl FnOnce(wake::Waker<'d>) -> Poll<T>,
    ) -> Result<Poll<(T, Reclaim)>, Error>
    where
        'd: 'a,
    {
        let binding = self.at(edge.local());
        // SAFETY: the edge selects the exact binding in the same pinned
        // scheduler owner, which reclaims it before either endpoint is dropped.
        unsafe { crate::task::raw::Binding::poll_domain(binding, edge, domain, poll) }
    }

    pub(crate) fn reclaim<D>(
        self: pin::Pin<&Self>,
        index: Index<N>,
        domain: &mut Domain<'d, D, N>,
    ) -> Option<Reclaim> {
        let binding = self.at(index);
        crate::task::raw::Binding::reclaim(binding, domain)
    }

    pub(crate) fn poll_pool<'a, D, T>(
        self: pin::Pin<&'a Self>,
        edge: topology::Edge<'a, 'd, Index<N>>,
        pool: &pool::Pool<'d, D, N>,
        root: pool::Root<'d>,
        poll: impl FnOnce(wake::Waker<'d>) -> Poll<T>,
    ) -> Result<Poll<T>, Error>
    where
        'd: 'a,
    {
        let binding = self.at(edge.local());
        // SAFETY: the edge selects the exact binding in the same pinned
        // scheduler owner, and `pool` is exclusively lent to that owner.
        unsafe { crate::task::raw::Binding::poll_pool(binding, edge, pool, root, poll) }
    }

    pub(crate) fn reclaim_pool<D>(
        self: pin::Pin<&Self>,
        index: Index<N>,
        pool: &pool::Pool<'d, D, N>,
        root: pool::Root<'d>,
    ) {
        let binding = self.at(index);
        crate::task::raw::Binding::reclaim_pool(binding, pool, root)
    }
}

const _: () = {
    use core::mem;

    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<raw::Done<'static, (), ()>>() == word);
    assert!(mem::size_of::<Option<raw::Done<'static, (), ()>>>() == word);
    assert!(mem::size_of::<Index<1>>() == mem::size_of::<u16>());
    assert!(mem::size_of::<topology::Endpoint<'static, Index<1>>>() == 2 * word);
    assert!(mem::size_of::<topology::Slot<'static, Index<1>>>() == 2 * word);
    assert!(mem::size_of::<topology::Edge<'static, 'static, Index<1>>>() == 2 * word);
    assert!(mem::size_of::<task::raw::Node<'static, Index<1>>>() == 3 * word);
    assert!(mem::size_of::<Bindings<'static, 1>>() == 3 * word);
};

impl<'d, F, O, const N: usize> Table<'d, F, O, N> {
    pub(crate) fn vacant() -> Self {
        Self {
            bindings: Bindings::new(),
            slots: [const { raw::Entry::vacant() }; N],
        }
    }

    pub(crate) fn from_map<P>(input: [P; N], mut make: impl FnMut(P) -> F) -> Self {
        Self {
            bindings: Bindings::new(),
            slots: input.map(|value| raw::Entry::live(make(value))),
        }
    }

    pub(crate) fn from_fn(mut make: impl FnMut(batch::Index<N>) -> F) -> Self {
        Self {
            bindings: Bindings::new(),
            slots: batch::Index::map(|index| raw::Entry::live(make(index))),
        }
    }

    pub(crate) fn visit<I, R>(
        self: pin::Pin<&mut Self>,
        index: Index<N>,
        input: I,
        run: impl for<'a, 'b> FnOnce(
            pin::Pin<&'a mut F>,
            pin::Pin<&'b Bindings<'d, N>>,
            I,
        ) -> Action<O, R>,
    ) -> Phase<I, R> {
        let (slot, bindings) = self.parts(index);
        slot.visit(input, |fiber, input| run(fiber, bindings, input))
    }

    pub(in crate::task) fn cancel(
        self: pin::Pin<&mut Self>,
        index: Index<N>,
        reclaim: impl FnOnce(pin::Pin<&Bindings<'d, N>>),
    ) {
        let (slot, bindings) = self.parts(index);
        slot.cancel(|| reclaim(bindings));
    }

    fn parts(
        self: pin::Pin<&mut Self>,
        index: Index<N>,
    ) -> (pin::Pin<&mut raw::Entry<F, O>>, pin::Pin<&Bindings<'d, N>>) {
        let this = self.project();
        let bindings = this.bindings.into_ref();
        let slots = this.slots;
        // SAFETY: Index<N> proves the selected element is in bounds. The
        // projection keeps the binding and slot borrows disjoint.
        let slot = unsafe { slots.map_unchecked_mut(|items| items.get_unchecked_mut(index.get())) };
        (slot, bindings)
    }

    pub(in crate::task) fn drain_exact(
        self: pin::Pin<&mut Self>,
        complete: batch::Completed<N>,
    ) -> array::IntoIter<O, N> {
        Drain {
            slots: self.project().slots,
            complete,
        }
        .into_iter()
    }
}
