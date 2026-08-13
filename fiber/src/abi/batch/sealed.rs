use core::{marker, pin, ptr};

use o3::collections::{self, batch::set};

#[derive(Clone, Copy)]
pub(crate) struct Index<const N: usize>(usize);

pub(crate) struct Queue<const N: usize> {
    ready: set::Set<Index<N>>,
    _pin: marker::PhantomPinned,
}

pub(crate) trait PinnedArray<const N: usize> {
    type Pinned;

    fn at(self, index: Index<N>) -> Self::Pinned;
}

impl<const N: usize> Index<N> {
    pub(crate) fn new(index: usize) -> Option<Self> {
        (index < N).then_some(Self(index))
    }

    pub(crate) const fn get(self) -> usize {
        self.0
    }
}

// SAFETY: the private field is stable across copies, and every constructed
// Index<N> is below N. Typed Set storage only reconstructs values it accepted.
impl<const N: usize> set::DenseIndex for Index<N> {
    fn into_usize(self) -> usize {
        self.0
    }

    fn from_usize(raw: usize) -> Self {
        Self(raw)
    }
}

impl<const N: usize> Queue<N> {
    pub(crate) fn try_new() -> Result<Self, collections::AllocationError> {
        Ok(Self {
            ready: set::Set::try_with_capacity(N)?,
            _pin: marker::PhantomPinned,
        })
    }

    pub(crate) fn snapshot(self: pin::Pin<&Self>) -> Option<set::Drain<'_, Index<N>>> {
        self.get_ref().ready.drain_batch()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ready.is_empty()
    }

    pub(crate) fn return_ready(self: pin::Pin<&Self>, index: Index<N>) -> bool {
        self.ready.insert(index)
    }
}

impl<'array, T, const N: usize> PinnedArray<N> for pin::Pin<&'array [T; N]> {
    type Pinned = pin::Pin<&'array T>;

    fn at(self, index: Index<N>) -> Self::Pinned {
        // SAFETY: Index<N> can only contain a value below N, and a pinned
        // array pins each element for the lifetime of the array projection.
        unsafe { self.map_unchecked(|array| array.get_unchecked(index.get())) }
    }
}

impl<'array, T, const N: usize> PinnedArray<N> for pin::Pin<&'array mut [T; N]> {
    type Pinned = pin::Pin<&'array mut T>;

    fn at(self, index: Index<N>) -> Self::Pinned {
        // SAFETY: Index<N> proves the selected element is in bounds. This
        // exclusive projection neither moves nor aliases the pinned element.
        unsafe { self.map_unchecked_mut(|array| array.get_unchecked_mut(index.get())) }
    }
}

// SAFETY: Queue accepts only Index<N> values and registers their proven raw
// value in a ready set of capacity N. Every yielded index therefore remains
// representable by Index<N> for the queue's pinned lifetime.
unsafe impl<const N: usize> crate::raw::BindingQueue for Queue<N> {
    type Input = Index<N>;

    fn attach(self: pin::Pin<&Self>, _index: usize, index: Self::Input) -> usize {
        index.get()
    }

    fn ready(self: pin::Pin<&Self>) -> pin::Pin<&set::Set<usize>> {
        // SAFETY: ready is structurally pinned with Queue.
        let ready = unsafe { self.map_unchecked(|queue| &queue.ready) };
        // SAFETY: Set<I> is transparent over identical storage for every I.
        // attach returns the in-range raw projection of this Index<N>.
        unsafe { ready.map_unchecked(|set| &*ptr::from_ref(set).cast::<set::Set<usize>>()) }
    }
}

// SAFETY: the pinned Core owns both endpoints. Its pinned Drop unbinds
// every live task before the task array, queue, or parent brand can disappear.
unsafe impl<'a, 'd> crate::raw::StableBindingSource<'a, 'd> for super::Task<'a, 'd> {
    fn context(self) -> pin::Pin<&'a crate::raw::Binding<'d>> {
        self.context
    }
}
