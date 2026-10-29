use std::{marker, mem, pin};

use o3::{
    alloc,
    base::marker::ThreadBound,
    depot::{
        self,
        arena::slab::{self, key, storage},
        capacity,
        index::Initialized,
    },
};

use crate::{
    abi,
    task::storage::{
        caps,
        fixed::schedule::{Pool, pool::Entry, ready, selected},
    },
};

/// One pinned, lazily-initialized heap task slot extent in o3's arena slab.
type Inner<T, Tag, const MAX: u32> =
    slab::Pinned<T, Tag, MAX, false, 1, storage::Heap, storage::Lazy>;

/// One reserved heap slot reported before its task is installed.
type Anchor<'a, T, Tag, const MAX: u32> =
    slab::Anchor<'a, T, Tag, MAX, false, 1, storage::Heap, storage::Lazy>;

/// One occupied heap slot retaining its exclusive pinned store loan.
type Anchored<'a, T, Tag, const MAX: u32> =
    slab::Anchored<'a, T, Tag, MAX, false, 1, storage::Heap, storage::Lazy>;

/// Heap task slots that initialize on first admission.
///
/// The wrapper keeps only the schedule's validated `Local<N>` index domain and
/// its thread confinement; slots, generations, and the free list are o3's.
#[pin_project::pin_project(!Unpin)]
pub(in crate::task::storage::fixed::schedule) struct Slab<
    T,
    Tag,
    const N: usize,
    const MAX: u32 = { u32::MAX },
> {
    #[pin]
    inner: Inner<T, Tag, MAX>,
    local: marker::PhantomData<ThreadBound>,
}

impl<T, Tag, const N: usize, const MAX: u32> Slab<T, Tag, N, MAX> {
    pub(super) fn try_new(capacity: caps::Tasks<N>) -> Result<Self, alloc::Error> {
        Ok(Self {
            inner: Inner::try_new(capacity::Count::new(capacity.nonzero().get()))?,
            local: marker::PhantomData,
        })
    }

    pub(super) fn vacant(self: pin::Pin<&mut Self>) -> Option<Vacant<'_, T, Tag, N, MAX>> {
        Some(Vacant {
            anchor: self.project().inner.vacant()?,
            local: marker::PhantomData,
        })
    }

    /// Narrows a full-domain schedule identity to this extent's live generation.
    pub(super) fn remove_with<R>(
        self: pin::Pin<&mut Self>,
        parts: key::Parts,
        f: impl FnOnce(ready::raw::Local<N>, pin::Pin<&mut T>) -> R,
    ) -> Option<R> {
        let parts = key::Parts::<MAX>::from_raw(parts.into_raw())?;
        self.project().inner.remove_with(parts, |index, value| {
            // SAFETY: this extent holds exactly `N` slots, so the arena's index
            // is inside the schedule's local domain.
            f(unsafe { ready::raw::Local::from_raw(index) }, value)
        })
    }
}

impl<'d, F: abi::Fiber<'d>, D, const N: usize, T: Copy, const MAX: u32>
    Slab<Entry<'d, F, T>, D, N, MAX>
{
    /// Borrows the admitted index while its selected plane excludes removal.
    pub(super) fn selected<'a>(
        entry: selected::EntryRef<'a, Pool<'d, F, N, D, T, MAX>>,
    ) -> &'a Entry<'d, F, T> {
        let (member, index) = entry.into_parts();
        let index = index.local(caps::Tasks::<N>::new());
        // SAFETY: selection retains this exact pool's initialized entry and
        // its shared plane loan, excluding removal or reuse until the borrow ends.
        unsafe { depot::raw::Proof::<_, Initialized<()>>::new((&member.inner.inner, index.raw())) }
            .into()
    }
}

/// One reserved task slot reported before its value is installed.
/// The wrapper keeps the schedule's `Local<N>` index over o3's raw slot index.
pub(in crate::task::storage::fixed::schedule) struct Vacant<
    'a,
    T,
    Tag,
    const N: usize,
    const MAX: u32 = { u32::MAX },
> {
    anchor: Anchor<'a, T, Tag, MAX>,
    local: marker::PhantomData<ThreadBound>,
}

impl<'a, T, Tag, const N: usize, const MAX: u32> Vacant<'a, T, Tag, N, MAX> {
    pub(super) fn index(&self) -> ready::raw::Local<N> {
        // SAFETY: the anchor names one slot of this pool's exact `N`-slot extent.
        unsafe { ready::raw::Local::from_raw(self.anchor.index()) }
    }

    /// Widens the occupied identity into the schedule's full generation domain.
    pub(super) fn occupy(self, value: T) -> (key::Handle<Tag>, pin::Pin<&'a mut T>) {
        let (key, value) = self.anchor.occupy(value);
        (key::Handle::from_parts(key.parts().widen()), value)
    }

    pub(super) fn insert(self, value: T) -> pin::Pin<&'a mut T> {
        self.anchor.occupy(value).1
    }
}

/// One occupied task slot retaining its exclusive pinned store loan.
/// The wrapper keeps the schedule's `Local<N>` vocabulary over o3's slot.
pub(in crate::task::storage::fixed::schedule) struct Occupied<
    'a,
    T,
    Tag,
    const N: usize,
    const MAX: u32 = { u32::MAX },
> {
    anchor: Anchored<'a, T, Tag, MAX>,
    local: marker::PhantomData<ThreadBound>,
}

impl<'a, T, Tag, const N: usize, const MAX: u32> Occupied<'a, T, Tag, N, MAX> {
    pub(super) fn get_mut(self) -> pin::Pin<&'a mut T> {
        self.anchor.into_value()
    }

    pub(super) fn as_mut(&mut self) -> pin::Pin<&mut T> {
        self.anchor.value()
    }

    pub(super) fn discard(self) {
        self.anchor.discard();
    }
}

impl<'a, 'd, F: abi::Fiber<'d>, D, const N: usize, T: Copy, const MAX: u32>
    Occupied<'a, Entry<'d, F, T>, D, N, MAX>
{
    /// Claims the admitted index until this occupied loan is consumed.
    pub(super) fn selected(entry: selected::Entry<'a, Pool<'d, F, N, D, T, MAX>>) -> Self {
        let (member, index) = entry.into_parts();
        let index = index.local(caps::Tasks::<N>::new());
        let owner = member.project().inner.project().inner;
        // SAFETY: selection owns this pool's live entry and its exclusive
        // pinned plane loan; the native anchor keeps both through consumption.
        let live = unsafe { depot::raw::Proof::<_, Initialized<()>>::new((owner, index.raw())) };
        Self {
            anchor: live.into(),
            local: marker::PhantomData,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Slab<u8, (), 1>>() == mem::size_of::<Inner<u8, (), { u32::MAX }>>());
    assert!(
        mem::size_of::<Vacant<'_, u8, (), 1>>()
            == mem::size_of::<Anchor<'_, u8, (), { u32::MAX }>>()
    );
    assert!(
        mem::size_of::<Occupied<'_, u8, (), 1>>()
            == mem::size_of::<Anchored<'_, u8, (), { u32::MAX }>>()
    );
};
