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

use crate::task::storage::{
    caps,
    fixed::schedule::{
        pool::{Entry, Storage, poll},
        ready, selected,
    },
};

/// One pinned, lazily-initialized heap task slot extent in o3's arena slab.
type Inner<T, Tag, const MAX: u32> = slab::Pinned<T, Tag, MAX, false, 1, storage::Lazy>;

/// One reserved heap slot reported before its task is installed.
type Anchor<'a, T, Tag, const MAX: u32> = slab::Anchor<'a, T, Tag, MAX, false, 1, storage::Lazy>;

/// One occupied heap slot retaining its exclusive pinned store loan.
type Anchored<'a, T, Tag, const MAX: u32> =
    slab::Anchored<'a, T, Tag, MAX, false, 1, storage::Lazy>;

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
    pub(super) fn try_new(
        capacity: caps::Tasks<N>,
        physical: capacity::Count,
    ) -> Result<Self, alloc::Error> {
        let count = physical.min(capacity::Count::new(capacity.nonzero().get()));
        Ok(Self {
            inner: Inner::try_new(count)?,
            local: marker::PhantomData,
        })
    }

    pub(super) fn vacant(self: pin::Pin<&mut Self>) -> Option<Vacant<'_, T, Tag, N, MAX>> {
        Some(Vacant {
            anchor: self.project().inner.vacant()?,
            local: marker::PhantomData,
        })
    }

    pub(super) fn parts_mut(
        self: pin::Pin<&mut Self>,
        parts: key::Parts,
    ) -> Option<pin::Pin<&mut T>> {
        let parts = key::Parts::<MAX>::from_raw(parts.into_raw())?;
        self.project().inner.parts_mut(parts)
    }

    /// Narrows a full-domain schedule identity to this extent's live generation.
    pub(super) fn remove_with<R>(
        self: pin::Pin<&mut Self>,
        parts: key::Parts,
        f: impl FnOnce(ready::raw::Local<N>, pin::Pin<&mut T>) -> R,
    ) -> Option<R> {
        let parts = key::Parts::<MAX>::from_raw(parts.into_raw())?;
        self.project().inner.remove_with(parts, |index, value| {
            // SAFETY: this extent holds at most `N` slots, so the arena's index
            // is inside the schedule's local domain.
            f(unsafe { ready::raw::Local::from_raw(index) }, value)
        })
    }
}

impl<'d, F, D, const N: usize, T: Copy, const MAX: u32, M> Slab<Entry<'d, F, T, M>, D, N, MAX>
where
    M: poll::Mode<'d, F>,
{
    /// Borrows the admitted index while its selected plane excludes removal.
    pub(super) fn selected<'a>(
        entry: selected::EntryRef<'a, Storage<'d, F, N, D, T, MAX, M>>,
    ) -> &'a Entry<'d, F, T, M> {
        let (member, index) = entry.into_parts();
        let index = index.local(caps::Tasks::<N>::new());
        // SAFETY: selection retains this exact pool's initialized entry and
        // its shared plane loan, excluding removal or reuse until the borrow ends.
        unsafe { depot::raw::Proof::<_, Initialized<()>>::new((&member.inner.inner, index.raw())) }
            .into()
    }

    /// Claims the admitted index until this occupied loan is consumed.
    pub(super) fn selected_mut<'a>(
        entry: selected::Entry<'a, Storage<'d, F, N, D, T, MAX, M>>,
    ) -> Anchored<'a, Entry<'d, F, T, M>, D, MAX> {
        let (member, index) = entry.into_parts();
        let index = index.local(caps::Tasks::<N>::new());
        let owner = member.project().inner.project().inner;
        // SAFETY: selection owns this pool's live entry and its exclusive
        // pinned plane loan; the native anchor keeps both through consumption.
        let live = unsafe { depot::raw::Proof::<_, Initialized<()>>::new((owner, index.raw())) };
        live.into()
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
        // SAFETY: the anchor names a slot in this extent, which never exceeds `N`.
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

const _: () = {
    assert!(mem::size_of::<Slab<u8, (), 1>>() == mem::size_of::<Inner<u8, (), { u32::MAX }>>());
    assert!(
        mem::size_of::<Vacant<'_, u8, (), 1>>()
            == mem::size_of::<Anchor<'_, u8, (), { u32::MAX }>>()
    );
};
