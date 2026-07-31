use core::pin::Pin;
use core::ptr::NonNull;

/// A retained pointer to a structurally pinned value.
#[repr(transparent)]
pub(crate) struct PinnedLink<T> {
    pointer: NonNull<T>,
}

/// An owner-level proof for one retained pinned link.
/// # Safety
/// The target stays pinned and live until every produced link becomes inaccessible.
/// References derived from a link remain bounded by a borrow of that link.
pub(crate) unsafe trait StableLinkSource<T> {
    fn pointer(self) -> NonNull<T>;
}

impl<T> PinnedLink<T> {
    pub(crate) fn from_stable(source: impl StableLinkSource<T>) -> Self {
        Self {
            pointer: source.pointer(),
        }
    }

    pub(crate) unsafe fn from_raw(pointer: NonNull<T>) -> Self {
        Self { pointer }
    }

    pub(crate) fn get(&self) -> Pin<&T> {
        // SAFETY: StableLinkSource guarantees the target remains pinned and
        // live while the link can be borrowed.
        unsafe { Pin::new_unchecked(self.pointer.as_ref()) }
    }

    pub(crate) fn pointer(self) -> NonNull<T> {
        self.pointer
    }
}

impl<T> Clone for PinnedLink<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for PinnedLink<T> {}

impl<T> PartialEq for PinnedLink<T> {
    fn eq(&self, other: &Self) -> bool {
        self.pointer == other.pointer
    }
}

impl<T> Eq for PinnedLink<T> {}
