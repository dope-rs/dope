use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;
use std::ptr::NonNull;

pub struct Holding<'d, T> {
    ptr: NonNull<T>,
    _brand: PhantomData<&'d T>,
}

impl<T> Copy for Holding<'_, T> {}

impl<T> Clone for Holding<'_, T> {
    #[inline(always)]
    fn clone(&self) -> Self {
        *self
    }
}

impl<'d, T> Holding<'d, T> {
    #[inline(always)]
    pub fn of(target: Pin<&'d mut T>) -> Self {
        // SAFETY: Pin<&'d mut T> guarantees the pointee is pinned for 'd; Holding stores only the raw pointer.
        let raw = unsafe { target.get_unchecked_mut() };
        Self {
            ptr: NonNull::from(raw),
            _brand: PhantomData,
        }
    }

    /// # Safety
    /// The caller guarantees `ptr` is valid, pinned, and outlives `'d`.
    #[inline(always)]
    pub unsafe fn from_raw(ptr: NonNull<T>) -> Self {
        Self {
            ptr,
            _brand: PhantomData,
        }
    }

    #[inline(always)]
    pub fn hold(self) -> Pin<&'d mut T> {
        // SAFETY: caller upholds that no other borrow of this pointee is live while the returned Pin's scope lasts.
        unsafe { Pin::new_unchecked(&mut *self.ptr.as_ptr()) }
    }
}

impl<'d, T> Deref for Holding<'d, T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &T {
        // SAFETY: thread-per-core; no concurrent &mut T live across this shared borrow.
        unsafe { self.ptr.as_ref() }
    }
}
