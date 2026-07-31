use core::pin::Pin;

#[inline(always)]
pub(crate) fn get<T>(slice: Pin<&[T]>, index: usize) -> Option<Pin<&T>> {
    let value = slice.get_ref().get(index)?;
    // SAFETY: pinning a slice pins every element for the lifetime of the
    // slice. A shared projection cannot move or replace that element.
    Some(unsafe { Pin::new_unchecked(value) })
}

#[inline(always)]
pub(crate) fn get_mut<T>(slice: Pin<&mut [T]>, index: usize) -> Option<Pin<&mut T>> {
    if index >= slice.len() {
        return None;
    }
    // SAFETY: pinning a slice pins every element. This exclusive projection
    // preserves the source lifetime and selects one in-bounds element.
    Some(unsafe { slice.map_unchecked_mut(|slice| slice.get_unchecked_mut(index)) })
}
