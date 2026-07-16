use std::ptr::NonNull;

use super::DriverContext;

pub trait ProvidedBuffers {
    fn buffer_group(&self) -> u16;
    fn buffer_len(&self) -> usize;
    /// # Safety
    /// `bid` must name a live buffer uniquely owned by the caller.
    unsafe fn release(&mut self, bid: u16);
    /// # Safety
    /// `bid` must name a live buffer and `len` must fit its allocation.
    unsafe fn buffer_ptr_len(&mut self, len: u32, bid: u16) -> (NonNull<u8>, usize);
}

cfg_select! {
    target_os = "linux" => {
        use crate::backend::uring::provided::ring::Ring;

        impl ProvidedBuffers for DriverContext<'_, '_> {
            fn buffer_group(&self) -> u16 {
                Ring::BGID
            }

            fn buffer_len(&self) -> usize {
                self.backend_ref().provided.buf_len()
            }

            unsafe fn release(&mut self, bid: u16) {
                self.backend().provided.defer(bid);
            }

            unsafe fn buffer_ptr_len(&mut self, len: u32, bid: u16) -> (NonNull<u8>, usize) {
                let (ptr, len) = self.backend_ref().provided.ptr_len(bid, len as usize);
                (unsafe { NonNull::new_unchecked(ptr.cast_mut()) }, len)
            }
        }
    }
    _ => {
        use crate::backend::kqueue::driver::read::dispatch::Dispatch;

        impl ProvidedBuffers for DriverContext<'_, '_> {
            fn buffer_group(&self) -> u16 {
                0
            }

            fn buffer_len(&self) -> usize {
                self.backend_ref().provided.buf_len()
            }

            unsafe fn release(&mut self, bid: u16) {
                let state = self.backend();
                state.provided.defer(bid);
                if !state.resume.is_empty() {
                    state.resume_pending();
                }
            }

            unsafe fn buffer_ptr_len(&mut self, len: u32, bid: u16) -> (NonNull<u8>, usize) {
                unsafe { self.backend_ref().backing.ptr_len(bid, len as usize) }
            }
        }
    }
}
