use std::{mem::ManuallyDrop, ptr};

use o3::base::marker::ThreadBound;

use crate::kernel;

/// One joinable native thread, joined on drop by its creating thread.
#[repr(transparent)]
pub struct Handle(pub(super) libc::pthread_t, pub(super) ThreadBound);

impl Handle {
    /// Joins the uniquely owned thread from its creating thread.
    /// Native failure terminates before worker storage can be reclaimed.
    pub fn join(self) {
        ManuallyDrop::new(self).wait();
    }

    fn wait(&self) {
        // SAFETY: successful spawn created this unique joinable handle, retained
        // by its creating thread. pthread_join returns zero or a positive error
        // code, which is checked before the owner can reclaim worker storage.
        if let Err(cause) = unsafe {
            kernel::raw::Status::new(libc::pthread_join(self.0, ptr::null_mut())).try_pthread()
        } {
            kernel::Fatal::Join(cause).terminate()
        }
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.wait();
    }
}
