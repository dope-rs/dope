use std::{mem, ptr};

use o3::base::marker::ThreadBound;

use crate::{
    Errno, kernel,
    thread::{Attrs, Handle},
};

cfg_select! {
    target_arch = "x86_64" => {
        mod x86;
        pub(super) use x86::try_crc32;
    }
    all(target_arch = "aarch64", target_os = "linux") => {
        mod linux;
        pub(super) use linux::try_crc32;
    }
    all(target_arch = "aarch64", target_os = "macos") => {
        mod macos;
        pub(super) use macos::try_crc32;
    }
    _ => {
        mod scalar;
        pub(super) use scalar::try_crc32;
    }
}

pub(super) fn entropy<S: kernel::raw::Source + ?Sized, const N: usize>(
    bytes: &mut mem::MaybeUninit<[u8; N]>,
) -> Result<&mut [u8; N], Errno> {
    const { assert!(N <= 256) };
    // SAFETY: bytes provides N writable bytes, within getentropy's 256-byte limit.
    let result = unsafe { libc::getentropy(bytes.as_mut_ptr().cast(), N) };
    // SAFETY: getentropy handles interruptions internally and returns zero or -1
    // with unchanged native errno; success initializes the complete requested extent.
    unsafe {
        kernel::raw::Status::new(result).try_zero::<Errno>(|| kernel::raw::Code::last::<S>().into())
    }?;
    // SAFETY: success initializes all N bytes, including the empty array.
    Ok(unsafe { bytes.assume_init_mut() })
}

/// # Safety
/// Implementors access only their admitted context and resources, with the promised thread exclusivity.
pub unsafe trait Task: Sized {
    /// # Safety
    /// The context and all resources it uses must retain their admitted access rights through this execution.
    unsafe fn run(context: ptr::NonNull<Self>);
}

/// One typed context admitted for a single native execution.
pub struct Entry<T: Task> {
    context: ptr::NonNull<T>,
}

impl<T: Task> Entry<T> {
    /// # Safety
    /// Retain the initialized context and its borrowed resources at stable addresses through discard, creation failure, or successful join.
    /// All accesses must satisfy T's thread exclusivity contract; forgetting the handle does not release these obligations.
    pub const unsafe fn new(context: ptr::NonNull<T>) -> Self {
        Self { context }
    }

    /// Transfers this admitted entry into one joinable native thread.
    pub fn spawn(self, attrs: &Attrs) -> Result<Handle, Errno> {
        let mut thread = mem::MaybeUninit::uninit();
        // SAFETY: attrs and the writable handle outlive this call. The consumed
        // entry preserves T's exact context and access through join.
        // pthread_create returns zero or a positive error code directly.
        unsafe {
            kernel::raw::Status::new(libc::pthread_create(
                thread.as_mut_ptr(),
                attrs.as_ptr(),
                Self::run,
                self.context.as_ptr().cast(),
            ))
            .try_pthread()
        }?;
        // SAFETY: pthread_create wrote the handle before returning success.
        Ok(Handle(unsafe { thread.assume_init() }, ThreadBound::NEW))
    }

    extern "C" fn run(context: *mut std::ffi::c_void) -> *mut std::ffi::c_void {
        // SAFETY: spawn passes this admitted nonnull T context exactly once;
        // its owner retains the allocation and access rights through join.
        unsafe { T::run(ptr::NonNull::new_unchecked(context.cast())) };
        ptr::null_mut()
    }
}
