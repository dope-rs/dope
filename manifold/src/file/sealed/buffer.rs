use std::mem;

pub(super) struct Buffer<T>(mem::MaybeUninit<T>);

impl<T> Buffer<T> {
    pub(super) fn zeroed() -> Self {
        use std::mem::MaybeUninit;
        Self(MaybeUninit::zeroed())
    }

    pub(super) fn as_uninit_mut(&mut self) -> &mut mem::MaybeUninit<T> {
        &mut self.0
    }

    /// Takes the value after the kernel reported successful completion.
    pub(super) fn take_initialized(&mut self) -> T {
        // SAFETY: `Request` calls this only for a successful stat completion
        // that wrote to this exact buffer.
        unsafe { self.0.assume_init_read() }
    }
}
