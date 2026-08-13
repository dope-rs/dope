use std::mem;

/// Linear proof that a driver scope will be quiesced before release.
#[doc(hidden)]
pub struct Owner {
    _private: (),
}

const _: () = assert!(mem::size_of::<Owner>() == 0);

impl Owner {
    /// # Safety
    /// Every retained source must outlive completion or synchronous quiescence.
    pub unsafe fn new() -> Self {
        Self { _private: () }
    }
}
