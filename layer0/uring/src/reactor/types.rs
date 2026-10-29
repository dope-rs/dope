use std::mem;

#[repr(C)]
pub struct OpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}

impl OpenHow {
    pub(crate) const fn new(flags: u64, mode: u64, resolve: u64) -> Self {
        Self {
            flags,
            mode,
            resolve,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<OpenHow>() == 24);
    assert!(mem::align_of::<OpenHow>() == 8);
};
