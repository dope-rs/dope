pub(super) trait Stamp {
    fn stamp(&mut self);
}

pub(super) trait Pod: Sized {
    fn zeroed() -> Self {
        // SAFETY: implementors are POD socket-address aggregates whose all-zero bit pattern is a valid value.
        unsafe { std::mem::MaybeUninit::<Self>::zeroed().assume_init() }
    }
}

impl Pod for libc::sockaddr_in {}
impl Pod for libc::sockaddr_in6 {}
impl Pod for libc::sockaddr_un {}
impl Pod for libc::sockaddr_storage {}
