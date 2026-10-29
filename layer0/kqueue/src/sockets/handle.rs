use std::os::fd::{AsFd, BorrowedFd, OwnedFd};

use dope_sys::io::socket::Setup;

use crate::Kqueue;

#[derive(Debug)]
#[repr(transparent)]
pub struct Handle(pub(crate) OwnedFd);

impl AsFd for Handle {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }
}

const _: () = {
    assert!(size_of::<Handle>() == size_of::<OwnedFd>());
    assert!(align_of::<Handle>() == align_of::<OwnedFd>());
    assert!(size_of::<Option<Handle>>() == size_of::<OwnedFd>());
    assert!(align_of::<Option<Handle>>() == align_of::<OwnedFd>());
};

impl Handle {
    pub(crate) fn prepare(file: OwnedFd) -> Result<Self, Setup> {
        Kqueue::nosigpipe(file.as_fd()).map_err(Setup::sigpipe)?;
        Ok(Self(file))
    }
}
