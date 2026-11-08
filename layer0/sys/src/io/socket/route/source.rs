use std::os::fd;

use crate::io::socket::route::{Attach, Plan};

/// Selects routing authority without exposing the native program owner.
pub trait Source {
    type Storage;
    type Install<'a>: Attach;
    type Ready;
    type Lease<'a>: fd::AsFd;

    const STORAGE: Self::Storage;

    fn try_prepare(
        storage: &mut Self::Storage,
        plan: Plan,
    ) -> Result<(Self::Install<'_>, Self::Ready), Plan>;
    fn lease<'a>(ready: &Self::Ready, socket: fd::BorrowedFd<'a>) -> Self::Lease<'a>;
}
