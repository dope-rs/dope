use libc::socklen_t;

use crate::{io, io::socket::addr};

/// Native requests retaining every input address through their borrow.
pub trait Borrow<'a>: Sized {
    type Target;
    type Buffer;

    fn send(slot: Self::Target, input: Self::Buffer) -> Self;
    fn send_msg<M: io::raw::Message<'a>>(slot: Self::Target, message: M) -> Self;
    fn accept(
        listener: Self::Target,
        address: &'a mut libc::sockaddr_storage,
        len: &'a mut socklen_t,
    ) -> Self;
    fn connect(slot: Self::Target, address: addr::Addr<'a>) -> Self;
}
