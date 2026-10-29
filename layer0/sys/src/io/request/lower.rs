use o3::buffer::bytes::{self, bounded};

use crate::io::request;

/// Native requests whose input borrows are consumed during lowering.
pub trait Lower: Sized {
    type Target;
    type Outbound;
    type Domain;
    type Datagram;
    type Borrowed<'a>: request::Borrow<'a, Target = Self::Target, Buffer = bytes::Prefix<'a, { bounded::MAX }>>
    where
        Self: 'a;

    fn socket(slot: Self::Outbound, domain: Self::Domain) -> Self;
    fn accept_multi(listener: Self::Target) -> Self;
    fn recv<const GROUP: u16>(slot: Self::Target) -> Self;
    fn recv_msg<const GROUP: u16>(datagram: Self::Datagram) -> Self;
    fn shutdown(slot: Self::Target) -> Self;
}
