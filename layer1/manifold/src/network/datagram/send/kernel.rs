use dope_core::io::recv;
use o3::buffer::{bytes::bounded, pool};

pub(in crate::network::datagram) enum Payload<'d, B = pool::Cursor> {
    Owned(bounded::Value<Vec<u8>>),
    Buffer(bounded::Value<B>),
    Packet(recv::Lease<'d>),
}
