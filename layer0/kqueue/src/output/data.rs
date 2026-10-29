use dope_sys::{
    io::{reactor::retry, socket::msg::Header},
    memory,
};

#[repr(transparent)]
pub struct Data<C: retry::Context>(pub(in crate::output) Payload<C>);

pub(in crate::output) enum Payload<C: retry::Context> {
    Bytes {
        ptr: *const u8,
        len: <C::Extent as memory::raw::Extent>::Length,
    },
    Message {
        msg: *const Header,
    },
}
