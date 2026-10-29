use dope_sys::io::socket::{addr, msg::Header};
use libc::socklen_t;
use o3::buffer::bytes;

pub enum Retained<'a, I> {
    Accept {
        listener: I,
        addr: &'a mut libc::sockaddr_storage,
        len: &'a mut socklen_t,
    },
    Send {
        slot: I,
        input: bytes::Prefix<'a, { i32::MAX as usize }>,
    },
    SendMsg {
        slot: I,
        msg: &'a Header,
    },
    Connect {
        slot: I,
        addr: addr::Addr<'a>,
    },
}
