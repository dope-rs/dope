use dope_sys::io::reactor::read;
use libc::socklen_t;

use crate::reactor::input::entry::Header;

#[derive(Clone, Copy)]
pub(in crate::reactor::input) struct Accept<C: read::Context> {
    pub(in crate::reactor::input) header: Header<C>,
    pub(in crate::reactor::input) addr: *mut libc::sockaddr_storage,
    pub(in crate::reactor::input) len: *mut socklen_t,
    pub(in crate::reactor::input) flow: C::Flow,
}
