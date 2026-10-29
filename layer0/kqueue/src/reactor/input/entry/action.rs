use dope_sys::io::reactor::read;

use crate::reactor::input::entry::{Accept, Header};

#[derive(Clone, Copy)]
pub(in crate::reactor::input) enum Action<C: read::Context> {
    Accept(Accept<C>),
    Recv(Header<C>),
    Message(Header<C>),
}
