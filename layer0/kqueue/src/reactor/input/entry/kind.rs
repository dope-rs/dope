#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor::input) enum Kind {
    Accept,
    Recv,
    Message,
}

impl Kind {
    pub(in crate::reactor::input) fn operation<C: read::Context>(self) -> C::Operation {
        match self {
            Self::Accept => C::ACCEPT,
            Self::Recv => C::RECV,
            Self::Message => C::MESSAGE,
        }
    }
}
use dope_sys::io::reactor::read;
