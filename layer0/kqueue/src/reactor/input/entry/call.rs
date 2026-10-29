use dope_sys::io::reactor::read::{self, Family};

use crate::reactor::input::{
    File,
    entry::{Action, Header, Kind},
};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Call<C: read::Context>(pub(in crate::reactor::input) Action<C>);

impl<C: read::Context> Call<C> {
    pub(in crate::reactor::input) fn header(self) -> Header<C> {
        match self.0 {
            Action::Accept(accept) => accept.header,
            Action::Recv(header) | Action::Message(header) => header,
        }
    }

    pub(in crate::reactor::input) fn kind(&self) -> Kind {
        match &self.0 {
            Action::Accept(_) => Kind::Accept,
            Action::Recv(_) => Kind::Recv,
            Action::Message(_) => Kind::Message,
        }
    }
}

impl<C: read::Context> read::Call for Call<C> {
    type File = File<C>;
    type Key = C::Key;

    fn file(self) -> Self::File {
        self.header().file
    }

    fn key(self) -> C::Key {
        self.header().key
    }

    fn family(self) -> Family {
        match self.kind() {
            Kind::Accept => Family::Accept,
            Kind::Recv | Kind::Message => Family::Recv,
        }
    }
}
