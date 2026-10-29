use dope_sys::{
    io::reactor::read::{self, Call as _, Family},
    table::{change::Id as _, registry},
};
use libc::socklen_t;

use crate::reactor::input::{
    File,
    entry::{Accept, Action, Call, Header, Id},
};

#[repr(transparent)]
pub struct Slot<C: read::Context>(Call<C>);

impl<C: read::Context> registry::Entry for Slot<C> {
    type Id = Id;
    type Echo = C::Key;
    type File = File<C>;
    type Family = Family;
    type Call = Call<C>;

    fn id(&self) -> Id {
        Id {
            key: self.0.key().into(),
            kind: self.0.kind(),
        }
    }

    fn echo(&self) -> C::Key {
        self.0.key()
    }

    fn file(&self) -> File<C> {
        self.0.file()
    }

    fn index(&self) -> u32 {
        self.0.file().slot().into()
    }

    fn family(&self) -> Family {
        self.0.family()
    }

    fn operation(&self) -> Call<C> {
        self.0
    }

    fn into_call(self) -> Call<C> {
        self.0
    }

    fn key(id: Id) -> usize {
        id.key
    }
}

impl<C: read::Context> read::raw::Entry<C> for Slot<C> {
    fn accept(
        file: File<C>,
        key: C::Key,
        addr: *mut libc::sockaddr_storage,
        len: *mut socklen_t,
        flow: C::Flow,
    ) -> Self {
        Self(Call(Action::Accept(Accept {
            header: Header { file, key },
            addr,
            len,
            flow,
        })))
    }

    fn recv(file: File<C>, key: C::Key) -> Self {
        Self(Call(Action::Recv(Header { file, key })))
    }

    fn message(file: File<C>, key: C::Key) -> Self {
        Self(Call(Action::Message(Header { file, key })))
    }
}
